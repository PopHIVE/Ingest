# =============================================================================
# NCHS Provisional COVID-19 Death Counts by Week Ending Date and State
# Source: https://data.cdc.gov/d/r8kw-7aab
#
# Weekly provisional death counts (COVID-19, pneumonia, influenza, total) for
# the US, 50 states, DC and Puerto Rico. Only the "By Week" rows are kept; the
# month / year / total rows are aggregates of them.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
raw_state <- dcf::dcf_download_cdc(
  "r8kw-7aab",
  "raw",
  process$raw_state,
  parquet = TRUE
)

if (!identical(process$raw_state, raw_state)) {

  # CDC's export renders counts >= 1000 with thousands separators ("60,170")
  as_num <- function(x) {
    if (is.character(x)) as.numeric(gsub(",", "", x, fixed = TRUE)) else x
  }

  # ---------------------------------------------------------------------------
  # 2. Read raw data
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom(
    "../../resources/all_fips.csv.gz",
    show_col_types = FALSE,
    col_types = vroom::cols(.default = vroom::col_character())
  )
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  count_cols <- c(
    nchs_covid_deaths = "COVID-19 Deaths",
    nchs_pneumonia_deaths = "Pneumonia Deaths",
    nchs_pneumonia_covid_deaths = "Pneumonia and COVID-19 Deaths",
    nchs_influenza_deaths = "Influenza Deaths",
    nchs_pneumonia_flu_covid_deaths = "Pneumonia, Influenza, or COVID-19 Deaths"
  )

  data_raw <- arrow::read_parquet("raw/r8kw-7aab.parquet") %>%
    as.data.frame() %>%
    filter(Group == "By Week")

  # ---------------------------------------------------------------------------
  # 3. Transform
  #    - Counts of 1-9 are suppressed (NA) by NCHS. Following project
  #      convention they are imputed as 5 with a suppressed flag.
  #    - NCHS reports New York City separately from New York (state excluding
  #      NYC). They are summed into one row for the state (FIPS 36).
  #      "Percent of Expected Deaths" is recombined through expected deaths
  #      (total / pct), since a percent cannot simply be summed.
  # ---------------------------------------------------------------------------
  data <- data_raw %>%
    transmute(
      is_nyc = State == "New York City",
      state_name = if_else(is_nyc, "New York", State),
      time = as.Date(`Week Ending Date`, "%m/%d/%Y"),
      nchs_total_deaths = as_num(`Total Deaths`),
      # 0% of expected with 0 deaths = week not yet reported (e.g. Alaska, latest weeks)
      expected_deaths = if_else(`Percent of Expected Deaths` > 0,
                                nchs_total_deaths / (`Percent of Expected Deaths` / 100), 0),
      across(all_of(count_cols), as_num)
    )

  # NY (state excluding NYC) and NYC are summed. The state part decides the flag:
  # if it has data the flag is 0, and if NYC is suppressed it is simply left out
  # (the value is then the NY total, which may be an undercount). If the state
  # part itself is suppressed (every other state is a single part) it is imputed
  # as 5 with flag 1; an available NYC count is still added.
  flag_cols <- paste0("nchs_suppressed_flag_",
                      sub("_deaths$", "", sub("^nchs_", "", names(count_cols))))
  names(flag_cols) <- names(count_cols)

  data <- data %>%
    group_by(state_name, time) %>%
    summarize(
      across(c(nchs_total_deaths, expected_deaths), sum),
      # flags first: the value columns below overwrite the raw columns they read
      across(all_of(names(count_cols)), ~ as.integer(is.na(.x[!is_nyc])), .names = "flag__{.col}"),
      across(all_of(names(count_cols)),
             ~ if_else(is.na(.x[!is_nyc]), 5, .x[!is_nyc]) + sum(.x[is_nyc], na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    rename(!!!setNames(paste0("flag__", names(flag_cols)), flag_cols)) %>%
    mutate(nchs_pct_expected_deaths = if_else(expected_deaths > 0,
                                              round(nchs_total_deaths / expected_deaths * 100),
                                              NA_real_)) %>%
    select(-expected_deaths) %>%
    left_join(
      bind_rows(state_fips_lookup,
                data.frame(geography = "00", geography_name = "United States")) %>%
        distinct(),
      by = c("state_name" = "geography_name")
    )

  # Puerto Rico has no geography_name in all_fips (name only via abbreviation)
  if (anyNA(data$geography)) {
    pr <- all_fips %>% filter(state == "PR", nchar(geography) == 2) %>% pull(geography)
    data$geography[is.na(data$geography) & data$state_name == "Puerto Rico"] <- pr
  }
  if (anyNA(data$geography)) {
    stop("no FIPS for: ", paste(unique(data$state_name[is.na(data$geography)]), collapse = ", "))
  }

  data_standard <- data %>%
    select(geography, time, nchs_total_deaths, nchs_pct_expected_deaths,
           all_of(names(count_cols)), all_of(unname(flag_cols))) %>%
    arrange(geography, time)

  if (anyDuplicated(data_standard[c("geography", "time")])) {
    stop("duplicate geography/time rows")
  }

  # ---------------------------------------------------------------------------
  # 4. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
