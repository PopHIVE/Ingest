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
  # Dates arrive as "YYYY-mm-dd", "mm/dd/YYYY" or "YYYY Mon dd ...", depending
  # on how the file was exported
  parse_cdc_date <- function(x) {
    if (inherits(x, "Date")) return(x)
    if (inherits(x, "POSIXt")) return(as.Date(x))
    x <- as.character(x)
    out <- as.Date(rep(NA_character_, length(x)))
    for (fmt in c("%Y-%m-%d", "%m/%d/%Y", "%Y %b %d")) {
      i <- is.na(out) & !is.na(x)
      out[i] <- as.Date(x[i], format = fmt)
    }
    bad <- is.na(out) & !is.na(x)
    if (any(bad)) stop("unparsed dates: ", paste(head(unique(x[bad])), collapse = ", "))
    out
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
  #    - A week NCHS has not received any certificates for yet is published as
  #      0 deaths and 0% of expected (e.g. Alaska, latest weeks). That is not an
  #      observation, so every value in such a row is set to NA.
  #    - NCHS reports New York City separately from New York (state excluding
  #      NYC). They are summed into one row for the state (FIPS 36). If either
  #      part is suppressed it is imputed as 5 and the row is flagged.
  #      "Percent of Expected Deaths" is recombined through expected deaths
  #      (total / pct), since a percent cannot simply be summed.
  # ---------------------------------------------------------------------------
  data <- data_raw %>%
    transmute(
      is_nyc = State == "New York City",
      state_name = if_else(is_nyc, "New York", State),
      time = parse_cdc_date(`Week Ending Date`),
      nchs_total_deaths = as_num(`Total Deaths`),
      pct_expected = as_num(`Percent of Expected Deaths`),
      across(all_of(count_cols), as_num)
    ) %>%
    mutate(
      unreported = coalesce(nchs_total_deaths == 0 & pct_expected == 0, FALSE),
      expected_deaths = if_else(!unreported & pct_expected > 0,
                                nchs_total_deaths / (pct_expected / 100), NA_real_)
    )

  flag_cols <- paste0("nchs_suppressed_flag_",
                      sub("_deaths$", "", sub("^nchs_", "", names(count_cols))))
  names(flag_cols) <- names(count_cols)

  # One row per state and week. Every state but New York is a single part; for
  # New York the state and NYC parts are summed. The flags are computed first
  # because the value columns below overwrite the raw columns they read.
  data <- data %>%
    group_by(state_name, time) %>%
    summarize(
      n_state_parts = sum(!is_nyc),
      unreported = any(unreported),
      across(c(nchs_total_deaths, expected_deaths), sum),
      across(all_of(names(count_cols)), ~ as.integer(any(is.na(.x))), .names = "flag__{.col}"),
      across(all_of(names(count_cols)), ~ sum(if_else(is.na(.x), 5, .x))),
      .groups = "drop"
    )
  if (any(data$n_state_parts != 1)) {
    stop("expected exactly one non-NYC row per state and week")
  }

  data <- data %>%
    mutate(
      across(c(nchs_total_deaths, expected_deaths, all_of(names(count_cols))),
             ~ if_else(unreported, NA_real_, .x)),
      across(starts_with("flag__"), ~ if_else(unreported, 0L, .x)),
      nchs_pct_expected_deaths = if_else(!is.na(expected_deaths) & expected_deaths > 0,
                                         round(nchs_total_deaths / expected_deaths * 100),
                                         NA_real_)
    ) %>%
    rename(!!!setNames(paste0("flag__", names(flag_cols)), flag_cols)) %>%
    select(-expected_deaths, -unreported, -n_state_parts) %>%
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

  if (anyNA(data_standard$time)) {
    stop("missing week ending dates")
  }
  if (anyDuplicated(data_standard[c("geography", "time")])) {
    stop("duplicate geography/time rows")
  }
  n_geo <- n_distinct(data_standard$geography)
  n_week <- n_distinct(data_standard$time)
  if (n_geo < 50 || nrow(data_standard) != n_geo * n_week) {
    stop("expected a complete geography by week grid, got ", nrow(data_standard),
         " rows for ", n_geo, " geographies and ", n_week, " weeks")
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
