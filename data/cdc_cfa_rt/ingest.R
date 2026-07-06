# =============================================================================
# CDC CFA Epidemic Trends and Rt Data Ingestion
# Source: https://data.cdc.gov/Public-Health-Surveillance/
#         CDC-Epidemic-Trends-and-Rt/5dqz-y4ea/
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record
process <- dcf::dcf_process_record()

# --- 1. Download raw data ---
raw_state <- dcf::dcf_download_cdc("5dqz-y4ea", "raw", process$raw_state)

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {

  # --- 2. Load FIPS lookup and read raw data ---
  all_fips <- vroom::vroom(
    "../../resources/all_fips.csv.gz",
    show_col_types = FALSE
  )

  state_fips_lookup <- all_fips |>
    filter(nchar(geography) == 2, !is.na(geography_name)) |>
    select(geography, geography_name)

  data_raw <- vroom::vroom(
    "raw/5dqz-y4ea.csv.xz",
    show_col_types = FALSE,
    col_types = list(
      median    = vroom::col_double(),
      lower     = vroom::col_double(),
      upper     = vroom::col_double(),
      p_growing = vroom::col_double()
    )
  ) %>%
  mutate( date = as.Date(date,"%m/%d/%Y"),
          as_of=as.Date(as_of,"%m/%d/%Y")
    )

  # --- 3. Transform data ---
  # Keep only the most recent model run per state. The national ("United
  # States") model runs on a slower/staggered cadence than the state models,
  # so a single global latest as_of would silently drop national estimates.
  data_latest <- data_raw |>
    group_by(state) |>
    filter(as_of == max(as_of, na.rm = TRUE)) |>
    ungroup()

  data_prepared <- data_latest |>
    mutate(
      disease_key = case_when(
        disease == "COVID-19"  ~ "covid",
        disease == "Influenza" ~ "flu",
        disease == "RSV"       ~ "rsv",
        TRUE ~ NA_character_
      ),
    ) |>
    rename(time= date) %>%
    filter(!is.na(disease_key)) |>
    left_join(state_fips_lookup, by = c("state" = "geography_name")) |>
    mutate(
      geography = case_when(
        state == "United States" ~ "00",
        !is.na(geography)        ~ geography,
        TRUE                     ~ NA_character_
      )
    ) |>
    filter(!is.na(geography), !is.na(median)) |>
    select(geography, time, disease_key, median, lower, upper, p_growing) |>
    distinct(geography, time, disease_key, .keep_all = TRUE)

  # Pivot to wide format: one row per geography/time, columns per disease
  data_wide <- data_prepared |>
    pivot_wider(
      names_from  = disease_key,
      values_from = c(median, lower, upper, p_growing),
      names_glue  = "cdc_rt_{disease_key}_{.value}"
    ) |>
    rename_with(~ sub("_median$", "", .x), ends_with("_median")) |>
    arrange(geography, time)

  # Guarantee a fixed column set even if a disease has zero estimates for the
  # whole period (e.g. flu/RSV fully "Not Estimated" off-season) - downstream
  # bundle_respiratory/build.R references these columns by name unconditionally
  expected_cols <- paste0(
    "cdc_rt_", rep(c("covid", "flu", "rsv"), each = 4),
    c("", "_lower", "_upper", "_p_growing")
  )
  missing_cols <- setdiff(expected_cols, names(data_wide))
  data_wide[missing_cols] <- NA_real_

  # --- 4. Write standardized output ---
  vroom::vroom_write(data_wide, "standard/data.csv.gz", delim = ",")

  # --- 5. Update process record ---
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
