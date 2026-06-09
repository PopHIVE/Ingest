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
  # Keep only rows from the single most recent model run (global max as_of)
  latest_as_of <- max(data_raw$as_of, na.rm = TRUE)
  data_latest <- data_raw |>
    filter(as_of == latest_as_of)

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
    filter(!is.na(geography)) |>
    select(geography, time, disease_key, median, lower, upper, p_growing) |>
    distinct(geography, time, disease_key, .keep_all = TRUE)

  # Pivot to wide format: one row per geography/time, columns per disease
  data_wide <- data_prepared |>
    pivot_wider(
      names_from  = disease_key,
      values_from = c(median, lower, upper, p_growing),
      names_glue  = "cdc_rt_{disease_key}_{.value}"
    ) |>
    rename(
      cdc_rt_covid = cdc_rt_covid_median,
      cdc_rt_flu   = cdc_rt_flu_median,
      cdc_rt_rsv   = cdc_rt_rsv_median
    ) |>
    arrange(geography, time)

  # --- 4. Write standardized output ---
  vroom::vroom_write(data_wide, "standard/data.csv.gz", delim = ",")

  # --- 5. Update process record ---
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
