# =============================================================================
# Wastewater Measles Data Ingestion
# Source: CDC NWSS Measles Wastewater Surveillance
# https://data.cdc.gov/d/akvg-8vrb
# =============================================================================

library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
raw_state <- dcf::dcf_download_cdc(
  "akvg-8vrb",
  "raw",
  process$raw_state
)

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Load FIPS lookup
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # State-level lookup (for 2-letter abbreviations)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state) %>%
    mutate(state = tolower(state))  # Match lowercase abbreviations in raw data

  # ---------------------------------------------------------------------------
  # 3. Read and transform data
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom("raw/akvg-8vrb.csv.xz", delim = ",", show_col_types = FALSE) %>%
    mutate(
      detected = if_else(pcr_target_avg_conc > 0, 1, 0),
      # Convert to Saturday at end of week (epiweek convention)
      # floor_date with week_start=7 (Sunday) gives start of week, add 6 days for Saturday
      weekdate = lubridate::floor_date(as.Date(sample_collect_date), unit = "week", week_start = 7) + 6
    )

  # ---------------------------------------------------------------------------
  # 4. State-level aggregation
  # ---------------------------------------------------------------------------
  data_state <- data_raw %>%
    group_by(wwtp_jurisdiction, weekdate) %>%
    summarize(
      detection_count = sum(detected, na.rm = TRUE),
      sample_count = n(),
      population_served = sum(population_served, na.rm = TRUE),
      detection_rate = if_else(
        sample_count > 0,
        detection_count / sample_count * 100,
        NA_real_
      ),
      .groups = "drop"
    ) %>%
    # Convert state abbreviation to FIPS code
    left_join(state_fips_lookup, by = c("wwtp_jurisdiction" = "state")) %>%
    # Format time as MM-DD-YYYY
    mutate(
      time = format(weekdate, "%m-%d-%Y")
    ) %>%
    select(
      geography,
      time,
      ww_detection_rate = detection_rate,
      ww_detection_count = detection_count,
      ww_sample_count = sample_count,
      ww_population_served = population_served
    ) %>%
    filter(!is.na(geography))  # Remove any unmatched states

  # ---------------------------------------------------------------------------
  # 5. National-level aggregation
  # ---------------------------------------------------------------------------
  data_national <- data_raw %>%
    group_by(weekdate) %>%
    summarize(
      detection_count = sum(detected, na.rm = TRUE),
      sample_count = n(),
      population_served = sum(population_served, na.rm = TRUE),
      detection_rate = if_else(
        sample_count > 0,
        detection_count / sample_count * 100,
        NA_real_
      ),
      .groups = "drop"
    ) %>%
    mutate(
      geography = "00",
      time = format(weekdate, "%m-%d-%Y")
    ) %>%
    select(
      geography,
      time,
      ww_detection_rate = detection_rate,
      ww_detection_count = detection_count,
      ww_sample_count = sample_count,
      ww_population_served = population_served
    )

  # Combine state and national
  data_state_final <- bind_rows(data_national, data_state) %>%
    arrange(geography, time)

  # ---------------------------------------------------------------------------
  # 6. County-level aggregation
  # ---------------------------------------------------------------------------
  # Expand rows for sites serving multiple counties
  data_county <- data_raw %>%
    # Split county_fips on ", " and expand to one row per county
    mutate(county_fips_list = strsplit(county_fips, ", ")) %>%
    unnest(county_fips_list) %>%
    rename(geography = county_fips_list) %>%
    # Aggregate by county and week
    group_by(geography, weekdate) %>%
    summarize(
      detection_count = sum(detected, na.rm = TRUE),
      sample_count = n(),
      population_served = sum(population_served, na.rm = TRUE),
      detection_rate = if_else(
        sample_count > 0,
        detection_count / sample_count * 100,
        NA_real_
      ),
      .groups = "drop"
    ) %>%
    # Format time as MM-DD-YYYY
    mutate(
      time = format(weekdate, "%m-%d-%Y")
    ) %>%
    select(
      geography,
      time,
      ww_detection_rate = detection_rate,
      ww_detection_count = detection_count,
      ww_sample_count = sample_count,
      ww_population_served = population_served
    ) %>%
    filter(!is.na(geography) & geography != "") %>%
    arrange(geography, time)

  # ---------------------------------------------------------------------------
  # 7. Write standardized outputs
  # ---------------------------------------------------------------------------
  # Create standard directory if it doesn't exist
  if (!dir.exists("standard")) {
    dir.create("standard")
  }

  vroom::vroom_write(
    data_state_final,
    "standard/data.csv.gz",
    delim = ","
  )

  vroom::vroom_write(
    data_county,
    "standard/data_county.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 8. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

