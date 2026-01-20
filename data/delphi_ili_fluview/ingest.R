# =============================================================================
# Delphi FluView ILI Data Ingestion
# Source: CMU Delphi Epidata API - FluView endpoint
# Documentation: https://cmu-delphi.github.io/delphi-epidata/api/fluview.html
# =============================================================================

library(epidatr)
library(tidyverse)
library(lubridate)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Download raw data from Delphi FluView API
# -----------------------------------------------------------------------------

# National data (nat = national)
epidata_fluview_nat <- pub_fluview(
  regions = "nat",
  epiweeks = epirange(199740, 202901)
)

# State data (all 50 states + DC)
# FluView uses state abbreviations for regions
epidata_fluview_state <- pub_fluview(
  regions = c(state.abb, "DC"),
  epiweeks = epirange(199740, 202901)
)

# Combine and save raw data
all_raw <- bind_rows(epidata_fluview_nat, epidata_fluview_state)
vroom::vroom_write(all_raw, "raw/data.csv.xz", ",")

# -----------------------------------------------------------------------------
# 2. Check raw state and process if changed
# -----------------------------------------------------------------------------
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv.xz",
  recursive = TRUE,
  full.names = TRUE
)))

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 3. Read and transform data
  # ---------------------------------------------------------------------------
  data <- vroom::vroom('./raw/data.csv.xz', show_col_types = FALSE) %>%
    mutate(
      # Convert region to FIPS code
      geography = case_when(
        region == "nat" ~ "00",
        region == "DC" ~ "11",
        TRUE ~ sprintf("%02d", cdlTools::fips(region, to = 'fips'))
      ),
      # Convert epiweek to date (Saturday at end of week)
      # Epiweek format: YYYYWW (e.g., 202301 = 2023 week 1)
      epiweek_year = as.integer(substr(as.character(epiweek), 1, 4)),
      epiweek_week = as.integer(substr(as.character(epiweek), 5, 6)),
      # Calculate Saturday ending the epiweek
      # MMWR week 1 always contains Jan 4, and weeks run Sunday-Saturday
      # Find Jan 4 of the year, get its MMWR week start (Sunday), then add weeks
      jan4 = as.Date(paste0(epiweek_year, "-01-04")),
      # Find the Sunday of the week containing Jan 4
      jan4_wday = lubridate::wday(jan4, week_start = 7),  # Sunday = 1
      week1_sunday = jan4 - (jan4_wday - 1),
      # Calculate the Saturday of the requested epiweek
      time = week1_sunday + (epiweek_week - 1) * 7 + 6  # +6 to get Saturday
    ) %>%
    # Select and rename columns to standard format
    # wili = weighted ILI percentage, ili = unweighted ILI percentage
    select(
      geography,
      time,
      delphi_fluview_wili = wili,      # Weighted percent ILI
      delphi_fluview_ili = ili,         # Unweighted percent ILI
      delphi_fluview_num_ili = num_ili, # Count of ILI cases
      delphi_fluview_num_patients = num_patients, # Total patients seen
      delphi_fluview_num_providers = num_providers # Number of reporting providers
    ) %>%
    # Remove any rows with missing geography or time
    filter(!is.na(geography) & !is.na(time)) %>%
    # Arrange by geography and time
    arrange(geography, time)

  # ---------------------------------------------------------------------------
  # 4. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
