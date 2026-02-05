# =============================================================================
# Delphi FluView ILI Data Ingestion
# Source: CMU Delphi Epidata API - FluView endpoint
# Documentation: https://cmu-delphi.github.io/delphi-epidata/api/fluview.html
# =============================================================================

library(epidatr)
library(tidyverse)
library(lubridate)

process <- dcf::dcf_process_record()

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

  # Load FIPS crosswalk for faster lookup (replaces cdlTools::fips)
  fips_lookup <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE) %>%
    filter(nchar(geography) == 2) %>%  # Only state-level
    select(geography, state) %>%
    mutate(state = tolower(state))  # Convert to lowercase to match raw data

  data <- vroom::vroom('./raw/data.csv.xz', show_col_types = FALSE) %>%
    # Merge FIPS codes
    left_join(fips_lookup, by = c("region" = "state")) %>%
    mutate(
      # Convert region to FIPS code
      geography = case_when(
        region == "nat" ~ "00",
        !is.na(geography) ~ geography,  # Use merged FIPS code
        TRUE ~ NA_character_  # Should not happen if lookup is complete
      ),
      # Convert epiweek (YYYY-mm-dd Sunday) to Saturday end-of-week
      time = as.Date(epiweek) + 6
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
