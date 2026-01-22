# =============================================================================
# Bundle: Measles
# Combines: wastewater_measles, vaccine_exemptions_kiang, measles_jhu, mmr_healthmap,
#           measles_cdc, schoolvaxview (WaPo)
# Output: Two consolidated files in long format:
#   1. measles_state.parquet - State-level data with geography = state name
#   2. measles_county.parquet - County-level data with geography = county FIPS
# =============================================================================

library(dplyr)
library(tidyr)
library(arrow)
library(lubridate)

process <- dcf::dcf_process_record()
standard_files <- paste0("../", names(process$source_files))

# -----------------------------------------------------------------------------
# 1. Load FIPS lookup for state name conversion
# -----------------------------------------------------------------------------
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(fips = geography, state_name = geography_name)

# -----------------------------------------------------------------------------
# 2. Load standardized source files
# -----------------------------------------------------------------------------
wastewater_measles <- vroom::vroom('../wastewater_measles/standard/data.csv.gz', show_col_types = FALSE)
vaccine_exemptions <- vroom::vroom('../vaccine_exemptions_kiang/standard/data.csv.gz', show_col_types = FALSE)
vaccine_exemptions_county <- vroom::vroom('../vaccine_exemptions_kiang/standard/data_county.csv.gz', show_col_types = FALSE)
measles_jhu_state <- vroom::vroom('../measles_jhu/standard/data_state.csv.gz', show_col_types = FALSE)
measles_jhu_county <- vroom::vroom('../measles_jhu/standard/data_county.csv.gz', show_col_types = FALSE)
mmr_healthmap_state <- vroom::vroom('../mmr_healthmap/standard/data_state.csv.gz', show_col_types = FALSE)
mmr_healthmap_county <- vroom::vroom('../mmr_healthmap/standard/data_county.csv.gz', show_col_types = FALSE)
wapo_counties <- vroom::vroom('../schoolvaxview/standard/data_wapo_counties.csv.gz', show_col_types = FALSE)
measles_cdc <- vroom::vroom('../measles_cdc/standard/data.csv.gz', show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 3. Prepare state-level FIPS codes for filtering
# -----------------------------------------------------------------------------
valid_state_fips <- state_fips_lookup$fips

# =============================================================================
# STATE-LEVEL FILE
# =============================================================================

# -----------------------------------------------------------------------------
# 4a. Wastewater measles detection (state-level, weekly)
# -----------------------------------------------------------------------------
wastewater_state <- wastewater_measles %>%
  filter(geography %in% valid_state_fips) %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = isoweek(date)
  ) %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(
    geography = if_else(geography == "00", "United States", state_name)
  ) %>%
  select(geography, date, year, week, value = detection_rate) %>%
  filter(!is.na(value)) %>%
  mutate(source = "wastewater_detection_rate")

# -----------------------------------------------------------------------------
# 4b. Vaccine exemptions (state-level, annual)
# -----------------------------------------------------------------------------
exemptions_state <- vaccine_exemptions %>%
  filter(geography %in% valid_state_fips) %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = NA_integer_
  ) %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(
    geography = if_else(geography == "00", "United States", state_name)
  ) %>%
  select(geography, date, year, week, value = exemption_rate_mmr) %>%
  filter(!is.na(value)) %>%
  mutate(source = "vaccine_exemption_rate")

# -----------------------------------------------------------------------------
# 4c. JHU measles cases (state-level, weekly)
# -----------------------------------------------------------------------------
jhu_state <- measles_jhu_state %>%
  filter(geography %in% valid_state_fips) %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = isoweek(date)
  ) %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(
    geography = if_else(geography == "00", "United States", state_name)
  ) %>%
  select(geography, date, year, week, value) %>%
  filter(!is.na(value)) %>%
  mutate(source = "jhu_measles_cases")

# -----------------------------------------------------------------------------
# 4d. MMR coverage estimates (state-level, annual/point-in-time)
# -----------------------------------------------------------------------------
mmr_state <- mmr_healthmap_state %>%
  filter(geography %in% valid_state_fips) %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = NA_integer_
  ) %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(
    geography = if_else(geography == "00", "United States", state_name)
  ) %>%
  select(geography, date, year, week, value) %>%
  filter(!is.na(value)) %>%
  mutate(source = "mmr_coverage_healthmap")

# -----------------------------------------------------------------------------
# 4e. CDC national weekly cases (national only)
# -----------------------------------------------------------------------------
cdc_national <- measles_cdc %>%
  filter(geography == "00") %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = isoweek(date),
    geography = "United States"
  ) %>%
  select(geography, date, year, week, value) %>%
  filter(!is.na(value)) %>%
  mutate(source = "cdc_measles_cases")

# -----------------------------------------------------------------------------
# 5. Combine all state-level data into long format
# -----------------------------------------------------------------------------
measles_state_long <- bind_rows(
  wastewater_state,
  exemptions_state,
  jhu_state,
  mmr_state,
  cdc_national
) %>%
  arrange(geography, source, date) %>%
  select(geography, date, year, week, source, value)

# Write state-level parquet
arrow::write_parquet(
  measles_state_long,
  "dist/measles_state.parquet",
  compression = "snappy"
)

# =============================================================================
# COUNTY-LEVEL FILE
# =============================================================================

# -----------------------------------------------------------------------------
# 6a. JHU measles cases (county-level, weekly)
# -----------------------------------------------------------------------------
jhu_county <- measles_jhu_county %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = isoweek(date)
  ) %>%
  select(geography, date, year, week, value) %>%
  filter(!is.na(value)) %>%
  mutate(source = "jhu_measles_cases")

# -----------------------------------------------------------------------------
# 6b. MMR coverage estimates (county-level, point-in-time)
# -----------------------------------------------------------------------------
mmr_county <- mmr_healthmap_county %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = NA_integer_
  ) %>%
  select(geography, date, year, week, value) %>%
  filter(!is.na(value)) %>%
  mutate(source = "mmr_coverage_healthmap")

# -----------------------------------------------------------------------------
# 6c. Vaccine exemptions (county-level, annual)
# -----------------------------------------------------------------------------
exemptions_county <- vaccine_exemptions_county %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = NA_integer_
  ) %>%
  select(geography, date, year, week, value = exemption_rate_mmr) %>%
  filter(!is.na(value)) %>%
  mutate(source = "vaccine_exemption_rate")

# -----------------------------------------------------------------------------
# 6d. Washington Post county vaccination rates (annual)
# -----------------------------------------------------------------------------
wapo_county <- wapo_counties %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = NA_integer_
  ) %>%
  select(geography, date, year, week, value = wapo_county_vax_rate) %>%
  filter(!is.na(value)) %>%
  mutate(source = "wapo_county_vax_rate")

# -----------------------------------------------------------------------------
# 6e. Wastewater measles detection (county-level, weekly)
# -----------------------------------------------------------------------------
wastewater_county <- wastewater_measles %>%
  filter(nchar(geography) == 5) %>%  # County FIPS codes are 5 digits

  mutate(
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date),
    week = isoweek(date)
  ) %>%
  select(geography, date, year, week, value = detection_rate) %>%
  filter(!is.na(value)) %>%
  mutate(source = "wastewater_detection_rate")

# -----------------------------------------------------------------------------
# 7. Combine all county-level data into long format
# -----------------------------------------------------------------------------
measles_county_long <- bind_rows(
  jhu_county,
  mmr_county,
  exemptions_county,
  wapo_county,
  wastewater_county
) %>%
  arrange(geography, source, date) %>%
  select(geography, date, year, week, source, value)

# Write county-level parquet
arrow::write_parquet(
  measles_county_long,
  "dist/measles_county.parquet",
  compression = "snappy"
)

# =============================================================================
# Summary
# =============================================================================
cat("Measles bundle created:\n")
cat("  - State-level:", nrow(measles_state_long), "records from",
    length(unique(measles_state_long$source)), "sources\n")
cat("  - County-level:", nrow(measles_county_long), "records from",
    length(unique(measles_county_long$source)), "sources\n")

