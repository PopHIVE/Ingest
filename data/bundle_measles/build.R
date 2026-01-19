# =============================================================================
# Bundle: Measles
# Combines: wastewater_measles, vaccine_exemptions_kiang
# =============================================================================

library(dplyr)
library(arrow)
library(cdlTools)
library(lubridate)

process <- dcf::dcf_process_record()
standard_files <- paste0("../", names(process$source_files))

# -----------------------------------------------------------------------------
# 1. Load standardized source files
# -----------------------------------------------------------------------------
wastewater_measles <- vroom::vroom('../wastewater_measles/standard/data.csv.gz', show_col_types = FALSE)
vaccine_exemptions <- vroom::vroom('../vaccine_exemptions_kiang/standard/data.csv.gz', show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 2. Prepare state-level data
# -----------------------------------------------------------------------------
state_fips <- c(0, as.numeric(unique(tidycensus::fips_codes$state_code)))
state_fips <- stringr::str_pad(gsub("\\D", "", state_fips), width = 2, pad = "0")

# -----------------------------------------------------------------------------
# 3. Create overall trends view (detection rate over time)
# -----------------------------------------------------------------------------
overall_trends <- wastewater_measles %>%
  filter(geography %in% state_fips) %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(geography, fips, date, detection_rate, detection_count, sample_count, sewershed_count, population_served) %>%
  arrange(geography, date) %>%
  group_by(geography) %>%
  mutate(
    # 3-period moving average for detection rate
    detection_rate_smooth = zoo::rollapplyr(
      detection_rate,
      3,
      mean,
      partial = TRUE,
      na.rm = TRUE
    ),
    detection_rate_smooth = if_else(is.nan(detection_rate_smooth), NA, detection_rate_smooth),
    # Scale to 0-100 (if there's variation)
    detection_rate_smooth_scale = if_else(
      max(detection_rate_smooth, na.rm = TRUE) > min(detection_rate_smooth, na.rm = TRUE),
      (detection_rate_smooth - min(detection_rate_smooth, na.rm = TRUE)) /
        (max(detection_rate_smooth, na.rm = TRUE) - min(detection_rate_smooth, na.rm = TRUE)) * 100,
      50
    )
  ) %>%
  ungroup() %>%
  filter(!is.na(detection_rate))

arrow::write_parquet(
  overall_trends,
  "dist/measles_overall_trends.parquet",
  compression = "snappy"
)

# Also write CSV for compatibility
vroom::vroom_write(overall_trends, "dist/measles_overall_trends.csv.gz", ",")

# -----------------------------------------------------------------------------
# 4. Create geographic summary (latest data by state)
# -----------------------------------------------------------------------------
geographic_summary <- wastewater_measles %>%
  filter(geography %in% state_fips) %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  group_by(geography, fips) %>%
  filter(date == max(date, na.rm = TRUE)) %>%
  ungroup() %>%
  select(geography, fips, date, detection_rate, detection_count, sample_count, sewershed_count, population_served) %>%
  arrange(geography)

arrow::write_parquet(
  geographic_summary,
  "dist/measles_geographic_summary.parquet",
  compression = "snappy"
)

vroom::vroom_write(geographic_summary, "dist/measles_geographic_summary.csv.gz", ",")

# -----------------------------------------------------------------------------
# 5. Create time series of detection status
# -----------------------------------------------------------------------------
detection_status <- wastewater_measles %>%
  filter(geography %in% state_fips) %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y"),
    # Create detection status categories
    detection_status = case_when(
      sample_count == 0 ~ "No Data",
      detection_count == 0 ~ "No Detection",
      detection_count > 0 ~ "Detection"
    )
  ) %>%
  select(geography, fips, date, detection_status, detection_count, sample_count, sewershed_count) %>%
  arrange(geography, date)

arrow::write_parquet(
  detection_status,
  "dist/measles_detection_status.parquet",
  compression = "snappy"
)

vroom::vroom_write(detection_status, "dist/measles_detection_status.csv.gz", ",")

# -----------------------------------------------------------------------------
# 6. Create vaccine exemptions time series
# -----------------------------------------------------------------------------
exemptions_trends <- vaccine_exemptions %>%
  filter(geography %in% state_fips) %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date)
  ) %>%
  select(geography, fips, date, year, exemption_rate_mmr) %>%
  arrange(geography, date)

arrow::write_parquet(
  exemptions_trends,
  "dist/measles_exemptions_trends.parquet",
  compression = "snappy"
)

vroom::vroom_write(exemptions_trends, "dist/measles_exemptions_trends.csv.gz", ",")

# -----------------------------------------------------------------------------
# 7. Create combined view: exemptions with latest wastewater detection
# -----------------------------------------------------------------------------
latest_wastewater <- wastewater_measles %>%
  filter(geography %in% state_fips) %>%
  group_by(geography) %>%
  filter(time == max(time, na.rm = TRUE)) %>%
  ungroup() %>%
  select(
    geography,
    latest_detection_date = time,
    detection_flag,
    detection_rate,
    detection_count
  )

exemptions_with_wastewater <- vaccine_exemptions %>%
  filter(geography %in% state_fips) %>%
  left_join(latest_wastewater, by = "geography") %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y"),
    year = year(date)
  ) %>%
  select(
    geography,
    fips,
    date,
    year,
    exemption_rate_mmr,
    latest_detection_date,
    detection_flag,
    detection_rate,
    detection_count
  ) %>%
  arrange(geography, date)

arrow::write_parquet(
  exemptions_with_wastewater,
  "dist/measles_exemptions_with_wastewater.parquet",
  compression = "snappy"
)

vroom::vroom_write(exemptions_with_wastewater, "dist/measles_exemptions_with_wastewater.csv.gz", ",")
