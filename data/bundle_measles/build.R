# =============================================================================
# Bundle: Measles
# Combines: wastewater_measles, vaccine_exemptions_kiang, measles_jhu, mmr_healthmap,
#           measles_cdc
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
measles_jhu_state <- vroom::vroom('../measles_jhu/standard/data_state.csv.gz', show_col_types = FALSE)
measles_jhu_county <- vroom::vroom('../measles_jhu/standard/data_county.csv.gz', show_col_types = FALSE)
mmr_healthmap_state <- vroom::vroom('../mmr_healthmap/standard/data_state.csv.gz', show_col_types = FALSE)
mmr_healthmap_county <- vroom::vroom('../mmr_healthmap/standard/data_county.csv.gz', show_col_types = FALSE)
mmr_healthmap_zcta <- vroom::vroom('../mmr_healthmap/standard/data_zcta.csv.gz', show_col_types = FALSE)
wapo_counties <- vroom::vroom('../schoolvaxview/standard/data_wapo_counties.csv.gz', show_col_types = FALSE)
wapo_schools <- vroom::vroom('../schoolvaxview/standard/data_wapo_schools.csv.gz', show_col_types = FALSE)
measles_cdc <- vroom::vroom('../measles_cdc/standard/data.csv.gz', show_col_types = FALSE)

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

# -----------------------------------------------------------------------------
# 8. Create JHU case data time series (state-level)
# -----------------------------------------------------------------------------
jhu_cases_state <- measles_jhu_state %>%
  filter(geography %in% state_fips) %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(geography, fips, date, cases = value) %>%
  arrange(geography, date) %>%
  group_by(geography) %>%
  mutate(
    # Cumulative cases
    cumulative_cases = cumsum(cases),
    # 3-week moving average
    cases_smooth = zoo::rollapplyr(
      cases,
      3,
      mean,
      partial = TRUE,
      na.rm = TRUE
    ),
    cases_smooth = if_else(is.nan(cases_smooth), NA, cases_smooth)
  ) %>%
  ungroup()

arrow::write_parquet(
  jhu_cases_state,
  "dist/measles_jhu_cases_state.parquet",
  compression = "snappy"
)

vroom::vroom_write(jhu_cases_state, "dist/measles_jhu_cases_state.csv.gz", ",")

# -----------------------------------------------------------------------------
# 9. Create JHU case data time series (county-level)
# -----------------------------------------------------------------------------
jhu_cases_county <- measles_jhu_county %>%
  mutate(
    # Extract state and county information
    state_fips = substr(geography, 1, 2),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(county_fips = geography, state_fips, date, cases = value) %>%
  arrange(county_fips, date) %>%
  group_by(county_fips) %>%
  mutate(
    # Cumulative cases
    cumulative_cases = cumsum(cases),
    # 3-week moving average
    cases_smooth = zoo::rollapplyr(
      cases,
      3,
      mean,
      partial = TRUE,
      na.rm = TRUE
    ),
    cases_smooth = if_else(is.nan(cases_smooth), NA, cases_smooth)
  ) %>%
  ungroup()

arrow::write_parquet(
  jhu_cases_county,
  "dist/measles_jhu_cases_county.parquet",
  compression = "snappy"
)

vroom::vroom_write(jhu_cases_county, "dist/measles_jhu_cases_county.csv.gz", ",")

# -----------------------------------------------------------------------------
# 10. Create combined view: JHU cases with wastewater detection
# -----------------------------------------------------------------------------
latest_wastewater_for_cases <- wastewater_measles %>%
  filter(geography %in% state_fips) %>%
  group_by(geography) %>%
  filter(time == max(time, na.rm = TRUE)) %>%
  ungroup() %>%
  select(
    geography,
    latest_wastewater_date = time,
    wastewater_detection_flag = detection_flag,
    wastewater_detection_rate = detection_rate,
    wastewater_detection_count = detection_count
  )

jhu_cases_with_wastewater <- jhu_cases_state %>%
  left_join(latest_wastewater_for_cases, by = c("fips" = "geography")) %>%
  select(
    geography,
    fips,
    date,
    cases,
    cumulative_cases,
    cases_smooth,
    latest_wastewater_date,
    wastewater_detection_flag,
    wastewater_detection_rate,
    wastewater_detection_count
  ) %>%
  arrange(geography, date)

arrow::write_parquet(
  jhu_cases_with_wastewater,
  "dist/measles_jhu_cases_with_wastewater.parquet",
  compression = "snappy"
)

vroom::vroom_write(jhu_cases_with_wastewater, "dist/measles_jhu_cases_with_wastewater.csv.gz", ",")

# -----------------------------------------------------------------------------
# 11. Create combined comprehensive view: cases + wastewater + exemptions
# -----------------------------------------------------------------------------
latest_exemptions <- vaccine_exemptions %>%
  filter(geography %in% state_fips) %>%
  group_by(geography) %>%
  filter(time == max(time, na.rm = TRUE)) %>%
  ungroup() %>%
  select(
    geography,
    latest_exemption_date = time,
    exemption_rate_mmr
  )

measles_comprehensive <- jhu_cases_state %>%
  left_join(latest_wastewater_for_cases, by = c("fips" = "geography")) %>%
  left_join(latest_exemptions, by = c("fips" = "geography")) %>%
  select(
    geography,
    fips,
    date,
    cases,
    cumulative_cases,
    cases_smooth,
    latest_wastewater_date,
    wastewater_detection_flag,
    wastewater_detection_rate,
    wastewater_detection_count,
    latest_exemption_date,
    exemption_rate_mmr
  ) %>%
  arrange(geography, date)

arrow::write_parquet(
  measles_comprehensive,
  "dist/measles_comprehensive.parquet",
  compression = "snappy"
)

vroom::vroom_write(measles_comprehensive, "dist/measles_comprehensive.csv.gz", ",")

# -----------------------------------------------------------------------------
# 12. Create MMR coverage views (state-level)
# -----------------------------------------------------------------------------
mmr_coverage_state <- mmr_healthmap_state %>%
  rename(fips = geography) %>%
  mutate(
    geography = cdlTools::fips(fips, to = "Name"),
    geography = if_else(fips == '00', 'United States', geography),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(geography, fips, date, mmr_coverage = value) %>%
  arrange(geography)

arrow::write_parquet(
  mmr_coverage_state,
  "dist/measles_mmr_coverage_state.parquet",
  compression = "snappy"
)

vroom::vroom_write(mmr_coverage_state, "dist/measles_mmr_coverage_state.csv.gz", ",")

# -----------------------------------------------------------------------------
# 13. Create MMR coverage views (county-level)
# -----------------------------------------------------------------------------
mmr_coverage_county <- mmr_healthmap_county %>%
  mutate(
    state_fips = substr(geography, 1, 2),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(
    county_fips = geography,
    state_fips,
    date,
    mmr_coverage = value,
    risk_level,
    local_i,
    p_value,
    spatial_cluster
  ) %>%
  arrange(county_fips)

arrow::write_parquet(
  mmr_coverage_county,
  "dist/measles_mmr_coverage_county.parquet",
  compression = "snappy"
)

vroom::vroom_write(mmr_coverage_county, "dist/measles_mmr_coverage_county.csv.gz", ",")

# -----------------------------------------------------------------------------
# 14. Create MMR coverage views (ZCTA/ZIP code level)
# -----------------------------------------------------------------------------
mmr_coverage_zcta <- mmr_healthmap_zcta %>%
  mutate(
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(
    zcta = geography,
    date,
    mmr_coverage = value,
    risk_level,
    spatial_cluster,
    population_sample
  ) %>%
  arrange(zcta)

arrow::write_parquet(
  mmr_coverage_zcta,
  "dist/measles_mmr_coverage_zcta.parquet",
  compression = "snappy"
)

vroom::vroom_write(mmr_coverage_zcta, "dist/measles_mmr_coverage_zcta.csv.gz", ",")

# -----------------------------------------------------------------------------
# 15. Create comprehensive view with MMR coverage (state-level)
# -----------------------------------------------------------------------------
mmr_coverage_summary <- mmr_healthmap_state %>%
  rename(fips = geography) %>%
  mutate(date = as.Date(time, format = "%m-%d-%Y")) %>%
  select(fips, mmr_coverage_date = date, mmr_coverage = value)

measles_comprehensive_with_mmr <- measles_comprehensive %>%
  left_join(mmr_coverage_summary, by = "fips") %>%
  select(
    geography,
    fips,
    date,
    cases,
    cumulative_cases,
    cases_smooth,
    latest_wastewater_date,
    wastewater_detection_flag,
    wastewater_detection_rate,
    wastewater_detection_count,
    latest_exemption_date,
    exemption_rate_mmr,
    mmr_coverage_date,
    mmr_coverage
  ) %>%
  arrange(geography, date)

arrow::write_parquet(
  measles_comprehensive_with_mmr,
  "dist/measles_comprehensive_with_mmr.parquet",
  compression = "snappy"
)

vroom::vroom_write(measles_comprehensive_with_mmr, "dist/measles_comprehensive_with_mmr.csv.gz", ",")

# -----------------------------------------------------------------------------
# 16. Create Washington Post county vaccination rates view
# -----------------------------------------------------------------------------
wapo_counties_formatted <- wapo_counties %>%
  mutate(
    county_fips = geography,
    state_fips = substr(geography, 1, 2),
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(
    county_fips,
    state_fips,
    date,
    wapo_county_vax_rate,
    wapo_prepand_herd,
    wapo_postpand_herd
  ) %>%
  arrange(county_fips, date)

arrow::write_parquet(
  wapo_counties_formatted,
  "dist/measles_wapo_counties.parquet",
  compression = "snappy"
)

vroom::vroom_write(wapo_counties_formatted, "dist/measles_wapo_counties.csv.gz", ",")

# -----------------------------------------------------------------------------
# 17. Create Washington Post school vaccination rates view
# -----------------------------------------------------------------------------
wapo_schools_formatted <- wapo_schools %>%
  mutate(
    state_fips = geography,
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(
    state_fips,
    date,
    wapo_school_name,
    wapo_school_type,
    wapo_school_county,
    wapo_school_state,
    wapo_school_grade,
    wapo_students_enrolled,
    wapo_school_mmr_rate,
    wapo_school_overall_rate,
    wapo_school_medical_exemption_rate,
    wapo_school_religious_exemption_rate,
    wapo_school_personal_exemption_rate,
    wapo_school_nonmedical_exemption_rate,
    wapo_school_overall_exemption_rate,
    wapo_school_lat,
    wapo_school_lon
  ) %>%
  arrange(state_fips, wapo_school_county, wapo_school_name, date)

arrow::write_parquet(
  wapo_schools_formatted,
  "dist/measles_wapo_schools.parquet",
  compression = "snappy"
)

vroom::vroom_write(wapo_schools_formatted, "dist/measles_wapo_schools.csv.gz", ",")

# -----------------------------------------------------------------------------
# 18. Create combined view: Washington Post county data with JHU cases
# -----------------------------------------------------------------------------
latest_jhu_county <- measles_jhu_county %>%
  group_by(geography) %>%
  filter(time == max(time, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(
    latest_case_date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(
    county_fips = geography,
    latest_case_date,
    latest_cases = value,
    latest_cumulative_cases = cumulative_cases
  )

wapo_with_cases <- wapo_counties_formatted %>%
  left_join(latest_jhu_county, by = "county_fips") %>%
  select(
    county_fips,
    state_fips,
    date,
    wapo_county_vax_rate,
    wapo_prepand_herd,
    wapo_postpand_herd,
    latest_case_date,
    latest_cases,
    latest_cumulative_cases
  ) %>%
  arrange(county_fips, date)

arrow::write_parquet(
  wapo_with_cases,
  "dist/measles_wapo_counties_with_cases.parquet",
  compression = "snappy"
)

vroom::vroom_write(wapo_with_cases, "dist/measles_wapo_counties_with_cases.csv.gz", ",")

# -----------------------------------------------------------------------------
# 19. Create CDC weekly case data time series (national-level)
# -----------------------------------------------------------------------------
cdc_cases_national <- measles_cdc %>%
  filter(geography == "00") %>%
  rename(fips = geography) %>%
  mutate(
    geography = "United States",
    date = as.Date(time, format = "%m-%d-%Y")
  ) %>%
  select(geography, fips, date, cases = value) %>%
  arrange(date) %>%
  mutate(
    # Cumulative cases
    cumulative_cases = cumsum(cases),
    # 3-week moving average
    cases_smooth = zoo::rollapplyr(
      cases,
      3,
      mean,
      partial = TRUE,
      na.rm = TRUE
    ),
    cases_smooth = if_else(is.nan(cases_smooth), NA, cases_smooth)
  )

arrow::write_parquet(
  cdc_cases_national,
  "dist/measles_cdc_cases_national.parquet",
  compression = "snappy"
)

vroom::vroom_write(cdc_cases_national, "dist/measles_cdc_cases_national.csv.gz", ",")
