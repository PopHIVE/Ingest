# =============================================================================
# Wastewater Measles Data Ingestion
# Source: CDC NWSS Measles Wastewater Surveillance
# https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/measles/nwssmeaslessitemapnocoords.csv
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
url <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/measles/nwssmeaslessitemapnocoords.csv"
raw_path <- "raw/measles.csv"
download.file(url, raw_path)
unlink(paste0(raw_path, ".xz"))
system2("xz", c("-f", raw_path))

# -----------------------------------------------------------------------------
# 2. Check raw state
# -----------------------------------------------------------------------------
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv",
  recursive = TRUE,
  full.names = TRUE
)))

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 3. Read and transform data
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom("raw/measles.csv.xz", delim = ",", show_col_types = FALSE)

  # Load FIPS lookup from resources (faster and more consistent than census API)
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # State FIPS lookup (2-digit codes)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name, state)

  # County FIPS lookup (5-digit codes)
  # County names in all_fips include "County" suffix (e.g., "Los Angeles County")
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5) %>%
    select(geography, geography_name, state) %>%
    mutate(
      # Create county_name without "County" suffix to match raw data format
      county_name = sub(" County$", "", geography_name),
      county_name = sub(" Parish$", "", county_name),  # Louisiana
      county_name = sub(" Borough$", "", county_name), # Alaska
      county_name = sub(" Census Area$", "", county_name), # Alaska
      county_name = sub(" Municipality$", "", county_name), # Alaska
      state_fips = substr(geography, 1, 2)
    )

  # Load census data for population weighting (still needed for weighted averages)
  state_pop <- dcf::dcf_load_census(
    out_dir = "../../resources",
    state_only = TRUE
  )

  # ---------------------------------------------------------------------------
  # 3a. Create site-level (county) data
  # ---------------------------------------------------------------------------

  data_county <- data_raw %>%
    # Filter to relevant rows
    filter(
      !is.na(`State/Territory`),
      !is.na(Counties_Served),
      Pathogen_Target == "Measles"
    ) %>%
    # Map state names to FIPS codes using lookup
    left_join(
      state_fips_lookup %>% select(state_fips = geography, geography_name),
      by = c("State/Territory" = "geography_name")
    ) %>%
    # Parse and format time
    mutate(
      time = as.Date(Update_Date_Time, format = "%m-%d-%Y %I:%M %p"),
      time = format(time, "%m-%d-%Y")
    ) %>%
    # Separate multiple counties (some sewersheds serve multiple counties)
    tidyr::separate_rows(Counties_Served, sep = ",\\s*") %>%
    mutate(county_name = trimws(Counties_Served)) %>%
    # Map to county FIPS codes
    left_join(
      county_fips_lookup %>% select(geography, state_fips, county_name),
      by = c("state_fips", "county_name")
    ) %>%
    # Keep only successfully mapped counties
    filter(!is.na(geography)) %>%
    # Keep site-level data with sewershed ID
    mutate(
      sewershed_id = Sewershed,
      detection_status = Detection_Category,
      detection_count = Detection_Count,
      sample_count = Sample_Count,
      population_served = Population_Served,
      # Binary detection indicator: 1 if any detection, 0 otherwise
      detection_flag = if_else(Detection_Count > 0, 1, 0)
    ) %>%
    select(
      geography,
      time,
      sewershed_id,
      detection_status,
      detection_count,
      detection_flag,
      sample_count,
      population_served
    ) %>%
    # Remove duplicates (in case a sewershed was counted multiple times)
    distinct()

  # ---------------------------------------------------------------------------
  # 3b. Create state-level data (aggregated)
  # ---------------------------------------------------------------------------
  data_state <- data_raw %>%
    # Filter to relevant rows
    filter(
      !is.na(`State/Territory`),
      Pathogen_Target == "Measles"
    ) %>%
    # Map state names to FIPS codes using lookup
    left_join(
      state_fips_lookup %>% select(geography, geography_name),
      by = c("State/Territory" = "geography_name")
    ) %>%
    # Parse and format time
    mutate(
      time = as.Date(Update_Date_Time, format = "%m-%d-%Y %I:%M %p"),
      time = format(time, "%m-%d-%Y")
    ) %>%
    # Aggregate by geography and time
    group_by(geography, time) %>%
    summarize(
      # Count of sewersheds with detection
      detection_count = sum(Detection_Count, na.rm = TRUE),
      # Total sample count
      sample_count = sum(Sample_Count, na.rm = TRUE),
      # Detection rate (detections per sample)
      detection_rate = if_else(
        sample_count > 0,
        detection_count / sample_count * 100,
        NA_real_
      ),
      # Binary detection indicator: 1 if any detection in the state, 0 otherwise
      detection_flag = if_else(sum(Detection_Count, na.rm = TRUE) > 0, 1, 0),
      # Number of sewersheds reporting
      sewershed_count = n(),
      # Population served (sum)
      population_served = sum(Population_Served, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    # Remove rows with missing geography
    filter(!is.na(geography))

  # Calculate national average (population-weighted)
  nat_ave <- data_state %>%
    left_join(state_pop, by = c('geography' = 'GEOID')) %>%
    group_by(time) %>%
    summarize(
      # Population-weighted detection rate
      detection_rate = weighted.mean(
        detection_rate,
        Total,
        na.rm = TRUE
      ),
      # Sum of counts
      detection_count = sum(detection_count, na.rm = TRUE),
      sample_count = sum(sample_count, na.rm = TRUE),
      # Binary detection indicator for national level
      detection_flag = if_else(sum(detection_count, na.rm = TRUE) > 0, 1, 0),
      sewershed_count = sum(sewershed_count, na.rm = TRUE),
      population_served = sum(population_served, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(geography = '00')

  # Combine state and national data
  data_state_combined <- bind_rows(data_county, data_state, nat_ave)

  # ---------------------------------------------------------------------------
  # 4. Write standardized outputs
  # ---------------------------------------------------------------------------
  # State-level aggregated data
  vroom::vroom_write(
    data_state_combined,
    "standard/data_state.csv.gz",
    delim = ","
  )

  # County-level site data
  vroom::vroom_write(
    data_county,
    "standard/data_county.csv.gz",
    delim = ","
  )

  # Also write combined as main data file (for backwards compatibility)
  vroom::vroom_write(
    data_state_combined,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
