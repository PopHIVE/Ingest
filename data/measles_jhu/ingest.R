# =============================================================================
# JHU Measles Tracking Team Data Ingestion
# Source: https://github.com/CSSEGISandData/measles_data
# =============================================================================

library(dplyr)
library(lubridate)

process <- dcf::dcf_process_record()

# Load FIPS lookup for state abbreviation to FIPS code conversion
fips_lookup <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE) %>%
  filter(nchar(geography) == 2) %>%  # State-level only
  select(geography, state)

# -----------------------------------------------------------------------------
# 1. Download raw data from GitHub
# -----------------------------------------------------------------------------
base_url <- "https://raw.githubusercontent.com/CSSEGISandData/measles_data/main/"

files <- c(
  "Top_states_time_series.csv",
  "measles_county_all_updates.csv",
  "data_sources_by_state.csv"
)

# Download all files
for (file in files) {
  url <- paste0(base_url, file)
  dest <- file.path("raw", file)

  # Download file
  download.file(url, dest, mode = "wb", quiet = TRUE)

  # Remove existing .xz if it exists
  unlink(paste0(dest, ".xz"))

  # Compress with xz
  system2("xz", c("-f", dest))
}

# Calculate hash for change detection
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "\\.csv",
  recursive = TRUE,
  full.names = TRUE
)))

# Check if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read and transform Top States Time Series data
  # ---------------------------------------------------------------------------
  top_states <- vroom::vroom("raw/Top_states_time_series.csv.xz", show_col_types = FALSE)

  # Transform to standard format for state-level weekly data
  top_states_standard <- top_states %>%
    # Convert to long format
    tidyr::pivot_longer(
      cols = c(KS_cases, NM_cases, TX_cases),
      names_to = "state",
      values_to = "value"
    ) %>%
    # Extract state abbreviation and convert to FIPS via lookup
    mutate(
      state_abbr = stringr::str_extract(state, "^[A-Z]+")
    ) %>%
    left_join(fips_lookup, by = c("state_abbr" = "state")) %>%
    # Use week_end date and convert to MM-DD-YYYY format
    mutate(
      # Week ends on Saturday (adjust if needed)
      week_end_date = as.Date(week_end),
      time = format(week_end_date, "%m-%d-%Y")
    ) %>%
    # Ensure value is numeric and select standard columns
    mutate(value = as.numeric(value)) %>%
    select(geography, time, value) %>%
    # Remove any NA values
    filter(!is.na(value), !is.na(geography))

  # ---------------------------------------------------------------------------
  # 3. Read and transform County-level data
  # ---------------------------------------------------------------------------
  county_data <- vroom::vroom("raw/measles_county_all_updates.csv.xz", show_col_types = FALSE)

  # Transform to standard format for county-level daily data
  county_standard <- county_data %>%
    # Pad FIPS codes to 5 digits
    mutate(
      geography = stringr::str_pad(as.character(location_id), width = 5, pad = "0")
    ) %>%
    # Convert date to MM-DD-YYYY format
    mutate(
      date_obj = as.Date(date),
      time = format(date_obj, "%m-%d-%Y")
    ) %>%
    # Ensure value is numeric
    mutate(value = as.numeric(value)) %>%
    # Select standard columns
    select(geography, time, value) %>%
    # Remove any NA values
    filter(!is.na(value), !is.na(geography))

  # Aggregate county data to weekly level (matching epiweek convention - Saturday)
  county_weekly <- county_standard %>%
    mutate(
      date_obj = as.Date(time, format = "%m-%d-%Y"),
      # Get the Saturday at the end of the epiweek
      week_end = ceiling_date(date_obj, "week", week_start = 7) - days(1)
    ) %>%
    group_by(geography, week_end) %>%
    summarize(
      value = as.numeric(sum(value, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      time = format(week_end, "%m-%d-%Y")
    ) %>%
    select(geography, time, value)

  # Aggregate county data to state level for comparison
  state_from_county <- county_weekly %>%
    mutate(
      state_fips = substr(geography, 1, 2)
    ) %>%
    group_by(state_fips, time) %>%
    summarize(
      value = as.numeric(sum(value, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    rename(geography = state_fips)

  # ---------------------------------------------------------------------------
  # 4. Combine all data sources
  # ---------------------------------------------------------------------------

  # Combine top states data with aggregated county data
  # Priority: use top_states data where available, supplement with county data
  all_states_weekly <- bind_rows(
    top_states_standard %>% mutate(source = "top_states"),
    state_from_county %>% mutate(source = "county_agg")
  ) %>%
    arrange(geography, time, desc(source)) %>%
    # Keep top_states data when both exist for same geography/time
    distinct(geography, time, .keep_all = TRUE) %>%
    select(geography, time, value)

  # Calculate national totals
  national_weekly <- all_states_weekly %>%
    group_by(time) %>%
    summarize(
      value = as.numeric(sum(value, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(geography = "00")

  # Combine state and national data
  state_final <- bind_rows(all_states_weekly, national_weekly) %>%
    arrange(geography, time)

  # ---------------------------------------------------------------------------
  # 5. Write standardized outputs
  # ---------------------------------------------------------------------------

  # State-level weekly data (primary output)
  vroom::vroom_write(
    state_final,
    "standard/data_state.csv.gz",
    delim = ","
  )

  # County-level weekly data
  vroom::vroom_write(
    county_weekly,
    "standard/data_county.csv.gz",
    delim = ","
  )

  # Combined (state + county) for comprehensive view
  combined_final <- bind_rows(
    state_final %>% mutate(geographic_level = "state"),
    county_weekly %>% mutate(geographic_level = "county")
  ) %>%
    select(-geographic_level) %>%
    arrange(geography, time)

  vroom::vroom_write(
    combined_final,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
