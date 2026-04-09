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
  "measles_county_all_updates_detailed.csv",
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
      cols = c( NM_cases, TX_cases),
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
      week_end_date = as.Date(week_end, "%m/%d/%y"),
      time = format(week_end_date)
    ) %>%
    # Ensure value is numeric and select standard columns
    mutate(value = as.numeric(value)) %>%
    select(geography, time, value) %>%
    # Remove any NA values
    filter(!is.na(value), !is.na(geography))

  # ---------------------------------------------------------------------------
  # 3. Read and transform County-level data
  # ---------------------------------------------------------------------------
  county_data <- vroom::vroom("raw/measles_county_all_updates_detailed.csv.xz", show_col_types = FALSE) %>%
    filter(outcome_type == 'case_lab-confirmed')
    


  # Transform to standard format for county-level daily data
  # Manual FIPS mapping for non-standard geographic regions (location_id = 0).
  # These regions span multiple counties; each is mapped to the primary/largest
  # county in the region.
  manual_fips_map <- c(
    "Upstate, South Carolina"                     = "45045",  # Greenville County, SC
    "Southwest Health District, Utah"             = "49053",  # Washington County, UT
    "Bear River, Utah"                            = "49005",  # Cache County, UT
    "Central, Utah"                               = "49041",  # Sevier County, UT
    "Southeast Health District, Utah"             = "49007",  # Carbon County, UT
    "Mid-Cumberland Region, Tennessee"            = "47037",  # Davidson County, TN
    "Nashville-Davidson County Region, Tennessee" = "47037",  # Davidson County, TN
    "Upper Cumberland Region, Tennessee"          = "47141",  # Putnam County, TN
    "Central Region, Virginia"                    = "51087",  # Henrico County, VA
    "Eastern Region, Virginia"                    = "51810",  # Virginia Beach city, VA
    "Northern Region, Virginia"                   = "51059",  # Fairfax County, VA
    "Northwest Region, Virginia"                  = "51171",  # Shenandoah County, VA
    "Region 9, Louisiana"                         = "22071"   # Orleans Parish, LA
  )

  # Manual FIPS mapping for non-standard regions that use state FIPS codes as
  # location_id (values 1-999). Regional entries are mapped to the primary county;
  # unknown-county entries use the 2-digit state FIPS to indicate state-level.
  regional_fips_map <- c(
    "Central, Iowa"                     = "19153",  # Polk County, IA
    "Eastern, Iowa"                     = "19113",  # Linn County, IA
    "Western, Iowa"                     = "19193",  # Woodbury County, IA
    "North, Alabama"                    = "01089",  # Madison County, AL
    "Twin Cities Metro Area, Minnesota" = "27053",  # Hennepin County, MN
    "Coastal Health District, Georgia"  = "13051",  # Chatham County, GA
    "Metro East , Illinois"             = "17163",  # St. Clair County, IL
    "Unknown County, California"        = "06",     # State-level, county unknown
    "Unknown County, Idaho"             = "16",     # State-level, county unknown
    "Unknown County, Illinois"          = "17",     # State-level, county unknown
    "Unknown County, Kansas"            = "20",     # State-level, county unknown
    "Unknown County, Kentucky"          = "21067",  # Fayette County, KY
    "Unknown County, Minnesota"         = "27",     # State-level, county unknown
    "Unknown County, Nebraska"          = "31141",  # Platte County, NE
    "Unknown County, Oregon"            = "41",     # State-level, county unknown
    "Unknown County, Pennsylvania"      = "42",     # State-level, county unknown
    "Unknown County, Texas"             = "48"      # State-level, county unknown
  )

  county_standard <- county_data %>%
    mutate(
      geography = case_when(
        location_id == 0 ~
          manual_fips_map[location_name],
        # Date-specific mapping for New Jersey unknown county
        location_name == "Unknown County, New Jersey" & date == "2025-08-29" ~
          "34003",  # Bergen County, NJ
        location_name == "Unknown County, New Jersey" & date == "2025-10-29" ~
          "34",     # State-level, county unknown
        # Regional/unknown entries using state FIPS as location_id
        location_name %in% names(regional_fips_map) ~
          regional_fips_map[location_name],
        # Standard county FIPS: pad location_id to 5 digits
        TRUE ~
          stringr::str_pad(as.character(location_id), width = 5, pad = "0")
      )
    ) %>%
    # Convert date to MM-DD-YYYY format
    mutate(
      date_obj = as.Date(date),
      time = format(date_obj, "%Y-%m-%d")
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
      date_obj = as.Date(time, format = "%Y-%m-%d"),
      # Get the Saturday at the end of the epiweek
      week_end = ceiling_date(date_obj, "week", week_start = 7) - days(1)
    ) %>%
    group_by(geography, week_end) %>%
    summarize(
      value = as.numeric(sum(value, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      time = format(week_end, "%Y-%m-%d")
    ) %>%
    select(geography, time, value) %>%
     tidyr::complete(geography, time, fill = list(value = 0)) %>%
    arrange(geography, time)

  # Aggregate county data to state level for comparison
  state_from_county <- county_weekly %>%
    mutate(
      state_fips = substr(geography, 1, 2)
    ) %>%
    filter(state_fips != "00") %>%  # Exclude any non-county entries
    group_by(state_fips, time) %>%
    summarize(
      value = as.numeric(sum(value, na.rm = TRUE)),
      .groups = "drop"
    )%>%
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
      tidyr::complete(geography, time, fill = list(value = 0)) %>%
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

  # Combined (state + county) for comprehensive view, filling missing geography/time combos with 0
  combined_final <- bind_rows(
    state_final %>% mutate(geographic_level = "state"),
    county_weekly %>% mutate(geographic_level = "county")
  ) %>%
    select(-geographic_level) %>%
    tidyr::complete(geography, time, fill = list(value = 0)) %>%
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
