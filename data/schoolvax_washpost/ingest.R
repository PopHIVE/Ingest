
library(dplyr)
library(vroom)
library(readr)
library(readxl)
library(digest)
library(data.table)

# Disable vroom's Arrow ALTREP globally to prevent vec_math.arrow_binary() errors
# This affects both this script AND dcf_datapackage_add() which runs afterward

# Load FIPS lookup table (faster than cdlTools::fips())
# Use altrep = FALSE and as.data.frame() to avoid arrow backend issues in dcf_process()
fips_lookup <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE, altrep = FALSE) %>%
  as.data.frame() %>%
  filter(nchar(geography) == 2) %>%  # State-level only
  select(geography, geography_name, state)

# TN county FIPS lookup (state FIPS "47" = Tennessee)
county_fips_tn <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE, altrep = FALSE) %>%
  as.data.frame() %>%
  filter(nchar(geography) == 5, substr(geography, 1, 2) == "47") %>%
  mutate(county_name_clean = tolower(sub(" County$", "", geography_name))) %>%
  select(geography, county_name_clean)

process <- dcf::dcf_process_record()


# -----------------------------------------------------------------------------
# 1. Download Washington Post school vaccination data
# -----------------------------------------------------------------------------

# URLs for the Washington Post data
base_url <- paste0(
  "https://raw.githubusercontent.com/washingtonpost/",
  "data-school-vaccination-rates/refs/heads/main/"
)
urls <- list(
  counties = paste0(base_url, "vaxrates_counties.csv"),
  schools = paste0(base_url, "vaxrates_schools.csv")
)

# Read files directly from URLs
# Use altrep = FALSE to avoid arrow backend issues with digest and string operations
# Force full materialization with as.data.frame() to prevent Arrow type issues in dcf_process()
counties_raw_temp <- vroom::vroom(urls$counties, show_col_types = FALSE, altrep = FALSE) %>%
  as.data.frame()

schools_raw_temp <- vroom::vroom(urls$schools, show_col_types = FALSE, altrep = FALSE) %>%
  as.data.frame() %>%
  dplyr::select(-address)

# Calculate hash based on data content to detect changes
current_wapo_state <- list(
  counties_hash = digest::digest(counties_raw_temp),
  schools_hash = digest::digest(schools_raw_temp)
)

# -----------------------------------------------------------------------------
# 2. Check TN state Kindergarten Survey Excel file for changes
# -----------------------------------------------------------------------------
tn_excel_path <- "raw/KMMRCoverage_County.xlsx"
current_tn_state <- list(hash = tools::md5sum(tn_excel_path)[[1]])

# Only process if WaPo data or TN state data has changed
if (!identical(process$wapo_state, current_wapo_state) || !identical(process$tn_state, current_tn_state)) {

  # ---------------------------------------------------------------------------
  # 2a. Process WaPo county-level data
  # ---------------------------------------------------------------------------

  counties_standard <- counties_raw_temp %>%
    # Select relevant columns and rename
    select(
      fips = fiptxt,
      state_name = state,
      county_name = county,
      year_2018_2019 = `2018-2019`,
      year_2019_2020 = `2019-2020`,
      year_2023_2024 = `2023-2024`,
      year_2024_2025 = `2024-2025`,
      pop_2023,
      prepand_herd,
      postpand_herd
    ) %>%
    # Convert fips to character first (before pivot)
    mutate(fips = as.character(fips)) %>%
    # Pivot to long format
    tidyr::pivot_longer(
      cols = starts_with("year_"),
      names_to = "school_year",
      values_to = "wapo_county_vax_rate",
      names_prefix = "year_"
    ) %>%

    # Convert school year to time (use September 1st as start of academic year)
    mutate(
      school_year = gsub("_", "-", school_year),
      year_start = as.integer(substr(school_year, 1, 4)),
      time = format(as.Date(paste0(year_start, "-09-01")), "%m-%d-%Y"),
      # Ensure FIPS is 5 digits with leading zeros
      geography = stringr::str_pad(fips, width = 5, side = "left", pad = "0")
    ) %>%
    # Remove NAs and filter valid data
    filter(!is.na(wapo_county_vax_rate)) %>%
    # Add herd immunity indicators
    mutate(
      wapo_prepand_herd = prepand_herd,
      wapo_postpand_herd = postpand_herd
    ) %>%
    # Select final columns
    select(
      geography,
      time,
      wapo_county_vax_rate,
      wapo_prepand_herd,
      wapo_postpand_herd
    ) %>%
    distinct() %>%
    group_by(geography, time) %>%
    mutate(id1 = row_number()) %>%
    filter(id1 == 1) %>%
    ungroup() %>%
    dplyr::select(-id1) %>%
    filter(!is.na(geography))

  # ---------------------------------------------------------------------------
  # 2b. Process TN state Kindergarten Survey data (2024-25)
  # Tennessee 2024-25 data replaces WaPo county data for TN
  # ---------------------------------------------------------------------------

  tn_raw <- readxl::read_excel(tn_excel_path, sheet = "Data") %>%
    as.data.frame()

  tn_standard <- tn_raw %>%
    mutate(county_lower = tolower(trimws(county))) %>%
    left_join(county_fips_tn, by = c("county_lower" = "county_name_clean")) %>%
    mutate(
      time = "09-01-2024",
      wapo_county_vax_rate = as.numeric(percent_mmr),
      wapo_prepand_herd = NA_character_,
      wapo_postpand_herd = NA_character_
    ) %>%
    filter(!is.na(geography)) %>%
    select(geography, time, wapo_county_vax_rate, wapo_prepand_herd, wapo_postpand_herd)

  # Remove WaPo TN county rows and replace with TN state source data
  counties_standard <- counties_standard %>%
    filter(!(substr(geography, 1, 2) == "47" )) %>%
    bind_rows(tn_standard)

  # ---------------------------------------------------------------------------
  # 2c. Process school-level data (unchanged from WaPo)
  # ---------------------------------------------------------------------------

  schools_standard <- schools_raw_temp %>%
    # Parse school year to extract start year
    mutate(
      year_start = as.integer(substr(year, 1, 4)),
      time = format(as.Date(paste0(year_start, "-09-01")), "%m-%d-%Y")
    ) %>%
    # Rename and standardize columns with wapo_ prefix
    rename(
      wapo_school_name = school_name,
      wapo_school_type = school_type,
      wapo_students_enrolled = students_enrolled,
      wapo_school_mmr_rate = mmr_rate,
      wapo_school_overall_rate = overall_rate,
      wapo_school_medical_exemption_rate = medical_exemption_rate,
      wapo_school_religious_exemption_rate = religious_exemption_rate,
      wapo_school_personal_exemption_rate = personal_exemption_rate,
      wapo_school_nonmedical_exemption_rate = nonmedical_exemption_rate,
      wapo_school_overall_exemption_rate = overall_exemption_rate,
      wapo_school_lat = LAT,
      wapo_school_lon = LON,
      wapo_school_county = county,
      wapo_school_state = state,
      wapo_school_grade = grade
    ) %>%
    # Convert state to FIPS (state-level only, no county FIPS in school data)
    left_join(fips_lookup, by = c("wapo_school_state" = "state")) %>%
    # Select final columns
    select(
      geography,
      time,
      wapo_school_name,
      wapo_school_type,
      wapo_students_enrolled,
      wapo_school_mmr_rate,
      wapo_school_overall_rate,
      wapo_school_medical_exemption_rate,
      wapo_school_religious_exemption_rate,
      wapo_school_personal_exemption_rate,
      wapo_school_nonmedical_exemption_rate,
      wapo_school_overall_exemption_rate,
      wapo_school_lat,
      wapo_school_lon,
      wapo_school_county,
      wapo_school_state,
      wapo_school_grade
    ) %>%
    distinct()

  # ---------------------------------------------------------------------------
  # 2d. Write standardized output files
  # ---------------------------------------------------------------------------

  # Convert to plain data.frame to avoid vctrs/Arrow issues when dcf reads metadata
  counties_standard <- as.data.frame(counties_standard)
  schools_standard <- as.data.frame(schools_standard)

  # Use data.table::fwrite() which produces cleaner CSV that avoids Arrow binary issues
  data.table::fwrite(counties_standard, "standard/data_counties.csv.gz", compress = "gzip")
  data.table::fwrite(schools_standard, "standard/data_schools.csv.gz", compress = "gzip")

  # ---------------------------------------------------------------------------
  # 2e. Record processed state
  # ---------------------------------------------------------------------------

  process$wapo_state <- current_wapo_state
  process$tn_state <- current_tn_state
  dcf::dcf_process_record(updated = process)

}
