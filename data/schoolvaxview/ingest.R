# =============================================================================
# SchoolVaxView Data Ingestion
# Sources:
#   1. CDC SchoolVaxView (Socrata API)
#   2. Washington Post School Vaccination Rates (GitHub)
# =============================================================================

library(dplyr)
library(vroom)
library(digest)

# Load FIPS lookup table (faster than cdlTools::fips())
fips_lookup <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE) %>%
  filter(nchar(geography) == 2) %>%  # State-level only
  select(geography, geography_name, state)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download CDC SchoolVaxView data
# -----------------------------------------------------------------------------
raw_state <- dcf::dcf_download_cdc(
  "ijqb-a7ye",
  "raw",
  process$raw_state
)

if (!identical(process$raw_state, raw_state)) {
  
  data <- vroom::vroom("./raw/ijqb-a7ye.csv.xz", show_col_types = FALSE) %>%
    #filter(!grepl('Exemption',dose)) %>%
    rename(vaccine = "Vaccine/Exemption") %>%
    mutate(
      vaccine = tolower(vaccine),
      vax = if_else(is.na(Dose), vaccine,
        if_else(
        Dose == 'Any Exemption',
        'full_exempt',
        if_else(
          Dose == 'Medical Exemption',
          'medical_exempt',
          if_else(
            Dose == 'Non-Medical Exemption',
            'personal_exempt',
            vaccine
          )
        )
      )
      ),
      vax = if_else(
        vaccine == "dtp, dtap, or dt",
        'dtap',
        if_else(vaccine == "hepatitis b", 'hep_b', vax)
      ),
      grade = 'Kindergarten'
    ) %>%
    rename(
      year = 'School Year',
      N = "Population Size",
      value = "Estimate (%)",
      percent_surveyed =  "Percent Surveyed",
      survey_type = 'Survey Type',
      statename = Geography
    ) %>%
    filter(statename %in% c(state.name, 'District of Columbia', 'United States')) %>%
    left_join(fips_lookup, by = c("statename" = "geography_name")) %>%
    mutate(geography = if_else(statename == 'United States', "00", geography),
           time = paste(substr(year,1,4),'09','01', sep='-'), #set date to start of academic year (Sept 1,YYYY)
           vax = if_else(
             grepl('1 dose', Dose), NA_character_, vax  #removes the 1 dose varicella category
           )
           ) %>%
    filter(vax != '') %>%
    dplyr::select(time, geography, grade, N, vax, value, percent_surveyed, survey_type) %>%
    distinct() 
    
  
  
  exemptions <- data %>%
    filter(grepl('exempt', tolower(vax)))
  
  vroom::vroom_write(
    exemptions,
    "standard/data_exemptions.csv.gz",
    ","
  )
  
  
  vax2 <- data %>%
    filter(!grepl('exempt', tolower(vax))) %>%
    filter(!grepl('pac', vax))
  

    vroom::vroom_write(
      data,
      "standard/data.csv.gz",
      ","
    )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

# -----------------------------------------------------------------------------
# 2. Download Washington Post school vaccination data
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
counties_raw_temp <- vroom::vroom(urls$counties, show_col_types = FALSE)
schools_raw_temp <- vroom::vroom(urls$schools, show_col_types = FALSE)

# Calculate hash based on data content to detect changes
current_wapo_state <- list(
  counties_hash = digest::digest(counties_raw_temp),
  schools_hash = digest::digest(schools_raw_temp)
)

# Only process if data has changed
if (!identical(process$wapo_state, current_wapo_state)) {

  # ---------------------------------------------------------------------------
  # 2a. Process county-level data
  # ---------------------------------------------------------------------------

  # Use the data already loaded from URL
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
    distinct()

  # ---------------------------------------------------------------------------
  # 2b. Process school-level data
  # ---------------------------------------------------------------------------

  # Use the data already loaded from URL
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
      wapo_school_address = address,
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
      wapo_school_address,
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
  # 2c. Write standardized output files
  # ---------------------------------------------------------------------------

  # Write county-level data
  vroom::vroom_write(
    counties_standard,
    "standard/data_wapo_counties.csv.gz",
    delim = ","
  )

  # Write school-level data
  vroom::vroom_write(
    schools_standard,
    "standard/data_wapo_schools.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 2d. Record processed state
  # ---------------------------------------------------------------------------

  process$wapo_state <- current_wapo_state
  dcf::dcf_process_record(updated = process)

  cat("Successfully processed Washington Post vaccination data\n")
  cat("  - Counties:", nrow(counties_standard), "records\n")
  cat("  - Schools:", nrow(schools_standard), "records\n")

} else {
  cat("No changes detected in Washington Post data\n")
}
