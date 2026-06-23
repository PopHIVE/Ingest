# =============================================================================
# NHTSA FARS (Fatality Analysis Reporting System) Data Ingestion
# Source: https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/
# Coverage: 2000-2023, annual fatal motor vehicle crash data
# Geography: County (primary), State, National
# =============================================================================

library(dplyr)
library(purrr)
library(tidyr)
library(vroom)

# Initialize process record
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw FARS zip files (2000-latest)
# -----------------------------------------------------------------------------
find_latest_fars_year <- function() {
  yr <- as.integer(format(Sys.Date(), "%Y"))
  while (yr >= 2020) {
    url <- sprintf(
      paste0(
        "https://static.nhtsa.gov/nhtsa/downloads/FARS/",
        "%d/National/FARS%dNationalCSV.zip"
      ),
      yr, yr
    )
    status <- tryCatch(
      attr(curlGetHeaders(url), "status"),
      error = function(e) 0L
    )
    if (identical(status, 200L)) return(yr)
    yr <- yr - 1
  }
  stop("Could not determine latest available FARS year")
}

years <- 2000:find_latest_fars_year()

dir.create("raw", showWarnings = FALSE)

download_fars_zip <- function(yr) {
  url <- sprintf(
    "https://static.nhtsa.gov/nhtsa/downloads/FARS/%d/National/FARS%dNationalCSV.zip",
    yr, yr
  )
  dest <- sprintf("raw/FARS%dNationalCSV.zip", yr)
  if (!file.exists(dest)) {
    message(sprintf("Downloading FARS %d...", yr))
    download.file(url, dest, mode = "wb", quiet = TRUE)
  }
  list(hash = unname(tools::md5sum(dest)))
}

raw_state_new <- setNames(lapply(years, download_fars_zip), as.character(years))

# Only re-process if any year's data has changed
if (!identical(process$raw_state, raw_state_new)) {

  # ---------------------------------------------------------------------------
  # 2. Load reference data
  # ---------------------------------------------------------------------------
  all_fips <- vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # Valid county FIPS codes (5-digit) for filtering
  valid_county_fips <- all_fips %>%
    filter(nchar(geography) == 5) %>%
    pull(geography)

  # County population from 2021 Census (single-year reference denominator)
  pop_county <- vroom(
    "../../resources/census_population_2021.csv.xz",
    show_col_types = FALSE
  ) %>%
    filter(nchar(GEOID) == 5) %>%
    select(geography = GEOID, population = Total)

  pop_state <- vroom(
    "../../resources/census_population_2021.csv.xz",
    show_col_types = FALSE
  ) %>%
    filter(nchar(GEOID) == 2) %>%
    select(geography = GEOID, population = Total)

  pop_national <- pop_state %>%
    summarize(population = sum(population)) %>%
    mutate(geography = "00")

  pop_all <- bind_rows(pop_county, pop_state, pop_national)

  # ---------------------------------------------------------------------------
  # 3. Helper: read a named file from a FARS zip archive
  #    Handles case-insensitive file names and subdirectory paths
  # ---------------------------------------------------------------------------
  read_fars_file <- function(yr, file_pattern) {
    zip_path <- sprintf("raw/FARS%dNationalCSV.zip", yr)

    # List contents to find the right file (case-insensitive)
    zip_contents <- tryCatch(
      unzip(zip_path, list = TRUE),
      error = function(e) {
        warning(sprintf("Cannot list zip %s: %s", zip_path, e$message))
        return(NULL)
      }
    )
    if (is.null(zip_contents)) return(NULL)

    match_idx <- grep(file_pattern, basename(zip_contents$Name), ignore.case = TRUE)
    if (length(match_idx) == 0) {
      warning(sprintf("Pattern '%s' not found in FARS %d zip", file_pattern, yr))
      return(NULL)
    }
    target_file <- zip_contents$Name[match_idx[1]]

    readr::read_csv(
      unz(zip_path, target_file),
      show_col_types = FALSE,
      col_types = readr::cols(.default = "c")
    ) %>%
      mutate(YEAR = yr)
  }

  # ---------------------------------------------------------------------------
  # 4. Read and combine all years of accident and person data
  # ---------------------------------------------------------------------------
  message("Reading accident files...")
  accident_all <- map_dfr(years, read_fars_file, file_pattern = "^accident\\.csv$")

  message("Reading person files...")
  person_all <- map_dfr(years, read_fars_file, file_pattern = "^person\\.csv$")

  # ---------------------------------------------------------------------------
  # 5. Build geography fields and normalize key columns
  # ---------------------------------------------------------------------------
  accident_all <- accident_all %>%
    mutate(
      # County FIPS: 2-digit state + 3-digit county
      geography_county = sprintf("%02d%03d", as.integer(STATE), as.integer(COUNTY)),
      geography_state  = sprintf("%02d", as.integer(STATE)),
      time             = paste0(YEAR, "-12-31"),
      # Unknown county codes: 0 = not reported, 999 = unknown
      county_known     = as.integer(COUNTY) > 0 & as.integer(COUNTY) < 999,
      # Rural/urban: handle both old (LAND_USE) and new (RUR_URB) column names
      rural_urban = dplyr::coalesce(
        if ("RUR_URB"   %in% names(.)) as.integer(RUR_URB)   else NA_integer_,
        if ("LAND_USE"  %in% names(.)) as.integer(LAND_USE)  else NA_integer_
      ),
      # Alcohol involvement: DRUNK_DR = number of drunk drivers
      alcohol_related = !is.na(DRUNK_DR) & as.integer(DRUNK_DR) >= 1,
      # Speeding: SPEEDREL only available from 2010 onward
      speeding_related = if ("SPEEDREL" %in% names(.)) {
        YEAR >= 2010 & !is.na(SPEEDREL) & as.integer(SPEEDREL) %in% 1:4
      } else {
        NA
      },
      single_vehicle   = !is.na(VE_TOTAL) & as.integer(VE_TOTAL) == 1
    )

  # ---------------------------------------------------------------------------
  # 6. Output 1: data.csv.gz — Annual fatalities & crashes
  #    Primary: county-level. Also state and national.
  # ---------------------------------------------------------------------------
  summarize_crashes <- function(df) {
    df %>%
      group_by(geography, time) %>%
      summarize(
        nhtsa_fatalities   = sum(as.integer(FATALS), na.rm = TRUE),
        nhtsa_fatal_crashes = n(),
        .groups = "drop"
      )
  }

  data_county <- accident_all %>%
    filter(county_known) %>%
    rename(geography = geography_county) %>%
    summarize_crashes()

  data_state <- accident_all %>%
    rename(geography = geography_state) %>%
    summarize_crashes()

  data_national <- accident_all %>%
    mutate(geography = "00") %>%
    summarize_crashes()

  data_main <- bind_rows(data_county, data_state, data_national) %>%
    left_join(pop_all, by = "geography") %>%
    mutate(
      nhtsa_fatality_rate = round(nhtsa_fatalities / population * 100000, 2)
    ) %>%
    select(geography, time, nhtsa_fatalities, nhtsa_fatal_crashes,
           nhtsa_fatality_rate)

  vroom_write(data_main, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 7. Output 2: data_person_type.csv.gz — Fatalities by person type
  #    Based on fatal persons (INJ_SEV == 4) in person.csv
  # ---------------------------------------------------------------------------

  # Join person data with accident geography
  accident_geo <- accident_all %>%
    select(ST_CASE, YEAR, geography_county, geography_state, county_known, time)

  person_typed <- person_all %>%
    mutate(YEAR = as.integer(YEAR), ST_CASE = as.integer(ST_CASE)) %>%
    left_join(
      accident_geo %>%
        mutate(ST_CASE = as.integer(ST_CASE), YEAR = as.integer(YEAR)),
      by = c("ST_CASE", "YEAR")
    ) %>%
    filter(as.integer(INJ_SEV) == 4) %>%  # fatal persons only
    mutate(
      person_type = case_when(
        as.integer(PER_TYP) == 1              ~ "driver",
        as.integer(PER_TYP) %in% 2:4          ~ "passenger",
        as.integer(PER_TYP) == 5              ~ "pedestrian",
        as.integer(PER_TYP) %in% 6:7          ~ "cyclist",
        TRUE                                   ~ "other"
      )
    ) %>%
    filter(person_type != "other")

  agg_person_type <- function(df, geo_col) {
    df %>%
      rename(geography = !!sym(geo_col)) %>%
      group_by(geography, time, person_type) %>%
      summarize(nhtsa_fatalities = n(), .groups = "drop")
  }

  data_person_type <- bind_rows(
    person_typed %>% filter(county_known)  %>% agg_person_type("geography_county"),
    person_typed                            %>% agg_person_type("geography_state"),
    person_typed %>% mutate(geography_national = "00") %>% agg_person_type("geography_national")
  )

  vroom_write(data_person_type, "standard/data_person_type.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 8. Output 3: data_age_sex.csv.gz — Fatalities by age group and sex
  # ---------------------------------------------------------------------------
  person_demo <- person_all %>%
    mutate(YEAR = as.integer(YEAR), ST_CASE = as.integer(ST_CASE)) %>%
    left_join(
      accident_geo %>%
        mutate(ST_CASE = as.integer(ST_CASE), YEAR = as.integer(YEAR)),
      by = c("ST_CASE", "YEAR")
    ) %>%
    filter(as.integer(INJ_SEV) == 4) %>%
    mutate(
      age_num = as.integer(AGE),
      age = case_when(
        age_num <  15 ~ "0-14",
        age_num <  25 ~ "15-24",
        age_num <  45 ~ "25-44",
        age_num <  65 ~ "45-64",
        age_num < 998 ~ "65+",   # 998 = not reported, 999 = unknown
        TRUE          ~ NA_character_
      ),
      sex = case_when(
        as.integer(SEX) == 1 ~ "Male",
        as.integer(SEX) == 2 ~ "Female",
        TRUE                 ~ NA_character_
      )
    ) %>%
    filter(!is.na(age), !is.na(sex))

  agg_demo <- function(df, geo_col) {
    df %>%
      rename(geography = !!sym(geo_col)) %>%
      group_by(geography, time, age, sex) %>%
      summarize(nhtsa_fatalities = n(), .groups = "drop")
  }

  data_age_sex <- bind_rows(
    person_demo %>% filter(county_known)  %>% agg_demo("geography_county"),
    person_demo                            %>% agg_demo("geography_state"),
    person_demo %>% mutate(geography_national = "00") %>% agg_demo("geography_national")
  )

  vroom_write(data_age_sex, "standard/data_age_sex.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 9. Output 4: data_crash_type.csv.gz — Fatalities by crash characteristic
  #    Wide format: one column per crash type, stratified by age and sex
  # ---------------------------------------------------------------------------

  # Build person-level dataset with crash type flags inherited from the crash
  crash_flags <- accident_all %>%
    mutate(ST_CASE = as.integer(ST_CASE)) %>%
    select(ST_CASE, YEAR, alcohol_related, speeding_related, single_vehicle, rural_urban)

  person_crash_flags <- person_all %>%
    mutate(YEAR = as.integer(YEAR), ST_CASE = as.integer(ST_CASE)) %>%
    left_join(
      accident_geo %>% mutate(ST_CASE = as.integer(ST_CASE), YEAR = as.integer(YEAR)),
      by = c("ST_CASE", "YEAR")
    ) %>%
    filter(as.integer(INJ_SEV) == 4) %>%
    left_join(crash_flags, by = c("ST_CASE", "YEAR")) %>%
    mutate(
      age_num = as.integer(AGE),
      age = case_when(
        age_num <  15 ~ "0-14",
        age_num <  25 ~ "15-24",
        age_num <  45 ~ "25-44",
        age_num <  65 ~ "45-64",
        age_num < 998 ~ "65+",
        TRUE          ~ NA_character_
      ),
      sex = case_when(
        as.integer(SEX) == 1 ~ "Male",
        as.integer(SEX) == 2 ~ "Female",
        TRUE                 ~ NA_character_
      ),
      per_typ_int              = as.integer(PER_TYP),
      nhtsa_alcohol_related     = !is.na(alcohol_related)  & alcohol_related,
      nhtsa_speeding_related    = !is.na(speeding_related) & speeding_related,
      nhtsa_single_vehicle      = !is.na(single_vehicle)   & single_vehicle,
      nhtsa_rural               = !is.na(rural_urban) & rural_urban == 1,
      nhtsa_urban               = !is.na(rural_urban) & rural_urban == 2,
      nhtsa_pedestrian_involved = per_typ_int == 5,
      nhtsa_cyclist_involved    = per_typ_int %in% 6:7
    )

  crash_type_cols <- c("nhtsa_alcohol_related", "nhtsa_speeding_related",
                       "nhtsa_single_vehicle", "nhtsa_rural", "nhtsa_urban",
                       "nhtsa_pedestrian_involved", "nhtsa_cyclist_involved")

  agg_crash_demo <- function(df, geo_col, group_vars) {
    df %>%
      rename(geography = !!sym(geo_col)) %>%
      group_by(geography, time, across(all_of(group_vars))) %>%
      summarize(
        nhtsa_fatalities    = n(),
        nhtsa_fatal_crashes = n_distinct(ST_CASE, YEAR),
        across(all_of(crash_type_cols), ~ sum(.x, na.rm = TRUE)),
        .groups = "drop"
      )
  }

  make_crash_type <- function(df, geo_col) {
    overall <- agg_crash_demo(df, geo_col, character(0)) %>%
      mutate(age = "Overall", sex = "Overall")
    by_age <- df %>%
      filter(!is.na(age)) %>%
      agg_crash_demo(geo_col, "age") %>%
      mutate(sex = "Overall")
    by_sex <- df %>%
      filter(!is.na(sex)) %>%
      agg_crash_demo(geo_col, "sex") %>%
      mutate(age = "Overall")
    bind_rows(overall, by_age, by_sex)
  }

  data_crash_type <- bind_rows(
    person_crash_flags %>% filter(county_known) %>% make_crash_type("geography_county"),
    person_crash_flags                           %>% make_crash_type("geography_state"),
    person_crash_flags %>% mutate(geography_national = "00") %>%
      make_crash_type("geography_national")
  ) %>%
    select(geography, time, age, sex, nhtsa_fatalities, nhtsa_fatal_crashes,
           all_of(crash_type_cols))

  vroom_write(data_crash_type, "standard/data_crash_type.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 10. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state_new
  dcf::dcf_process_record(updated = process)

  message("NHTSA FARS ingestion complete.")
}
