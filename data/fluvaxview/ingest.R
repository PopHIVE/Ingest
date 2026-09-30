# =============================================================================
# FluVaxView: End-of-Season Influenza Vaccination Coverage (NIS-Flu + BRFSS)
# Source: vh55-3he6 "Influenza Vaccination Coverage for All Ages (6+ Months)"
#         (CDC FluVaxView Interactive)
#
# Cumulative coverage by month of the flu season (July through May) from the
# National Immunization Survey-Flu (children 6 months-17 years) and the
# Behavioral Risk Factor Surveillance System (adults 18+), 2009-10 season to
# present, for the nation, HHS regions, states, selected local areas, and (for
# adults, calendar years 2018-2022) counties. These are CDC's final
# post-season estimates, published each fall, not the in-season weekly
# dashboard numbers.
#
# Outputs
#   data.csv.gz          national + state, by age group, end of season
#   data_monthly.csv.gz  national + state, by age group, every season month
#   data_race.csv.gz     national + state, by race/ethnicity, every season month
#   data_setting.csv.gz  national + state, place of vaccination by age group
#   data_county.csv.gz   county, adults 18+, calendar years 2018-2022
#   data_substate.csv.gz HHS regions and local areas (non-FIPS), by age group
# =============================================================================

library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc("vh55-3he6", "raw", process$raw_state)

if (!identical(process$raw_state, raw_state)) {

  raw <- vroom::vroom(
    "raw/vh55-3he6.csv.xz", delim = ",",
    col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
  )
  needed <- c("Vaccine", "Geography Type", "Geography", "FIPS", "Season/Survey Year", "Month",
              "Dimension Type", "Dimension", "Estimate (%)", "95% CI (%)", "Sample Size")
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) stop("vh55-3he6 columns not found: ", paste(absent, collapse = ", "))

  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }
  slug <- function(x) gsub("^_|_$", "", gsub("[^a-z0-9]+", "_", tolower(x)))
  # Season month (7..12 = first year, 1..6 = second year) -> last day of month
  month_end <- function(season, m) {
    y1 <- as.integer(substr(season, 1, 4))
    y <- ifelse(m >= 7, y1, y1 + 1)
    as.Date(sprintf("%d-%02d-01", ifelse(m == 12, y + 1, y), ifelse(m == 12, 1, m + 1))) - 1
  }
  season_end <- function(season) as.Date(sprintf("%d-05-31", as.integer(substr(season, 1, 4)) + 1))

  # Territories have no name in all_fips.csv.gz; the FIPS column in this file
  # uses non-standard codes for territories and local areas, so states are
  # matched by name.
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  geo_lookup <- bind_rows(
    all_fips %>% filter(nchar(geography) == 2, !is.na(geography_name)) %>% select(geography, geography_name),
    tibble(
      geography = c("00", "72", "66", "78"),
      geography_name = c("United States", "Puerto Rico", "Guam", "U.S. Virgin Islands")
    )
  ) %>% distinct(geography_name, .keep_all = TRUE)
  state_abbr <- all_fips %>% filter(nchar(geography) == 2) %>% distinct(state, .keep_all = TRUE)

  SETTINGS <- c("Medical Setting", "Non-Medical Setting", "Pharmacy/Store", "Workplace", "School")

  base <- raw %>%
    filter(Vaccine == "Seasonal Influenza") %>%
    rename(
      geo_type = `Geography Type`, geo_name = Geography, fips = FIPS,
      season = `Season/Survey Year`, month = Month, dim_type = `Dimension Type`,
      dim = Dimension, estimate = `Estimate (%)`, ci = `95% CI (%)`, sample_size = `Sample Size`
    ) %>%
    mutate(
      dim_type = gsub("\xe2\x89\xa5", ">=", dim_type, useBytes = TRUE),
      dim      = gsub("\xe2\x89\xa5", ">=", dim, useBytes = TRUE),
      month = as.integer(month),
      # "NR" (with assorted footnote marks) = not reported / unreliable
      suppressed_flag = as.integer(grepl("^NR", estimate)),
      fluvaxview_coverage = suppressWarnings(as.numeric(estimate)),
      ci_clean = if_else(grepl("^[0-9.]+ to [0-9.]+", ci), sub("^([0-9.]+ to [0-9.]+).*$", "\\1", ci), NA_character_),
      fluvaxview_sample_size = suppressWarnings(as.numeric(gsub(",", "", sample_size)))
    ) %>%
    separate(ci_clean, into = c("fluvaxview_coverage_lcl", "fluvaxview_coverage_ucl"),
             sep = " to ", convert = TRUE, fill = "right") %>%
    # 2023-24 carries three duplicate age labels ("Greater 65", "Greater than
    # 18 Years flu", "Greater than 6 Months flu") alongside the standard ones.
    filter(!grepl("^Greater", dim))

  bad <- base %>% filter(is.na(fluvaxview_coverage), suppressed_flag == 0, !is.na(estimate))
  if (nrow(bad) > 0) stop("unrecognised estimate values: ", paste(unique(bad$estimate), collapse = ", "))

  VALUE_COLS <- c("fluvaxview_coverage", "fluvaxview_coverage_lcl", "fluvaxview_coverage_ucl",
                  "fluvaxview_sample_size", "suppressed_flag")

  # ---------------------------------------------------------------------------
  # National + state rows (FIPS) and substate rows (non-FIPS)
  # ---------------------------------------------------------------------------
  area <- base %>%
    filter(geo_type != "Counties") %>%
    mutate(
      is_region = geo_type == "HHS Regions/National" & geo_name != "United States",
      is_local  = geo_type == "States/Local Areas" & grepl("^[A-Z]{2}-", geo_name),
      geography = case_when(
        is_region ~ sub("^Region ", "hhs_", geo_name),
        is_local  ~ slug(geo_name),
        TRUE      ~ geo_lookup$geography[match(geo_name, geo_lookup$geography_name)]
      ),
      geography_level = case_when(is_region ~ "Region", is_local ~ "Local", TRUE ~ NA_character_),
      state_fips = if_else(is_local, state_abbr$geography[match(substr(geo_name, 1, 2), state_abbr$state)], NA_character_)
    )
  unmapped <- unique(area$geo_name[is.na(area$geography)])
  if (length(unmapped) > 0) stop("geographies not mapped: ", paste(unmapped, collapse = ", "))

  fips_area <- area %>% filter(!is_region, !is_local)

  monthly <- fips_area %>%
    filter(dim_type == "Age") %>%
    mutate(time = format(month_end(season, month), "%Y-%m-%d"), season_order = ifelse(month >= 7, month - 6, month + 6)) %>%
    select(geography, time, season, season_order, age = dim, all_of(VALUE_COLS)) %>%
    arrange(geography, age, time) %>%
    check_unique(c("geography", "time", "age"), "data_monthly")
  vroom::vroom_write(select(monthly, -season_order), "standard/data_monthly.csv.gz", ",")

  # End of season = the last reported month of each season
  data <- monthly %>%
    group_by(season) %>%
    filter(season_order == max(season_order)) %>%
    ungroup() %>%
    select(-season_order) %>%
    check_unique(c("geography", "season", "age"), "data")
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  data_race <- fips_area %>%
    filter(dim_type == "Race and Ethnicity") %>%
    mutate(time = format(month_end(season, month), "%Y-%m-%d")) %>%
    select(geography, time, season, race_ethnicity = dim, all_of(VALUE_COLS)) %>%
    arrange(geography, race_ethnicity, time) %>%
    check_unique(c("geography", "time", "race_ethnicity"), "data_race")
  vroom::vroom_write(data_race, "standard/data_race.csv.gz", ",")

  # Place of vaccination: one end-of-season value per age group (Month is a
  # placeholder in these rows)
  data_setting <- fips_area %>%
    filter(dim %in% SETTINGS) %>%
    mutate(time = format(season_end(season), "%Y-%m-%d")) %>%
    select(geography, time, season, age = dim_type, setting = dim, all_of(VALUE_COLS)) %>%
    arrange(geography, age, setting, time) %>%
    check_unique(c("geography", "time", "age", "setting"), "data_setting")
  vroom::vroom_write(data_setting, "standard/data_setting.csv.gz", ",")

  data_substate <- area %>%
    filter(is_region | is_local, dim_type == "Age") %>%
    mutate(time = format(month_end(season, month), "%Y-%m-%d")) %>%
    select(geography, geography_name = geo_name, geography_level, state_fips,
           time, season, age = dim, all_of(VALUE_COLS)) %>%
    arrange(geography, age, time) %>%
    check_unique(c("geography", "time", "age"), "data_substate")
  vroom::vroom_write(data_substate, "standard/data_substate.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # County rows: adults 18+, one value per calendar year (BRFSS)
  # ---------------------------------------------------------------------------
  data_county <- base %>%
    filter(geo_type == "Counties", dim_type == "Age") %>%
    mutate(geography = sprintf("%05d", as.integer(fips)), time = paste0(season, "-12-31")) %>%
    # county rows carry no sample size
    select(geography, time, age = dim, all_of(setdiff(VALUE_COLS, "fluvaxview_sample_size"))) %>%
    arrange(geography, time) %>%
    check_unique(c("geography", "time", "age"), "data_county")
  missing_fips <- setdiff(unique(data_county$geography), all_fips$geography)
  if (length(missing_fips) > 0) warning("county FIPS not in all_fips.csv.gz: ", paste(missing_fips, collapse = ", "))
  vroom::vroom_write(data_county, "standard/data_county.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
