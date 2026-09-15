# =============================================================================
# Bundle: Adolescent vaccination
#
# Vaccination coverage among adolescents, Medicaid adolescent quality measures,
# and weekly counts of the diseases the adolescent schedule targets, as tall
# parquet files keyed by geography (FIPS) + time + measure.
#
# Sources:
#   - medicaid_quality/standard/data.csv.gz
#       Child Core Set IMA (MenACWY + Tdap by 13; HPV series by 13), the
#       2014-2016 HPV measure, and well-care visits (annual, state only)
#   - nis_teen/standard/data.csv.gz
#       NIS-Teen coverage, ages 13-17 and 13-15 (annual, state + national)
#   - nis_teen/standard/data_{insurance,poverty,race_ethnicity,urban}.csv.gz
#       pooled 2018-2022 NIS-Teen coverage by demographic group
#   - school_immunizations_adolescent/standard/data.csv.gz
#       state school-entry assessments, 6th/7th grade (annual, county + state)
#   - nnds/standard/data.csv.gz
#       weekly case counts, national + state/territory
#
# Outputs (split by time resolution and grain):
#   - dist/adolescent_vax_state.parquet        : geography(2-digit) x year x measure
#   - dist/adolescent_vax_county.parquet       : geography(5-digit) x year x grade x measure
#   - dist/adolescent_vax_demographics.parquet : geography(2-digit) x year x stratum x measure
#   - dist/adolescent_vax_weekly.parquet       : geography(2-digit) x MMWR week x measure
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

# Read everything as character so sparse columns are not mistyped; numeric
# coercion happens in the pivot helpers.
read_chr <- function(path) vroom(path, col_types = cols(.default = "c"), show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 0. Measures taken from each source (source column names are kept as-is)
# -----------------------------------------------------------------------------

MEDICAID_STEMS <- c(
  "medicaid_ima_ch",        # MenACWY + Tdap by 13th birthday (Combination 1)
  "medicaid_ima_ch_hpv",    # HPV series by 13th birthday, 2017+
  "medicaid_hpv_ch",        # standalone HPV measure, 2014-2016
  "medicaid_awc_ch",        # adolescent well-care visits, ages 12-21, through 2020
  "medicaid_wcv_ch",        # well-care visits, ages 3-21, 2021+
  "medicaid_wcv_ch_12_17"   # well-care visits, ages 12-17, 2021+
)
MEDICAID_MEASURES <- paste0(rep(MEDICAID_STEMS, each = 3), c("_rate", "_pct_25", "_pct_75"))

NIS_VALUES <- c("nis_teen_coverage", "nis_teen_coverage_lcl",
                "nis_teen_coverage_ucl", "nis_teen_sample_size")
NIS_POOLED <- c("insurance", "poverty", "race_ethnicity", "urban")

SCHOOL_MEASURES <- paste0("school_adol_pct_", c(
  "tdap", "menacwy", "hpv", "complete",
  "medical_exempt", "religious_exempt", "personal_exempt", "full_exempt"
))

# varicella_disease replaced varicella_morbidity in late 2023; its 2023 weeks
# repeat the old column and are dropped below. Hepatitis B split into
# confirmed and probable at the start of 2024 with no overlap.
NNDS_MEASURES <- c(
  "pertussis",
  "meningococcal_disease_all_serogroups",
  "meningococcal_disease_serogroups_acwy",
  "meningococcal_disease_serogroup_b",
  "tetanus",
  "mumps",
  "varicella_morbidity",
  "varicella_disease",
  "hepatitis_b_acute",
  "hepatitis_b_acute_confirmed",
  "hepatitis_b_acute_probable"
)

# -----------------------------------------------------------------------------
# 1. Helpers
# -----------------------------------------------------------------------------

# Wide measure columns -> tall (id_cols + measure, value), dropping NA values.
pivot_measures <- function(df, measures, id_cols = c("geography", "time")) {
  present <- intersect(measures, colnames(df))
  missing <- setdiff(measures, colnames(df))
  if (length(missing) > 0) {
    warning("Expected source columns not found (skipped):\n",
            paste(" -", missing, collapse = "\n"))
  }
  df %>%
    select(all_of(id_cols), all_of(present)) %>%
    pivot_longer(all_of(present), names_to = "measure", values_to = "value") %>%
    mutate(value = suppressWarnings(as.numeric(value))) %>%
    filter(!is.na(value))
}

# No duplicate rows for the key columns with differing values; drops exact
# duplicates, stops if the same key has conflicting values.
check_dupes <- function(df, label, key_cols = c("geography", "time", "measure")) {
  dupes <- df %>%
    group_by(across(all_of(key_cols))) %>%
    summarize(n = n(), n_distinct_values = n_distinct(value), .groups = "drop") %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    if (any(dupes$n_distinct_values > 1)) {
      stop(label, ": ", sum(dupes$n_distinct_values > 1),
           " duplicate ", paste(key_cols, collapse = "-"),
           " rows have differing values.")
    }
    warning(label, ": ", nrow(dupes),
            " duplicate rows with identical values; keeping first occurrence.")
    df <- df %>%
      group_by(across(all_of(key_cols))) %>%
      slice(1) %>%
      ungroup()
  }
  df
}

# Annual sources store YYYY-01-01, YYYY-09-01 (school year start), or
# YYYY-12-31; use the year end.
year_end <- function(x) as.Date(paste0(substr(x, 1, 4), "-12-31"))

# NIS-Teen vaccine + dose + sex -> measure name, e.g. nis_teen_hpv_up_to_date,
# nis_teen_hpv_1_dose_female, nis_teen_menacwy_1_dose.
nis_measure <- function(vaccine, dose, sex) {
  d <- tolower(dose)
  d <- sub("^>=1 dose tdap$", ">=1 dose", d)
  d <- gsub(">=", "", d)
  d <- gsub("doses", "dose", d)
  d <- gsub("[^a-z0-9]+", "_", d)
  d <- gsub("^_|_$", "", d)
  m <- paste0("nis_teen_", tolower(vaccine), "_", d)
  if_else(sex == "Overall", m, paste0(m, "_", tolower(sex)))
}

# State name -> 2-digit FIPS. Territories have no name in all_fips.csv.gz.
all_fips <- read_chr("../../resources/all_fips.csv.gz")
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  mutate(geography_name = case_when(
    !is.na(geography_name) ~ geography_name,
    state == "AS" ~ "American Samoa",
    state == "GU" ~ "Guam",
    state == "MP" ~ "Northern Mariana Islands",
    state == "PR" ~ "Puerto Rico",
    state == "VI" ~ "U.S. Virgin Islands",
    TRUE ~ geography_name
  )) %>%
  select(fips = geography, geography_name)

# -----------------------------------------------------------------------------
# 2. Medicaid Core Set (annual, state only, geography stored as state name)
# -----------------------------------------------------------------------------

medicaid_raw <- read_chr("../medicaid_quality/standard/data.csv.gz") %>%
  # CHIP reports the child measures separately for the same state-years;
  # keep the Medicaid rows only.
  filter(payer == "Medicaid") %>%
  mutate(geography = if_else(geography == "Dist. of Col.", "District of Columbia", geography)) %>%
  left_join(state_fips_lookup, by = c("geography" = "geography_name"))

unmatched <- unique(medicaid_raw$geography[is.na(medicaid_raw$fips)])
if (length(unmatched) > 0) {
  warning("medicaid_quality geographies without a FIPS match (dropped): ",
          paste(unmatched, collapse = ", "))
}

medicaid_state <- medicaid_raw %>%
  filter(!is.na(fips)) %>%
  select(-geography) %>%
  rename(geography = fips) %>%
  pivot_measures(MEDICAID_MEASURES) %>%
  mutate(time = year_end(time), source = "CMS Medicaid Core Set")

# -----------------------------------------------------------------------------
# 3. NIS-Teen (annual, state + national; pooled demographic files)
# -----------------------------------------------------------------------------

nis_annual <- read_chr("../nis_teen/standard/data.csv.gz") %>%
  mutate(
    across(all_of(NIS_VALUES), ~ suppressWarnings(as.numeric(.x))),
    measure = nis_measure(vaccine, dose, sex),
    time = as.Date(time)
  ) %>%
  filter(!is.na(nis_teen_coverage))

nis_state <- nis_annual %>%
  filter(age == "13-17") %>%
  transmute(
    geography, time, measure,
    value = nis_teen_coverage,
    value_lcl = nis_teen_coverage_lcl,
    value_ucl = nis_teen_coverage_ucl,
    sample_size = nis_teen_sample_size,
    source = "CDC NIS-Teen"
  )

# Age groups from the annual file, then the pooled 2018-2022 files, one
# stratum type per file.
nis_by_age <- nis_annual %>%
  transmute(
    geography, time, survey_years,
    stratum_type = "age", stratum = age, measure,
    value = nis_teen_coverage,
    value_lcl = nis_teen_coverage_lcl,
    value_ucl = nis_teen_coverage_ucl,
    sample_size = nis_teen_sample_size
  )

nis_pooled <- bind_rows(lapply(NIS_POOLED, function(col) {
  read_chr(sprintf("../nis_teen/standard/data_%s.csv.gz", col)) %>%
    mutate(
      across(all_of(NIS_VALUES), ~ suppressWarnings(as.numeric(.x))),
      measure = nis_measure(vaccine, dose, sex),
      time = as.Date(time)
    ) %>%
    filter(!is.na(nis_teen_coverage)) %>%
    transmute(
      geography, time, survey_years,
      stratum_type = col, stratum = .data[[col]], measure,
      value = nis_teen_coverage,
      value_lcl = nis_teen_coverage_lcl,
      value_ucl = nis_teen_coverage_ucl,
      sample_size = nis_teen_sample_size
    )
}))

adolescent_vax_demographics <- bind_rows(nis_by_age, nis_pooled) %>%
  mutate(source = "CDC NIS-Teen") %>%
  arrange(stratum_type, measure, geography, time, stratum) %>%
  check_dupes("adolescent_vax_demographics",
              c("geography", "time", "stratum_type", "stratum", "measure"))

# -----------------------------------------------------------------------------
# 4. School immunization assessments (annual by school year, county + state).
#    County rows keep the grade as a column; state rows fold it into the
#    measure name so the state file stays one row per geography-year-measure.
# -----------------------------------------------------------------------------

school_raw <- read_chr("../school_immunizations_adolescent/standard/data.csv.gz") %>%
  mutate(suppressed_flag = as.integer(suppressed_flag))

school_tall <- school_raw %>%
  pivot_measures(SCHOOL_MEASURES, c("geography", "time", "grade", "suppressed_flag")) %>%
  mutate(time = year_end(time), source = "State school immunization assessments")

school_county <- school_tall %>%
  filter(nchar(geography) == 5)

school_state <- school_tall %>%
  filter(nchar(geography) == 2) %>%
  mutate(measure = paste0(measure, "_", tolower(grade))) %>%
  select(-grade, -suppressed_flag)

# -----------------------------------------------------------------------------
# 5. Assemble annual state and county outputs
# -----------------------------------------------------------------------------

adolescent_vax_state <- bind_rows(medicaid_state, nis_state, school_state) %>%
  select(geography, time, measure, value, value_lcl, value_ucl, sample_size, source) %>%
  arrange(measure, geography, time) %>%
  check_dupes("adolescent_vax_state")

adolescent_vax_county <- school_county %>%
  select(geography, time, grade, measure, value, suppressed_flag, source) %>%
  arrange(measure, geography, time, grade) %>%
  check_dupes("adolescent_vax_county", c("geography", "time", "grade", "measure"))

# -----------------------------------------------------------------------------
# 6. NNDSS (weekly, national + state/territory). The source stores cumulative
#    year-to-date counts; difference within each geography/measure/MMWR year
#    to get weekly counts. Negative values are downward revisions by CDC.
#    Missing YTD values are stored as 0 in the source, so a measure that was
#    not reported in a given year shows up as an all-zero national series;
#    those measure-years are dropped rather than emitted as zero counts.
# -----------------------------------------------------------------------------

nnds_cum <- read_chr("../nnds/standard/data.csv.gz") %>%
  select(geography, time, mmwr_year, mmwr_week, all_of(NNDS_MEASURES)) %>%
  pivot_longer(all_of(NNDS_MEASURES), names_to = "measure", values_to = "value") %>%
  mutate(
    value     = suppressWarnings(as.numeric(value)),
    mmwr_year = as.integer(mmwr_year),
    mmwr_week = as.integer(mmwr_week)
  ) %>%
  filter(!is.na(value)) %>%
  filter(!(measure == "varicella_disease" & mmwr_year < 2024))

reported <- nnds_cum %>%
  filter(geography == "00") %>%
  group_by(measure, mmwr_year) %>%
  summarize(reported = any(value > 0), .groups = "drop") %>%
  filter(reported) %>%
  select(measure, mmwr_year)

adolescent_vax_weekly <- nnds_cum %>%
  semi_join(reported, by = c("measure", "mmwr_year")) %>%
  arrange(geography, measure, mmwr_year, mmwr_week) %>%
  group_by(geography, measure, mmwr_year) %>%
  mutate(value = value - lag(value, default = 0)) %>%
  ungroup() %>%
  transmute(geography, time = as.Date(time), measure, value, source = "CDC NNDSS") %>%
  arrange(measure, geography, time) %>%
  check_dupes("adolescent_vax_weekly")

# -----------------------------------------------------------------------------
# 7. Write outputs
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)
write_parquet(adolescent_vax_state,        "dist/adolescent_vax_state.parquet")
write_parquet(adolescent_vax_county,       "dist/adolescent_vax_county.parquet")
write_parquet(adolescent_vax_demographics, "dist/adolescent_vax_demographics.parquet")
write_parquet(adolescent_vax_weekly,       "dist/adolescent_vax_weekly.parquet")

report <- function(df, name) {
  sprintf("  %-36s: %d rows, %d measures, %d geographies, %s to %s",
          name, nrow(df), n_distinct(df$measure), n_distinct(df$geography),
          min(df$time), max(df$time))
}
message(
  "bundle_adolescent_vaccination:\n",
  report(adolescent_vax_state,        "adolescent_vax_state.parquet"), "\n",
  report(adolescent_vax_county,       "adolescent_vax_county.parquet"), "\n",
  report(adolescent_vax_demographics, "adolescent_vax_demographics.parquet"), "\n",
  report(adolescent_vax_weekly,       "adolescent_vax_weekly.parquet")
)
