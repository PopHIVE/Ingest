# =============================================================================
# Bundle: STI
#
# Sexually transmitted infection surveillance, screening, and youth sexual
# behavior as tall parquet files keyed by geography (FIPS) + time + measure.
#
# Sources:
#   - county_health_rankings/standard/data_{state,county}.csv.gz
#       chlamydia incidence, HIV prevalence, teen births (annual)
#   - medicaid_quality/standard/data.csv.gz
#       chlamydia screening in women, Medicaid Core Set (annual, state only)
#   - cms_mmd/standard/data_state_county_age.csv.gz
#       STI screening among Medicare FFS beneficiaries (annual)
#   - nchs_mortality/standard/data_state_21_causes.csv.gz
#       HIV disease death rate (quarterly, state + national)
#   - nnds/standard/data.csv.gz
#       weekly case counts, national + state/territory
#   - yrbss/standard/data_age{,_sex,_ethnicity}.csv.gz
#       high school sexual behaviors (biennial, state + national)
#
# Outputs (split by time resolution so each file has one grain):
#   - dist/sti_state.parquet     : geography(2-digit) x year x measure x value
#   - dist/sti_county.parquet    : geography(5-digit) x year x measure x value
#   - dist/sti_quarterly.parquet : geography(2-digit) x quarter x measure x value
#   - dist/sti_weekly.parquet    : geography(2-digit) x MMWR week x measure x value
#   - dist/sti_youth.parquet     : geography(2-digit) x survey year x age x sex x
#                                  race_ethnicity x measure x value (+ CI, flag)
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

CHR_MEASURES <- c(
  "chr_sexually_transmitted_infections",
  "chr_hiv_prevalence",
  "chr_teen_births"
)

MEDICAID_MEASURES <- c(
  "medicaid_chl_ch_rate", "medicaid_chl_ch_pct_25", "medicaid_chl_ch_pct_75",
  "medicaid_chl_ad_rate", "medicaid_chl_ad_pct_25", "medicaid_chl_ad_pct_75"
)

CMS_MEASURES <- "cms_scrn_prvnt_sti"

NCHS_MEASURES <- "rate_hiv_disease"

# hepatitis_b_acute_2 is left out: it holds five weeks of late 2023 that
# duplicate hepatitis_b_acute.
NNDS_MEASURES <- c(
  "chlamydia_trachomatis_infection",
  "gonorrhea",
  "syphilis_primary_and_secondary",
  "syphilis_congenital",
  "chancroid",
  "mpox",
  "hepatitis_b_acute",
  "hepatitis_b_acute_confirmed",
  "hepatitis_b_acute_probable",
  "hepatitis_b_chronic_confirmed",
  "hepatitis_b_chronic_probable",
  "hepatitis_b_perinatal_infection",
  "hepatitis_b_perinatal_confirmed",
  "hepatitis_c_acute_confirmed",
  "hepatitis_c_acute_probable",
  "hepatitis_c_chronic_confirmed",
  "hepatitis_c_chronic_probable",
  "hepatitis_c_perinatal_infection",
  "hepatitis_c_perinatal_confirmed"
)

YRBSS_MEASURES <- c(
  "pct_ever_sex",
  "pct_sex_before_13",
  "pct_four_plus_partners",
  "pct_currently_sexually_active",
  "pct_alcohol_drugs_before_sex",
  "pct_no_condom_last_sex",
  "pct_no_birth_control_pills",
  "pct_no_iud_implant",
  "pct_no_hormonal_contraception",
  "pct_no_pregnancy_prevention",
  "pct_no_verbal_consent",
  "pct_never_tested_hiv",
  "pct_not_tested_std"
)
YRBSS_FLAGS <- c("_lcl", "_ucl", "_suppressed", "_not_asked")

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

# Wide measure columns with per-measure companions (<measure>_lcl, ...) ->
# tall, one column per companion. Measures are matched longest-first so
# overlapping names resolve correctly.
tall_flagged <- function(df, measures, strata, suffixes) {
  measures <- intersect(measures, names(df))
  measures <- measures[order(-nchar(measures))]
  wanted <- c(measures, paste0(rep(measures, each = length(suffixes)),
                               rep(suffixes, length(measures))))
  df %>%
    select(all_of(c(strata, intersect(wanted, names(df))))) %>%
    rename_with(~ paste0(.x, "_value"), all_of(measures)) %>%
    pivot_longer(
      cols          = -all_of(strata),
      names_to      = c("measure", ".value"),
      names_pattern = sprintf("^(%s)_(%s)$",
                              paste(measures, collapse = "|"),
                              paste(c("value", sub("^_", "", suffixes)), collapse = "|"))
    )
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

# Annual sources store either YYYY-01-01 or YYYY-12-31; use the year end.
year_end <- function(x) as.Date(paste0(substr(x, 1, 4), "-12-31"))

# Quarterly source stores the first day of the quarter; use the last.
quarter_end <- function(x) {
  d <- as.Date(x)
  y <- as.integer(format(d, "%Y"))
  m <- as.integer(format(d, "%m")) + 3
  next_q <- ifelse(m > 12, sprintf("%d-01-01", y + 1), sprintf("%d-%02d-01", y, m))
  as.Date(next_q) - 1
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
# 2. County Health Rankings (annual, state + county)
# -----------------------------------------------------------------------------

chr_state <- read_chr("../county_health_rankings/standard/data_state.csv.gz") %>%
  pivot_measures(CHR_MEASURES) %>%
  mutate(source = "County Health Rankings")

chr_county <- read_chr("../county_health_rankings/standard/data_county.csv.gz") %>%
  pivot_measures(CHR_MEASURES) %>%
  mutate(source = "County Health Rankings")

# -----------------------------------------------------------------------------
# 3. Medicaid Core Set (annual, state only, geography stored as state name)
# -----------------------------------------------------------------------------

medicaid_raw <- read_chr("../medicaid_quality/standard/data.csv.gz") %>%
  # CHIP reports the adolescent measure separately for the same state-years;
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
  mutate(source = "CMS Medicaid Core Set")

# -----------------------------------------------------------------------------
# 4. CMS Mapping Medicare Disparities (annual, national + state + county).
#    All-ages rows only; the age, race, and sex splits stay in the source.
# -----------------------------------------------------------------------------

cms_raw <- read_chr("../cms_mmd/standard/data_state_county_age.csv.gz") %>%
  filter(age == "Total")

cms_state <- cms_raw %>%
  filter(geography_level %in% c("n", "s")) %>%
  pivot_measures(CMS_MEASURES) %>%
  mutate(source = "CMS Medicare FFS")

cms_county <- cms_raw %>%
  filter(geography_level == "c") %>%
  pivot_measures(CMS_MEASURES) %>%
  mutate(source = "CMS Medicare FFS")

# -----------------------------------------------------------------------------
# 5. Assemble annual state and county outputs
# -----------------------------------------------------------------------------

sti_state <- bind_rows(chr_state, medicaid_state, cms_state) %>%
  mutate(time = year_end(time)) %>%
  arrange(measure, geography, time) %>%
  check_dupes("sti_state")

sti_county <- bind_rows(chr_county, cms_county) %>%
  mutate(
    geography = formatC(as.integer(geography), width = 5, flag = "0"),
    time = year_end(time)
  ) %>%
  arrange(measure, geography, time) %>%
  check_dupes("sti_county")

# -----------------------------------------------------------------------------
# 6. NCHS VSRR HIV disease mortality (quarterly, state + national)
# -----------------------------------------------------------------------------

sti_quarterly <- read_chr("../nchs_mortality/standard/data_state_21_causes.csv.gz") %>%
  pivot_measures(NCHS_MEASURES) %>%
  mutate(time = quarter_end(time), source = "NCHS VSRR") %>%
  arrange(measure, geography, time) %>%
  check_dupes("sti_quarterly")

# -----------------------------------------------------------------------------
# 7. NNDSS (weekly, national + state/territory). The source stores cumulative
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
  filter(!is.na(value))

reported <- nnds_cum %>%
  filter(geography == "00") %>%
  group_by(measure, mmwr_year) %>%
  summarize(reported = any(value > 0), .groups = "drop") %>%
  filter(reported) %>%
  select(measure, mmwr_year)

sti_weekly <- nnds_cum %>%
  semi_join(reported, by = c("measure", "mmwr_year")) %>%
  arrange(geography, measure, mmwr_year, mmwr_week) %>%
  group_by(geography, measure, mmwr_year) %>%
  mutate(value = value - lag(value, default = 0)) %>%
  ungroup() %>%
  transmute(geography, time = as.Date(time), measure, value, source = "CDC NNDSS") %>%
  arrange(measure, geography, time) %>%
  check_dupes("sti_weekly")

# -----------------------------------------------------------------------------
# 8. YRBSS sexual behaviors (biennial, national + state). Strata are marginal:
#    ages 14-17 exist only unstratified, sex and race/ethnicity only at
#    age == "Overall". Questions not asked in a jurisdiction-year are dropped;
#    suppressed estimates are kept as NA with suppressed_flag = 1 (the source
#    writes 0 for both).
# -----------------------------------------------------------------------------

yrbss_read <- function(file, strata) {
  read_chr(file.path("../yrbss/standard", file)) %>%
    tall_flagged(YRBSS_MEASURES, c("geography", "time", "age", strata), YRBSS_FLAGS)
}

# The sex and race/ethnicity files repeat the unstratified rows under
# "Overall"; drop those so they are not duplicated against the age file.
sti_youth <- bind_rows(
  yrbss_read("data_age.csv.gz", character()) %>%
    mutate(sex = "Overall", race_ethnicity = "Overall"),
  yrbss_read("data_age_sex.csv.gz", "sex") %>%
    filter(sex != "Overall") %>%
    mutate(race_ethnicity = "Overall"),
  yrbss_read("data_age_ethnicity.csv.gz", "race_ethnicity") %>%
    filter(race_ethnicity != "Overall") %>%
    mutate(sex = "Overall")
) %>%
  mutate(
    across(c(value, lcl, ucl), ~ suppressWarnings(as.numeric(.x))),
    across(c(suppressed, not_asked), as.integer)
  ) %>%
  filter(not_asked == 0) %>%
  mutate(across(c(value, lcl, ucl), ~ if_else(suppressed == 1, NA_real_, .x))) %>%
  transmute(
    geography, time = as.Date(time), age, sex, race_ethnicity, measure,
    value, value_lcl = lcl, value_ucl = ucl, suppressed_flag = suppressed,
    source = "CDC YRBSS"
  ) %>%
  arrange(measure, geography, time, age, sex, race_ethnicity) %>%
  check_dupes("sti_youth", c("geography", "time", "age", "sex", "race_ethnicity", "measure"))

# -----------------------------------------------------------------------------
# 9. Write outputs
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)
write_parquet(sti_state,     "dist/sti_state.parquet")
write_parquet(sti_county,    "dist/sti_county.parquet")
write_parquet(sti_quarterly, "dist/sti_quarterly.parquet")
write_parquet(sti_weekly,    "dist/sti_weekly.parquet")
write_parquet(sti_youth,     "dist/sti_youth.parquet")

report <- function(df, name) {
  sprintf("  %-22s: %d rows, %d measures, %d geographies, %s to %s",
          name, nrow(df), n_distinct(df$measure), n_distinct(df$geography),
          min(df$time), max(df$time))
}
message(
  "bundle_sti:\n",
  report(sti_state,     "sti_state.parquet"), "\n",
  report(sti_county,    "sti_county.parquet"), "\n",
  report(sti_quarterly, "sti_quarterly.parquet"), "\n",
  report(sti_weekly,    "sti_weekly.parquet"), "\n",
  report(sti_youth,     "sti_youth.parquet")
)
