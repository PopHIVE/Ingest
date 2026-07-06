# =============================================================================
# Bundle: Maternal Health
#
# Combines maternal- and infant-health indicators into tall-format parquet
# files keyed by geography (FIPS) + year + measure.
#
# Sources:
#   - census/standard/data_{state,county}.csv.gz          (ACS, fertility)
#   - county_health_rankings/standard/data_{state,county}.csv.gz (CHR&R)
#   - medicaid_quality/standard/data.csv.gz               (CMS Core Set,
#                                                          state-level only)
#   - cdc_vssr/standard/data.csv.gz                       (NCHS VSRR maternal
#                                                          mortality, national
#                                                          only, monthly, by
#                                                          age & race/ethnicity)
#
# Outputs:
#   - dist/maternal_state.parquet    : geography(2-digit) x time x measure x value
#   - dist/maternal_county.parquet   : geography(5-digit) x time x measure x value
#   - dist/maternal_mortality.parquet: geography("00") x time x age x
#                                       race_ethnicity x measure x value
#                                       (national only, monthly — kept separate
#                                       from maternal_state.parquet because it
#                                       has different time resolution and
#                                       demographic stratification)
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

# Read all columns as character so sparse measure columns aren't mistyped and
# coerced to NA. Numeric coercion happens in pivot_measures().
read_chr <- function(path) vroom(path, col_types = cols(.default = "c"), show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 0. Configuration: source column -> bundle measure id
# -----------------------------------------------------------------------------

# Census (ACS) — state and county
CENSUS_MEASURES <- c(
  acs_BTH = "birth_rate"
)

# County Health Rankings — state and county
CHR_MEASURES <- c(
  chr_teen_births              = "teen_birth_rate",
  chr_low_birth_weight         = "low_birth_weight",
  chr_infant_mortality         = "infant_mortality",
  chr_child_mortality          = "child_mortality",
  chr_smoking_during_pregnancy = "smoking_during_pregnancy",
  chr_breastfeeding            = "breastfeeding"
)

# Medicaid quality (CMS) — state level only; values stored in *_rate columns
MEDICAID_MEASURES <- c(
  medicaid_ppc_ad_rate  = "medicaid_prenatal_postpartum_care_adult",
  medicaid_ppc_ch_rate  = "medicaid_prenatal_postpartum_care_child",
  medicaid_fpc_ch_rate  = "medicaid_first_prenatal_visit",
  medicaid_cpa_ad_rate  = "medicaid_contraceptive_postpartum_adult",
  medicaid_cpc_ch_rate  = "medicaid_contraceptive_postpartum_child",
  medicaid_lbw_ch_rate  = "medicaid_low_birthweight",
  medicaid_lrcd_ch_rate = "medicaid_low_birthweight_risk_adjusted"
)

# CDC VSRR provisional maternal mortality — national only
VSSR_MEASURES <- c(
  vssr_maternal_mortality_rate = "maternal_mortality_rate"
)

# -----------------------------------------------------------------------------
# 1. Helpers
# -----------------------------------------------------------------------------

# Pivot a set of wide measure columns to tall (id_cols + measure, value),
# renaming source columns to bundle measure ids and dropping missing values.
pivot_measures <- function(df, mapping, id_cols = c("geography", "time")) {
  present <- intersect(names(mapping), colnames(df))
  missing <- setdiff(names(mapping), colnames(df))
  if (length(missing) > 0) {
    warning(
      "Expected source columns not found (skipped):\n",
      paste(" -", missing, collapse = "\n")
    )
  }
  df %>%
    select(all_of(id_cols), all_of(present)) %>%
    rename(!!!setNames(present, unname(mapping[present]))) %>%
    pivot_longer(
      cols = all_of(unname(mapping[present])),
      names_to = "measure",
      values_to = "value"
    ) %>%
    mutate(value = suppressWarnings(as.numeric(value))) %>%
    filter(!is.na(value))
}

# No duplicate rows for the given key columns with differing values; drops
# exact duplicates, stops if the same key has conflicting values (usually
# means a stratification column is missing from `key_cols`).
check_dupes <- function(df, label, key_cols = c("geography", "time", "measure")) {
  dupes <- df %>%
    group_by(across(all_of(key_cols))) %>%
    summarize(n = n(), n_distinct_values = n_distinct(value), .groups = "drop") %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    if (any(dupes$n_distinct_values > 1)) {
      stop(
        label, ": ", sum(dupes$n_distinct_values > 1),
        " duplicate ", paste(key_cols, collapse = "-"),
        " rows have differing values — a stratification column may be missing."
      )
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

# -----------------------------------------------------------------------------
# 2. Census (ACS) — state + county
# -----------------------------------------------------------------------------

census_state  <- read_chr("../census/standard/data_state.csv.gz")
census_county <- read_chr("../census/standard/data_county.csv.gz")

census_state_long  <- pivot_measures(census_state,  CENSUS_MEASURES)
census_county_long <- pivot_measures(census_county, CENSUS_MEASURES)

# -----------------------------------------------------------------------------
# 3. County Health Rankings — state + county
# -----------------------------------------------------------------------------

chr_state  <- read_chr("../county_health_rankings/standard/data_state.csv.gz")
chr_county <- read_chr("../county_health_rankings/standard/data_county.csv.gz")

chr_state_long  <- pivot_measures(chr_state,  CHR_MEASURES)
chr_county_long <- pivot_measures(chr_county, CHR_MEASURES)

# Drop corrupt 2013 infant_mortality values: the CHR 2013 release stored a
# mis-scaled/unrelated metric here (483-3042 vs the expected 2-32 per 1,000).
# Proper fix belongs upstream in county_health_rankings/ingest.R.
drop_bad_2013_infant <- function(df) {
  df %>% filter(!(measure == "infant_mortality" &
                    format(as.Date(time), "%Y") == "2013"))
}
chr_state_long  <- drop_bad_2013_infant(chr_state_long)
chr_county_long <- drop_bad_2013_infant(chr_county_long)

# -----------------------------------------------------------------------------
# 4. Medicaid quality (CMS) — state only; geography stored as state name
# -----------------------------------------------------------------------------

medicaid_raw <- read_chr("../medicaid_quality/standard/data.csv.gz")

# Map state names -> 2-digit FIPS using the project crosswalk.
all_fips <- read_chr("../../resources/all_fips.csv.gz")
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(fips = geography, geography_name)

medicaid_long <- medicaid_raw %>%
  # Restrict to Medicaid payer to avoid duplicate rows across payer splits.
  filter(payer == "Medicaid") %>%
  left_join(state_fips_lookup, by = c("geography" = "geography_name")) %>%
  filter(!is.na(fips)) %>%
  select(-geography) %>%
  rename(geography = fips) %>%
  pivot_measures(MEDICAID_MEASURES)

# -----------------------------------------------------------------------------
# 5. Assemble state and county outputs
# -----------------------------------------------------------------------------

maternal_state <- bind_rows(
  census_state_long,
  chr_state_long,
  medicaid_long
) %>%
  mutate(time = as.Date(time)) %>%
  arrange(measure, geography, time)

maternal_county <- bind_rows(
  census_county_long,
  chr_county_long
) %>%
  mutate(
    geography = formatC(as.integer(geography), width = 5, flag = "0"),
    time = as.Date(time)
  ) %>%
  arrange(measure, geography, time)

maternal_state  <- check_dupes(maternal_state,  "maternal_state")
maternal_county <- check_dupes(maternal_county, "maternal_county")

# -----------------------------------------------------------------------------
# 6. CDC VSRR provisional maternal mortality — national only, monthly, kept in
#    its own output (different time resolution + demographic stratification
#    than the annual, non-stratified maternal_state/county measures above).
# -----------------------------------------------------------------------------

vssr_raw <- read_chr("../cdc_vssr/standard/data.csv.gz")

maternal_mortality <- vssr_raw %>%
  pivot_measures(VSSR_MEASURES, id_cols = c("geography", "time", "age", "race_ethnicity")) %>%
  mutate(time = as.Date(time)) %>%
  arrange(measure, age, race_ethnicity, time)

maternal_mortality <- check_dupes(
  maternal_mortality, "maternal_mortality",
  key_cols = c("geography", "time", "age", "race_ethnicity", "measure")
)

# -----------------------------------------------------------------------------
# 7. Write outputs
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)
write_parquet(maternal_state,      "dist/maternal_state.parquet")
write_parquet(maternal_county,     "dist/maternal_county.parquet")
write_parquet(maternal_mortality,  "dist/maternal_mortality.parquet")

message(
  "bundle_maternal_health:\n",
  "  maternal_state.parquet     : ", nrow(maternal_state), " rows, ",
  n_distinct(maternal_state$measure), " measures, ",
  n_distinct(maternal_state$geography), " geographies\n",
  "  maternal_county.parquet    : ", nrow(maternal_county), " rows, ",
  n_distinct(maternal_county$measure), " measures, ",
  n_distinct(maternal_county$geography), " geographies\n",
  "  maternal_mortality.parquet : ", nrow(maternal_mortality), " rows, ",
  n_distinct(maternal_mortality$age), " age groups, ",
  n_distinct(maternal_mortality$race_ethnicity), " race/ethnicity groups\n",
  "  state measures: ", paste(sort(unique(maternal_state$measure)), collapse = ", "), "\n",
  "  county measures: ", paste(sort(unique(maternal_county$measure)), collapse = ", ")
)
