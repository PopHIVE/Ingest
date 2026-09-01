# =============================================================================
# Bundle: County Access to Care
# Combines county-level healthcare access measures from County Health Rankings
# into a single long-format parquet file queryable by county FIPS + year.
#
# Sources:
#   - county_health_rankings/standard/data_county.csv.gz (CHR&R via Zenodo)
#
# Output:
#   - dist/county_access.parquet
#     One row per county (5-digit FIPS) x year x outcome_name, 2010-2025.
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

ACCESS_MEASURES <- c(
  # Provider availability
  "chr_primary_care_physicians",
  "chr_other_primary_care_providers",
  "chr_mental_health_providers",
  "chr_dentists",
  # Insurance coverage
  "chr_uninsured",
  "chr_uninsured_adults",
  "chr_uninsured_children",
  # Access barriers
  "chr_could_not_see_doctor_due_to_cost",
  "chr_did_not_get_needed_health_care",
  # Preventive care utilization
  "chr_mammography_screening"
)

# -----------------------------------------------------------------------------
# 1. Load source data
# -----------------------------------------------------------------------------

chr_path <- "../county_health_rankings/standard/data_county.csv.gz"

if (!file.exists(chr_path)) {
  stop(
    "county_health_rankings/standard/data_county.csv.gz not found. ",
    "Run the county_health_rankings ingest first."
  )
}

chr_raw <- vroom(chr_path, show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 2. Filter to access measure columns, clean, and pivot to long format
# -----------------------------------------------------------------------------

available_measures <- intersect(ACCESS_MEASURES, colnames(chr_raw))

missing_measures <- setdiff(ACCESS_MEASURES, colnames(chr_raw))
if (length(missing_measures) > 0) {
  warning(
    "The following expected CHR&R measures were not found in source data ",
    "and will be absent from the bundle output:\n",
    paste(" -", missing_measures, collapse = "\n")
  )
}

county_access <- chr_raw %>%
  filter(nchar(geography) == 5) %>%
  select(geography, time, all_of(available_measures)) %>%
  mutate(geography = formatC(as.integer(geography), width = 5, flag = "0")) %>%
  pivot_longer(
    cols = all_of(available_measures),
    names_to = "outcome_name",
    values_to = "value"
  ) %>%
  filter(!is.na(value)) %>%
  arrange(outcome_name, geography, time)

# -----------------------------------------------------------------------------
# 3. Validate — check for duplicate geography-time-outcome_name rows
# -----------------------------------------------------------------------------

dupes <- county_access %>%
  count(geography, time, outcome_name) %>%
  filter(n > 1)

if (nrow(dupes) > 0) {
  # Check whether duplicates have differing values — if so, a stratification
  # column is likely missing and the data should not be silently deduplicated
  dupe_values <- county_access %>%
    semi_join(dupes, by = c("geography", "time", "outcome_name")) %>%
    group_by(geography, time, outcome_name) %>%
    summarize(n_distinct_values = n_distinct(value, na.rm = TRUE), .groups = "drop") %>%
    filter(n_distinct_values > 1)

  if (nrow(dupe_values) > 0) {
    stop(
      nrow(dupe_values), " duplicate geography-time-outcome_name combinations ",
      "have differing values — a stratification column (e.g. age, sex) may be ",
      "missing. Inspect before proceeding."
    )
  }

  warning(
    nrow(dupes), " duplicate geography-time-outcome_name combinations found ",
    "(values are identical). Keeping first occurrence."
  )
  county_access <- county_access %>%
    group_by(geography, time, outcome_name) %>%
    slice(1) %>%
    ungroup()
}

# -----------------------------------------------------------------------------
# 4. Write output
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)

write_parquet(county_access, "dist/county_access.parquet")

message(
  "bundle_county_access: wrote ", nrow(county_access), " rows x ",
  ncol(county_access), " columns to dist/county_access.parquet\n",
  "  Counties: ", n_distinct(county_access$geography), "\n",
  "  Years:    ", paste(
    format(min(county_access$time), "%Y"), "to",
    format(max(county_access$time), "%Y")
  ), "\n",
  "  Measures: ", paste(available_measures, collapse = ", ")
)

# -----------------------------------------------------------------------------
# 5. County and state social/economic/health-resource determinants
#    (moved here from the former bundle_rural_health)
#    Sources: hud_chas, area_health_resource_file, bls_laus, usda_food_access,
#             census (ACS 5-year SDOH, SAHIE, SAIPE, urban/rural, OQM)
#    Outputs: dist/county_determinants.parquet, dist/state_determinants.parquet
#    Same long format as county_access.parquet, plus a `source` column.
# -----------------------------------------------------------------------------

det_paths <- c(
  hud_county  = "../hud_chas/standard/data_county.csv.gz",
  hud_state   = "../hud_chas/standard/data_state.csv.gz",
  ahrf        = "../area_health_resource_file/standard/data.csv.gz",
  bls_county  = "../bls_laus/standard/data_county.csv.gz",
  bls_state   = "../bls_laus/standard/data_state.csv.gz",
  usda_county = "../usda_food_access/standard/data_county.csv.gz",
  acs_state   = "../census/standard/data_state.csv.gz",
  acs_county  = "../census/standard/data_county.csv.gz",
  sahie       = "../census/standard/data_sahie.csv.gz",
  saipe       = "../census/standard/data_saipe.csv.gz",
  oqm         = "../census/standard/data_oqm.csv.gz"
)

det_missing <- det_paths[!file.exists(det_paths)]
if (length(det_missing) > 0) {
  stop(
    "Missing source files (run the corresponding ingest first):\n",
    paste(" -", det_missing, collapse = "\n")
  )
}

read_det_source <- function(path, source_label) {
  vroom(path, show_col_types = FALSE,
        col_types = cols(geography = col_character(), .default = col_guess())) %>%
    filter(!is.na(geography)) %>%
    mutate(
      time = as.Date(time),
      geography = if_else(
        nchar(geography) > 2,
        formatC(as.integer(geography), width = 5, flag = "0"),
        formatC(as.integer(geography), width = 2, flag = "0")
      )
    ) %>%
    pivot_longer(
      cols = -c(geography, time),
      names_to = "outcome_name",
      values_to = "value",
      values_transform = as.numeric
    ) %>%
    filter(!is.na(value)) %>%
    mutate(source = source_label)
}

# Same long format as read_det_source(), but for standard files whose columns
# are split across bundles: only `measures` are kept, and rows are restricted to
# one geography level. The level filter is needed because the census SAHIE,
# SAIPE and OQM files each carry national ("00"), state and county rows in a
# single file. Census geography codes are already zero-padded, so no reformat.
read_det_census <- function(path, measures, source_label, geo_nchar) {
  raw <- vroom(path, show_col_types = FALSE,
               col_types = cols(geography = col_character(), .default = col_guess()))

  present <- intersect(measures, colnames(raw))
  absent <- setdiff(measures, colnames(raw))
  if (length(absent) > 0) {
    warning(
      "Expected census measures not found in ", basename(path), " (skipped):\n",
      paste(" -", absent, collapse = "\n")
    )
  }
  if (length(present) == 0) {
    return(NULL)
  }

  raw %>%
    filter(!is.na(geography), nchar(geography) == geo_nchar) %>%
    select(geography, time, all_of(present)) %>%
    mutate(time = as.Date(time)) %>%
    pivot_longer(
      cols = all_of(present),
      names_to = "outcome_name",
      values_to = "value",
      values_transform = as.numeric
    ) %>%
    filter(!is.na(value)) %>%
    mutate(source = source_label)
}

# Census ACS 5-year social determinants. Population-structure measures from the
# same standard files (acs_POP*, acs_PCT*, acs_AGE, acs_DEP, acs_DIS, acs_REX,
# acs_VAL) are not here, and acs_BTH belongs to bundle_maternal_health,
# so this is an explicit allow-list rather than "every column".
ACS_SDOH_MEASURES <- c(
  # Economic stability — income, poverty, housing cost, employment
  "acs_INB", "acs_INC", "acs_PCI",
  "acs_INL", "acs_INM", "acs_INN", "acs_INO", "acs_INP", "acs_INQ",
  "acs_GNI", "acs_OWS",
  "acs_POV", "acs_PVA", "acs_PVB", "acs_PVC",
  "acs_HBU", "acs_HBS",
  "acs_UMP",
  # Education access and quality
  "acs_EDB", "acs_EDC", "acs_DCY", "acs_LEQ",
  # Health care access and quality — insurance coverage
  "acs_UNS", "acs_MCD", "acs_MCR",
  # Neighborhood and built environment — housing quality, utilities, transit
  "acs_HUO", "acs_HUN", "acs_HTJ", "acs_HUF", "acs_HUG", "acs_GRP",
  "acs_BDB", "acs_WWN", "acs_PUB",
  # Social and community context
  "acs_HTA",
  # Food access — grouped with USDA Food Access Research Atlas below rather
  # than with the clinical measures of bundle_preventative_services.
  "acs_SNP"
)

# Urban/rural allocation. County-level only, and shipped inside the ACS county
# standard file rather than a file of its own.
CENSUS_URBAN_MEASURES <- c(
  "census_ur_pct_urban_pop", "census_ur_pct_urban_land", "census_ur_pct_urban_hu"
)

SAHIE_MEASURES <- c(
  "sahie_pct_uninsured", "sahie_pct_uninsured_adults", "sahie_pct_uninsured_children"
)
SAIPE_MEASURES <- c("saipe_pct_children_poverty", "saipe_median_household_income")
OQM_MEASURES <- c("oqm_self_response_rate")

det_hud_county  <- read_det_source(det_paths[["hud_county"]],  "HUD CHAS")
det_hud_state   <- read_det_source(det_paths[["hud_state"]],   "HUD CHAS")
det_ahrf        <- read_det_source(det_paths[["ahrf"]],        "HRSA AHRF")
det_bls_county  <- read_det_source(det_paths[["bls_county"]],  "BLS LAUS")
det_bls_state   <- read_det_source(det_paths[["bls_state"]],   "BLS LAUS")
det_usda_county <- read_det_source(det_paths[["usda_county"]], "USDA Food Access Research Atlas")

det_census_county <- bind_rows(
  read_det_census(det_paths[["acs_county"]], ACS_SDOH_MEASURES, "Census ACS 5-Year", 5),
  read_det_census(det_paths[["acs_county"]], CENSUS_URBAN_MEASURES, "Census Urban Areas", 5),
  read_det_census(det_paths[["sahie"]], SAHIE_MEASURES, "Census SAHIE", 5),
  read_det_census(det_paths[["saipe"]], SAIPE_MEASURES, "Census SAIPE", 5),
  read_det_census(det_paths[["oqm"]], OQM_MEASURES, "Census OQM", 5)
)

# The ACS state file carries the national total as geography "00", the same
# convention state_determinants.parquet already uses; census_ur_* has no
# state-level equivalent.
det_census_state <- bind_rows(
  read_det_census(det_paths[["acs_state"]], ACS_SDOH_MEASURES, "Census ACS 5-Year", 2),
  read_det_census(det_paths[["sahie"]], SAHIE_MEASURES, "Census SAHIE", 2),
  read_det_census(det_paths[["saipe"]], SAIPE_MEASURES, "Census SAIPE", 2),
  read_det_census(det_paths[["oqm"]], OQM_MEASURES, "Census OQM", 2)
)

county_determinants <- bind_rows(
  det_hud_county,
  det_ahrf %>% filter(nchar(geography) == 5),
  det_bls_county,
  det_usda_county,
  det_census_county
) %>%
  filter(nchar(geography) == 5) %>%
  select(geography, time, outcome_name, value, source) %>%
  arrange(outcome_name, geography, time)

state_determinants <- bind_rows(
  det_hud_state,
  det_ahrf %>% filter(nchar(geography) == 2),
  det_bls_state,
  det_census_state
) %>%
  filter(nchar(geography) == 2) %>%
  select(geography, time, outcome_name, value, source) %>%
  arrange(outcome_name, geography, time)

check_det_dupes <- function(df, label) {
  det_dupes <- df %>%
    count(geography, time, outcome_name) %>%
    filter(n > 1)
  if (nrow(det_dupes) > 0) {
    stop(
      nrow(det_dupes), " duplicate geography-time-outcome_name combinations in ",
      label, ". Inspect before proceeding."
    )
  }
  invisible(TRUE)
}

check_det_dupes(county_determinants, "county_determinants")
check_det_dupes(state_determinants,  "state_determinants")

write_parquet(county_determinants, "dist/county_determinants.parquet", compression = "snappy")
message(sprintf(
  "Wrote %d rows to dist/county_determinants.parquet (%d counties, %d measures, %s to %s)",
  nrow(county_determinants),
  n_distinct(county_determinants$geography),
  n_distinct(county_determinants$outcome_name),
  format(min(county_determinants$time), "%Y"),
  format(max(county_determinants$time), "%Y")
))

write_parquet(state_determinants, "dist/state_determinants.parquet", compression = "snappy")
message(sprintf(
  "Wrote %d rows to dist/state_determinants.parquet (%d states, %d measures, %s to %s)",
  nrow(state_determinants),
  n_distinct(state_determinants$geography),
  n_distinct(state_determinants$outcome_name),
  format(min(state_determinants$time), "%Y"),
  format(max(state_determinants$time), "%Y")
))
