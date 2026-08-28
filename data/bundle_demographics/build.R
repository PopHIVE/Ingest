# =============================================================================
# Bundle: Population Demographics
#
# Combines Census population-structure measures into tall-format parquet files
# keyed by geography (FIPS) + year + measure. This is the shared
# denominator/context bundle: population counts and shares by age, sex, and
# race/ethnicity, plus median age, dependency ratio, disability, diversity, and
# median home value.
#
# Sources:
#   - census/standard/data_state.csv.gz  (ACS 5-year, 2-digit FIPS + "00")
#   - census/standard/data_county.csv.gz (ACS 5-year, 5-digit FIPS)
#   - census/standard/data_pep.csv.gz    (Population Estimates Program; carries
#                                         national, state and county rows in a
#                                         single file)
#
# Outputs:
#   - dist/demographics_state.parquet  : geography(2-digit or "00") x time x
#                                        measure x value x source
#   - dist/demographics_county.parquet : geography(5-digit) x time x
#                                        measure x value x source
#
# The other Census measure families deliberately live elsewhere:
#   acs_BTH (fertility)                -> bundle_maternal_health
#   ACS SDOH, SAHIE, SAIPE, urban/rural, OQM -> bundle_county_access
#     (county_determinants / state_determinants)
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

# -----------------------------------------------------------------------------
# 0. Configuration: which measures belong to this bundle
# -----------------------------------------------------------------------------

# ACS 5-year — population structure. Available at both state and county level.
ACS_MEASURES <- c(
  # Summary measures
  "acs_POP", "acs_AGE", "acs_DEP", "acs_DIS", "acs_REX", "acs_VAL",
  # Population counts by sex
  "acs_POP_M", "acs_POP_F",
  # Population counts by age band
  "acs_POP_I", "acs_POP_J", "acs_POP_Y", "acs_POP_O", "acs_POP_S",
  # Population counts by race/ethnicity
  "acs_POP_W", "acs_POP_B", "acs_POP_A", "acs_POP_H",
  "acs_POP_P", "acs_POP_P1", "acs_POP_Q",
  # Population shares by sex
  "acs_PCT_M", "acs_PCT_F",
  # Population shares by age band
  "acs_PCT_I", "acs_PCT_J", "acs_PCT_Y", "acs_PCT_O", "acs_PCT_S",
  # Population shares by race/ethnicity
  "acs_PCT_W", "acs_PCT_B", "acs_PCT_A", "acs_PCT_H",
  "acs_PCT_P", "acs_PCT_P1", "acs_PCT_Q"
)

# Population Estimates Program — a more recent, single-vintage snapshot of the
# same population structure. Kept alongside ACS rather than merged: the two
# programs use different methodologies, so they are distinguished by `source`.
PEP_MEASURES <- c(
  "pep_population",
  "pep_pct_65_older", "pep_pct_under_18", "pep_pct_female",
  "pep_pct_aian", "pep_pct_asian", "pep_pct_nhpi",
  "pep_pct_nh_black", "pep_pct_nh_white", "pep_pct_hispanic"
)

SOURCE_PATHS <- c(
  acs_state  = "../census/standard/data_state.csv.gz",
  acs_county = "../census/standard/data_county.csv.gz",
  pep        = "../census/standard/data_pep.csv.gz"
)

missing_paths <- SOURCE_PATHS[!file.exists(SOURCE_PATHS)]
if (length(missing_paths) > 0) {
  stop(
    "Missing Census source files (run the census ingest first):\n",
    paste(" -", missing_paths, collapse = "\n")
  )
}

# -----------------------------------------------------------------------------
# 1. Helper: read one standard file, keep a measure subset and one geography
#    level, and pivot to tall format.
#
#    `geo_nchar` is needed because data_pep.csv.gz carries national ("00"),
#    state (2-digit) and county (5-digit) rows in the same file.
# -----------------------------------------------------------------------------

read_demographics <- function(path, measures, source_label, geo_nchar) {
  raw <- vroom(
    path,
    show_col_types = FALSE,
    col_types = cols(geography = col_character(), .default = col_guess())
  )

  present <- intersect(measures, colnames(raw))
  absent <- setdiff(measures, colnames(raw))
  if (length(absent) > 0) {
    warning(
      "Expected measures not found in ", basename(path), " (skipped):\n",
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
      names_to = "measure",
      values_to = "value",
      values_transform = as.numeric
    ) %>%
    filter(!is.na(value)) %>%
    mutate(source = source_label)
}

# No duplicate rows for the given key columns with differing values; drops exact
# duplicates, stops if the same key has conflicting values (usually means a
# stratification column is missing from `key_cols`).
check_dupes <- function(df, label,
                        key_cols = c("geography", "time", "measure", "source")) {
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
    warning(
      label, ": ", nrow(dupes),
      " duplicate rows with identical values; keeping first occurrence."
    )
    df <- df %>%
      group_by(across(all_of(key_cols))) %>%
      slice(1) %>%
      ungroup()
  }
  df
}

# -----------------------------------------------------------------------------
# 2. Assemble state (+ national) and county outputs
# -----------------------------------------------------------------------------

demographics_state <- bind_rows(
  read_demographics(SOURCE_PATHS[["acs_state"]], ACS_MEASURES, "Census ACS 5-Year", 2),
  read_demographics(SOURCE_PATHS[["pep"]], PEP_MEASURES, "Census PEP", 2)
) %>%
  select(geography, time, measure, value, source) %>%
  arrange(measure, geography, time)

demographics_county <- bind_rows(
  read_demographics(SOURCE_PATHS[["acs_county"]], ACS_MEASURES, "Census ACS 5-Year", 5),
  read_demographics(SOURCE_PATHS[["pep"]], PEP_MEASURES, "Census PEP", 5)
) %>%
  select(geography, time, measure, value, source) %>%
  arrange(measure, geography, time)

demographics_state <- check_dupes(demographics_state, "demographics_state")
demographics_county <- check_dupes(demographics_county, "demographics_county")

# -----------------------------------------------------------------------------
# 3. Write outputs
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)

write_parquet(demographics_state, "dist/demographics_state.parquet", compression = "snappy")
write_parquet(demographics_county, "dist/demographics_county.parquet", compression = "snappy")

report <- function(df, file) {
  message(sprintf(
    "Wrote %d rows to dist/%s (%d geographies, %d measures, %s to %s)",
    nrow(df), file,
    n_distinct(df$geography),
    n_distinct(df$measure),
    format(min(df$time), "%Y"),
    format(max(df$time), "%Y")
  ))
}

report(demographics_state, "demographics_state.parquet")
report(demographics_county, "demographics_county.parquet")
