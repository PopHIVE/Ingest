# =============================================================================
# Bundle: County Chronic Conditions
# Combines county-level chronic condition measures from County Health Rankings
# into a single long-format parquet file queryable by county FIPS + year.
#
# Sources:
#   - county_health_rankings/standard/data_county.csv.gz (CHR&R via Zenodo)
#
# Output:
#   - dist/county_chronic.parquet
#     One row per county (5-digit FIPS) x year x outcome_name, 2010-2025.
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

CHR_MEASURES <- c(
  "chr_diabetes_prevalence",
  "chr_adult_obesity",
  "chr_poor_or_fair_health",
  "chr_poor_physical_health_days",
  "chr_poor_mental_health_days",
  "chr_premature_death",
  "chr_premature_age_adjusted_mortality",
  "chr_life_expectancy",
  "chr_preventable_hospital_stays"
)

# -----------------------------------------------------------------------------
# 1. Load source data
# -----------------------------------------------------------------------------

chr_path <- "../county_health_rankings/standard/data_county.csv.gz"

if (!file.exists(chr_path)) {
  stop(
    "county_health_rankings/standard/data_county.csv.gz not found. ",
    "This file is produced by the county_health_rankings repo — run its ingest first."
  )
}

chr_raw <- vroom(chr_path, show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 2. Filter to chronic condition columns, clean, and pivot to long format
# -----------------------------------------------------------------------------

available_measures <- intersect(CHR_MEASURES, colnames(chr_raw))

missing_measures <- setdiff(CHR_MEASURES, colnames(chr_raw))
if (length(missing_measures) > 0) {
  warning(
    "The following expected CHR&R measures were not found in source data ",
    "and will be absent from the bundle output:\n",
    paste(" -", missing_measures, collapse = "\n")
  )
}

county_chronic <- chr_raw %>%
  filter(nchar(geography) == 5) %>%
  select(geography, time, all_of(available_measures)) %>%
  mutate(geography = formatC(as.integer(geography), width = 5, flag = "0")) %>%
  pivot_longer(
    cols = all_of(available_measures),
    names_to = "outcome_name",
    values_to = "value"
  ) %>%
  filter(!is.na(value)) %>%
  arrange(geography, time, outcome_name)

# -----------------------------------------------------------------------------
# 3. Validate — check for duplicate geography-time-outcome_name rows
# -----------------------------------------------------------------------------

dupes <- county_chronic %>%
  count(geography, time, outcome_name) %>%
  filter(n > 1)

if (nrow(dupes) > 0) {
  # Check whether duplicates have differing values — if so, a stratification
  # column is likely missing and the data should not be silently deduplicated
  dupe_values <- county_chronic %>%
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
  county_chronic <- county_chronic %>%
    group_by(geography, time, outcome_name) %>%
    slice(1) %>%
    ungroup()
}

# -----------------------------------------------------------------------------
# 4. Write output
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)

write_parquet(county_chronic, "dist/county_chronic.parquet")

message(
  "bundle_county_chronic: wrote ", nrow(county_chronic), " rows x ",
  ncol(county_chronic), " columns to dist/county_chronic.parquet\n",
  "  Counties: ", n_distinct(county_chronic$geography), "\n",
  "  Years:    ", paste(
    format(min(county_chronic$time), "%Y"), "to",
    format(max(county_chronic$time), "%Y")
  ), "\n",
  "  Measures: ", paste(available_measures, collapse = ", ")
)
