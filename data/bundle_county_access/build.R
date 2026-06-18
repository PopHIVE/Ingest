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
