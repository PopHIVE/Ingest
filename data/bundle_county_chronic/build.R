# =============================================================================
# Bundle: County Chronic Conditions
# Combines county-level chronic condition measures from County Health Rankings
# into a single wide-format parquet file queryable by county FIPS + year.
#
# Sources:
#   - county_health_rankings/standard/data_county.csv.gz (CHR&R via Zenodo)
#
# Output:
#   - dist/county_chronic.parquet
#     One row per county (5-digit FIPS) per year, 2010-2025.
# =============================================================================

library(dplyr)
library(vroom)
library(arrow)

# Measures to retain from CHR&R — chronic conditions only
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
    "Run the county_health_rankings ingest first."
  )
}

chr_raw <- vroom(chr_path, show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 2. Filter to chronic condition columns and clean
# -----------------------------------------------------------------------------

# Keep only columns that exist in the source (guard against future CHR&R
# column name changes)
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
  # Retain only valid 5-digit county FIPS (exclude state-level rows)
  filter(nchar(geography) == 5) %>%
  # Keep index columns + chronic measures
  select(geography, time, all_of(available_measures)) %>%
  # Ensure geography is zero-padded character
  mutate(geography = formatC(as.integer(geography), width = 5, flag = "0")) %>%
  # Drop rows where all measure columns are NA
  filter(if_any(all_of(available_measures), ~ !is.na(.))) %>%
  # Sort for predictable output
  arrange(geography, time)

# -----------------------------------------------------------------------------
# 3. Validate — no duplicate county-year rows
# -----------------------------------------------------------------------------

dupes <- county_chronic %>%
  count(geography, time) %>%
  filter(n > 1)

if (nrow(dupes) > 0) {
  warning(
    nrow(dupes), " duplicate geography-time combinations found. ",
    "Keeping first occurrence."
  )
  county_chronic <- county_chronic %>%
    group_by(geography, time) %>%
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
