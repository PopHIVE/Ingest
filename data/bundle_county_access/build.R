# =============================================================================
# Bundle: County Access to Care
# Combines county-level healthcare access measures from County Health Rankings
# into a single wide-format parquet file queryable by county FIPS + year.
#
# Sources:
#   - county_health_rankings/standard/data_county.csv.gz (CHR&R via Zenodo)
#
# Output:
#   - dist/county_access.parquet
#     One row per county (5-digit FIPS) per year, 2010-2025.
# =============================================================================

library(dplyr)
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
    "Copy the standardized file from the county_health_rankings repo first."
  )
}

chr_raw <- vroom(chr_path, show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 2. Filter to access measure columns and clean
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
  filter(if_any(all_of(available_measures), ~ !is.na(.))) %>%
  arrange(geography, time)

# -----------------------------------------------------------------------------
# 3. Validate — no duplicate county-year rows
# -----------------------------------------------------------------------------

dupes <- county_access %>%
  count(geography, time) %>%
  filter(n > 1)

if (nrow(dupes) > 0) {
  warning(
    nrow(dupes), " duplicate geography-time combinations found. ",
    "Keeping first occurrence."
  )
  county_access <- county_access %>%
    group_by(geography, time) %>%
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
