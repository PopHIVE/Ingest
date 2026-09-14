# =============================================================================
# County Buddy Data Ingestion
# Source: https://github.com/ColinVu/CountyBuddy/blob/main/county_data.csv
#
# County Buddy (Vu, Baniassad & Andris, Georgia Tech College of Computing) is a
# companion dataset that flags U.S. counties with an unusually large
# incarcerated, university student, active-military, or Native American
# population -- populations the authors found associated with geographic
# anomalies in socio-economic choropleths (e.g. life expectancy, income).
#
# A county appears as a row ONLY if at least one of the four populations
# exceeds 2 standard deviations above the national mean (the paper's outlier
# threshold for county-level data); categories that don't meet that bar are
# zeroed out within an included row. Counties with no outlier category in any
# of the four dimensions are absent from the file entirely -- this is a
# curated outlier list, not a complete county census.
#
# Underlying figures are point-in-time, not a time series: total/group-quarters
# population and Native American population are 2020 Census; prison population
# uses 2017 facility-capacity data; military bases are as of 2023; university
# enrollment is as of 2020. We record all rows at time = 2020-12-31 (the
# dataset's dominant vintage) per the Informational Document at
# https://github.com/ColinVu/CountyBuddy/blob/main/County_Buddy_Informational_Document.pdf
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
dir.create("raw", showWarnings = FALSE)

download.file(
  "https://raw.githubusercontent.com/ColinVu/CountyBuddy/main/county_data.csv",
  "raw/county_data.csv",
  mode = "wb",
  quiet = TRUE
)

raw_state <- list(hash = tools::md5sum("raw/county_data.csv")[[1]])

# -----------------------------------------------------------------------------
# 2. Check for changes
# -----------------------------------------------------------------------------
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 3. Read raw data
  # ---------------------------------------------------------------------------
  raw <- vroom::vroom("raw/county_data.csv", show_col_types = FALSE, altrep = FALSE) %>%
    as.data.frame()

  pct_to_numeric <- function(x) as.numeric(sub("%$", "", x))

  # ---------------------------------------------------------------------------
  # 4. Transform to standard wide format
  # ---------------------------------------------------------------------------
  # Institution-name columns (Prisons, Universities, Military_Bases,
  # Native_American_Reservations) are free-text lists, not measures, and are
  # dropped -- only the numeric population counts/percentages are kept.
  data_standard <- raw %>%
    transmute(
      geography = sprintf("%05d", as.integer(FIPS)),
      time = "2020-12-31",
      countybuddy_pop_total = as.numeric(Total_Population),
      countybuddy_pop_prison = as.numeric(Prison_Population),
      countybuddy_pct_prison = pct_to_numeric(Percent_In_Prison),
      countybuddy_pop_student = as.numeric(Student_Population),
      countybuddy_pct_student = pct_to_numeric(Percent_Student),
      countybuddy_pop_military = as.numeric(Military_Population),
      countybuddy_pct_military = pct_to_numeric(Percent_In_Military),
      countybuddy_pop_native_american = as.numeric(Native_American_Population),
      countybuddy_pct_native_american = pct_to_numeric(Percent_Native_American)
    ) %>%
    arrange(geography)

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
