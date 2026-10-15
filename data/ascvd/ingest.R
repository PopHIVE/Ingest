# =============================================================================
# 10-year ASCVD risk, ages 40-70 Data Ingestion
# Source: Veradigm data submission (data_submission_VERADIGM_2025-08.xlsx),
#   first tab "ASCVD BY ZIP3": mean 10-year ASCVD risk by 3-digit ZIP, overall
#   and by age group, race, and sex.
#
# No automated download exists -- place a fresh data_submission_VERADIGM_*.xlsx
# in raw/ by hand before running.
#
# Caveats behind the parsing choices below: the sheet stores risk as a
# proportion (0.0795), converted here to percent (7.95). "." means no
# observations and becomes NA (rows without a value are dropped). `time` is
# the last date of the period represented (EndDate). Geography type is "zcta3"
# (ZIP3 codes are not FIPS and are flagged by `geography_type`); the one row
# with a blank zip3 is the all-area total and is mapped to national ("00").
# Only the first tab is used (the "MONTH BY ZIP3" tab is ignored).
# =============================================================================

library(dplyr)
library(stringr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Change detection on raw file
# -----------------------------------------------------------------------------
raw_files <- list.files("raw", pattern = "^data_submission_VERADIGM_.*\\.xlsx$", full.names = TRUE)
if (length(raw_files) == 0) {
  stop("No raw/data_submission_VERADIGM_*.xlsx file found -- place the submission in raw/ before running.")
}
if (length(raw_files) > 1) stop("More than one raw/data_submission_VERADIGM_*.xlsx file found")
raw_state <- as.list(tools::md5sum(raw_files))
names(raw_state) <- basename(raw_files)

if (!identical(process$raw_state, raw_state)) {
  dir.create("standard", showWarnings = FALSE)

  # ---------------------------------------------------------------------------
  # 2. Read first tab
  # ---------------------------------------------------------------------------
  raw <- readxl::read_excel(raw_files, sheet = 1, col_types = "text")

  num <- function(x) suppressWarnings(as.numeric(x))

  # ---------------------------------------------------------------------------
  # 3. Reshape: one stratum per row (age, race_ethnicity, sex; others "Overall")
  # ---------------------------------------------------------------------------
  strata <- tibble::tribble(
    ~suffix,         ~age,      ~race_ethnicity, ~sex,
    "",              "Overall", "Overall",       "Overall",
    "_age_40_to_49", "40-49",   "Overall",       "Overall",
    "_age_50_to_59", "50-59",   "Overall",       "Overall",
    "_age_60_to_69", "60-69",   "Overall",       "Overall",
    "_age_70",       "70+",     "Overall",       "Overall",
    "_race_asian",   "Overall", "Asian",         "Overall",
    "_race_black",   "Overall", "Black",         "Overall",
    "_race_white",   "Overall", "White",         "Overall",
    "_race_other",   "Overall", "Other",         "Overall",
    "_race_null",    "Overall", "Unknown",       "Overall",
    "_sex_female",   "Overall", "Overall",       "Female",
    "_sex_male",     "Overall", "Overall",       "Male"
  )

  base <- raw %>%
    transmute(
      geography = ifelse(is.na(zip3) | zip3 == "NA", "00", str_pad(zip3, 3, pad = "0")),
      geography_type = ifelse(geography == "00", "national", "zcta3"),
      time = as.Date(substr(EndDate, 1, 10))
    )

  data_standard <- bind_rows(lapply(seq_len(nrow(strata)), function(i) {
    s <- strata[i, ]
    bind_cols(
      base,
      tibble::tibble(
        age = s$age,
        race_ethnicity = s$race_ethnicity,
        sex = s$sex,
        ascvd_risk = num(raw[[paste0("MeanASCVD", s$suffix)]]) * 100,
        ascvd_n = num(raw[[paste0("MeanASCVD", s$suffix, "_obs")]])
      )
    )
  })) %>%
    filter(!is.na(ascvd_risk), !is.na(ascvd_n), ascvd_n > 0) %>%
    arrange(geography_type, geography, age, race_ethnicity, sex)

  # ---------------------------------------------------------------------------
  # 4. Validate
  # ---------------------------------------------------------------------------
  dup <- data_standard %>% count(geography, geography_type, time, age, race_ethnicity, sex) %>% filter(n > 1)
  if (nrow(dup) > 0) stop("Duplicate geography/time/stratum rows found")
  if (any(is.na(data_standard$geography) | is.na(data_standard$time))) stop("Missing geography or time")
  if (any(data_standard$ascvd_risk < 0 | data_standard$ascvd_risk > 100)) stop("ascvd_risk outside 0-100")
  if (sum(data_standard$geography == "00") == 0) stop("No national total row found")

  # ---------------------------------------------------------------------------
  # 5. Write standardized output and record processed state
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
