# =============================================================================
# Drowning Mortality Data Ingestion
# Source: CDC WONDER, Underlying Cause of Death, 2018-2024 (ICD-10, expanded;
#         https://wonder.cdc.gov/ucd-icd10-expanded.html)
#         Unintentional drowning: ICD-10 codes W65, W66, W67, W68, W69, W70,
#         W73, W74. CDC WONDER has no API, so the exports were downloaded
#         manually (one CSV per year x geography level x demographic
#         breakdown x state for counties) and consolidated into a single
#         zstd-compressed parquet of the original string cells:
#         raw/ucd_icd10_expanded_2018_2024.parquet
#         (columns: year, level, breakdown, file_state, geo_name, geography,
#         raw_category, raw_category_code, deaths, population, crude_rate,
#         crude_rate_lcl, crude_rate_ucl, crude_rate_se; footers removed).
# Geography: State and county. No national totals are computed -- the raw data
#            has no national query, and summing state totals that each contain
#            unknown-magnitude suppressed cells would fabricate a biased number.
# =============================================================================

library(dplyr)
library(purrr)

RAW_FILE <- "raw/ucd_icd10_expanded_2018_2024.parquet"
YEARS <- 2018:2024

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

# CDC WONDER encodes two independent kinds of missingness in a value cell.
# They are NOT mutually exclusive -- a single row can carry both, and in the
# county files that combination is common.
#
#   Suppressed     confidentiality suppression, applied to any cell
#                  representing 1-9 deaths. Occurs in Deaths and in the rate
#                  columns derived from it, never in Population.
#
#   Not Applicable no population denominator exists for this geography and
#   Not Available  demographic combination, so nothing derived from a
#                  denominator can be reported. Occurs in Population and the
#                  rate columns, never in Deaths. ("Not Available" is CDC's
#                  alternate wording for the same condition.)
SUPPRESSED_MARKER <- "Suppressed"
MISSING_DENOM_MARKERS <- c("Not Applicable", "Not Available")

# Sentinel written into a value column wherever suppressed_flag or
# missing_denom_flag is 1. Chosen over 0 so a missing cell can never be
# mistaken for a true zero without cross-referencing a flag column.
MISSING_SENTINEL <- -999

RAW_VALUE_COLS <- c(
  "deaths_raw", "population_raw", "crude_rate_raw",
  "crude_rate_lcl_raw", "crude_rate_ucl_raw", "crude_rate_se_raw"
)

# Convert a raw value cell to a number, treating either marker as NA.
to_num <- function(x) {
  suppressWarnings(as.numeric(ifelse(x %in% c(SUPPRESSED_MARKER, MISSING_DENOM_MARKERS), NA, x)))
}

# %in% (not ==) so the length-2 marker vector is not recycled down the column,
# and so blank cells yield FALSE rather than NA.
is_suppressed <- function(x) x %in% SUPPRESSED_MARKER
is_missing_denom <- function(x) x %in% MISSING_DENOM_MARKERS

# TRUE for a row where `pred` holds in at least one of the named cells.
any_cell <- function(df, cols, pred) Reduce(`|`, lapply(df[cols], pred))

# Guard against silent data drift: a cell that is neither numeric nor a known
# marker would become an unflagged sentinel. Surface it loudly instead.
warn_unknown_values <- function(df, label, cols = RAW_VALUE_COLS) {
  vals <- unique(unlist(df[cols], use.names = FALSE))
  unknown <- vals[
    is.na(vals) |
      (is.na(suppressWarnings(as.numeric(vals))) &
        !(vals %in% c(SUPPRESSED_MARKER, MISSING_DENOM_MARKERS)))
  ]
  if (length(unknown)) {
    warning(
      "Unrecognized value(s) in the numeric cells of ", label, ": ",
      paste(ifelse(is.na(unknown), "<blank>", unknown), collapse = ", "),
      ". These become unflagged ", MISSING_SENTINEL, "s; extend SUPPRESSED_MARKER or ",
      "MISSING_DENOM_MARKERS to classify them.",
      call. = FALSE
    )
  }
}

VALUE_COLS <- c(
  "drowning_deaths", "drowning_population", "drowning_crude_rate",
  "drowning_crude_rate_lcl", "drowning_crude_rate_ucl"
)

# Called only after all aggregation, so sums/rates are computed on real NAs.
fill_missing_with_sentinel <- function(df) {
  df %>% mutate(across(all_of(VALUE_COLS), ~ ifelse(is.na(.x), MISSING_SENTINEL, .x)))
}

clean_age_label <- function(x) {
  x <- gsub(" years?$", "", x)
  x <- gsub("^< 1$", "<1", x)
  x
}

clean_ethnicity_label <- function(x) {
  case_when(
    x == "Hispanic or Latino" ~ "Hispanic",
    x == "Not Hispanic or Latino" ~ "Not Hispanic",
    TRUE ~ x
  )
}

# Standardizes one raw slice (one year x level x breakdown) into long format
# for a single dimension (age, sex, or race_ethnicity), with the other two
# dimensions set to "Overall".
#
# suppressed_flag and missing_denom_flag are row-level and independent: each is
# 1 if *any* of the row's six value cells carried the corresponding marker.
# Both are 1 when a row has both kinds of marker in different cells -- common
# in the county files, where Deaths is "Suppressed" and Population is
# "Not Applicable".
read_category <- function(df, dim, map_fn, drop_raw = character(0), time_val) {
  df <- df %>%
    rename(
      deaths_raw = deaths, population_raw = population, crude_rate_raw = crude_rate,
      crude_rate_lcl_raw = crude_rate_lcl, crude_rate_ucl_raw = crude_rate_ucl,
      crude_rate_se_raw = crude_rate_se
    ) %>%
    filter(!(raw_category %in% drop_raw)) %>%
    mutate(category_value = map_fn(raw_category))

  warn_unknown_values(df, paste(time_val, dim))

  df$suppressed_flag <- as.integer(any_cell(df, RAW_VALUE_COLS, is_suppressed))
  df$missing_denom_flag <- as.integer(any_cell(df, RAW_VALUE_COLS, is_missing_denom))

  out <- df %>%
    transmute(
      geography = geography,
      time = time_val,
      suppressed_flag = suppressed_flag,
      missing_denom_flag = missing_denom_flag,
      drowning_deaths = to_num(deaths_raw),
      drowning_population = to_num(population_raw),
      drowning_crude_rate = to_num(crude_rate_raw),
      drowning_crude_rate_lcl = to_num(crude_rate_lcl_raw),
      drowning_crude_rate_ucl = to_num(crude_rate_ucl_raw),
      !!dim := category_value
    )

  for (other_dim in setdiff(c("age", "sex", "race_ethnicity"), dim)) {
    out[[other_dim]] <- "Overall"
  }
  out
}

# CDC WONDER does not report Population or Crude Rate for County x Ten-Year
# Age Groups (the export silently omits them on every state/year). Deaths are
# still reported, and County x Five-Year Age Groups still reports Population, so
# population is derived by summing the Five-Year county population export into
# Ten-Year buckets. The two boundary groups (<1, 1-4) match 1:1; every other
# Ten-Year bucket sums two Five-Year groups, except 85+, which sums
# 85-89/90-94/95-99/100+ (county Five-Year age goes further than state).
FIVE_TO_TEN_YEAR_AGE <- c(
  "< 1 year" = "<1",
  "1-4 years" = "1-4",
  "5-9 years" = "5-14", "10-14 years" = "5-14",
  "15-19 years" = "15-24", "20-24 years" = "15-24",
  "25-29 years" = "25-34", "30-34 years" = "25-34",
  "35-39 years" = "35-44", "40-44 years" = "35-44",
  "45-49 years" = "45-54", "50-54 years" = "45-54",
  "55-59 years" = "55-64", "60-64 years" = "55-64",
  "65-69 years" = "65-74", "70-74 years" = "65-74",
  "75-79 years" = "75-84", "80-84 years" = "75-84",
  "85-89 years" = "85+", "90-94 years" = "85+",
  "95-99 years" = "85+", "100+ years" = "85+"
)

# County Ten-Year age deaths (Deaths cell only; no population/rate cells).
read_county_age_deaths <- function(df, time_val) {
  df <- df %>% filter(raw_category != "Not Stated")
  warn_unknown_values(df, paste(time_val, "county age"), cols = "deaths")

  df %>%
    transmute(
      geography = geography,
      time = time_val,
      age = clean_age_label(raw_category),
      suppressed_flag = as.integer(is_suppressed(deaths)),
      drowning_deaths = to_num(deaths)
    )
}

# County Five-Year age export, used only for Population, re-aggregated into
# Ten-Year buckets. missing_denom_flag is 1 if ANY Five-Year cell folded into a
# bucket was missing its population -- a partial sum would understate the total.
read_county_pop5yr <- function(df) {
  df %>%
    filter(trimws(raw_category) %in% names(FIVE_TO_TEN_YEAR_AGE)) %>%
    mutate(age = FIVE_TO_TEN_YEAR_AGE[trimws(raw_category)]) %>%
    group_by(geography, age) %>%
    summarize(
      drowning_population = sum(to_num(population), na.rm = FALSE),
      missing_denom_flag = as.integer(any(is_missing_denom(population))),
      .groups = "drop"
    )
}

# A county/age cell with no Five-Year population match at all (e.g. a state
# absent from the Five-Year export for a year) is treated as
# missing_denom_flag = 1 rather than silently left NA.
build_county_age <- function(deaths_raw, pop_raw, time_val) {
  deaths_df <- read_county_age_deaths(deaths_raw, time_val)
  pop_df <- read_county_pop5yr(pop_raw)

  deaths_df %>%
    left_join(pop_df, by = c("geography", "age")) %>%
    mutate(
      missing_denom_flag = as.integer(coalesce(missing_denom_flag, 1L) == 1),
      drowning_crude_rate = ifelse(
        is.na(drowning_deaths) | is.na(drowning_population) | missing_denom_flag == 1 | drowning_population == 0,
        NA_real_,
        drowning_deaths / drowning_population * 1e5
      ),
      # Not computed: a valid CI for a rate built from a derived population
      # needs CDC's internal interval method, which is out of scope here.
      drowning_crude_rate_lcl = NA_real_,
      drowning_crude_rate_ucl = NA_real_,
      sex = "Overall", race_ethnicity = "Overall"
    )
}

# "Overall" row per geography/time by summing Female + Male -- the only
# exhaustive 2-way split. If either sex is missing the total is left NA rather
# than fabricated, and both flags carry up from the sex rows.
build_overall_from_sex <- function(sex_df) {
  sex_df %>%
    group_by(geography, time) %>%
    summarize(
      drowning_deaths = sum(drowning_deaths, na.rm = FALSE),
      drowning_population = sum(drowning_population, na.rm = FALSE),
      suppressed_flag = as.integer(any(suppressed_flag == 1)),
      missing_denom_flag = as.integer(any(missing_denom_flag == 1)),
      .groups = "drop"
    ) %>%
    mutate(
      drowning_crude_rate = ifelse(
        is.na(drowning_deaths) | is.na(drowning_population) | drowning_population == 0,
        NA_real_,
        drowning_deaths / drowning_population * 1e5
      ),
      drowning_crude_rate_lcl = NA_real_,
      drowning_crude_rate_ucl = NA_real_,
      age = "Overall", sex = "Overall", race_ethnicity = "Overall"
    )
}

# Full standardized long table for one geography level across all years and
# the four demographic breakdowns.
build_level <- function(raw, level) {
  raw <- raw %>% filter(level == !!level)
  slice_of <- function(yr, bd) raw %>% filter(year == yr, breakdown == bd)

  age_parts <- list()
  sex_parts <- list()
  race_eth_parts <- list()

  for (yr in YEARS) {
    time_val <- sprintf("%d-12-31", yr)
    key <- as.character(yr)

    age_parts[[key]] <- if (level == "state") {
      read_category(slice_of(yr, "age"), "age", clean_age_label, drop_raw = "Not Stated", time_val = time_val)
    } else {
      build_county_age(slice_of(yr, "age"), slice_of(yr, "pop5yr"), time_val)
    }
    sex_parts[[key]] <- read_category(slice_of(yr, "gender"), "sex", identity, time_val = time_val)
    race_rows <- read_category(slice_of(yr, "race"), "race_ethnicity", identity,
      drop_raw = "Not Available", time_val = time_val
    )
    eth_rows <- read_category(slice_of(yr, "ethnicity"), "race_ethnicity", clean_ethnicity_label,
      drop_raw = "Not Stated", time_val = time_val
    )
    race_eth_parts[[key]] <- bind_rows(race_rows, eth_rows)
  }

  sex_all <- bind_rows(sex_parts)

  bind_rows(bind_rows(age_parts), sex_all, bind_rows(race_eth_parts), build_overall_from_sex(sex_all)) %>%
    fill_missing_with_sentinel() %>%
    select(
      geography, time, age, sex, race_ethnicity,
      drowning_deaths, drowning_population, drowning_crude_rate,
      drowning_crude_rate_lcl, drowning_crude_rate_ucl,
      suppressed_flag, missing_denom_flag
    ) %>%
    arrange(geography, time, age, sex, race_ethnicity)
}

# -----------------------------------------------------------------------------
# 1. Initialize process record and check for changes
# -----------------------------------------------------------------------------
process <- dcf::dcf_process_record()

raw_state <- list(hash = unname(tools::md5sum(RAW_FILE)))

if (!identical(process$raw_state, raw_state)) {
  # ---------------------------------------------------------------------------
  # 2. Build state- and county-level standardized tables
  # ---------------------------------------------------------------------------
  raw <- arrow::read_parquet(RAW_FILE)

  data_state <- build_level(raw, "state")
  data_county <- build_level(raw, "county")

  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_state, "standard/data_state.csv.gz", delim = ",")
  vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 4. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
