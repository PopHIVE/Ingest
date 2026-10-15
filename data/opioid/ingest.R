# =============================================================================
# Opioid overdose Data Ingestion
# Source: area-level snapshot exports (Results_Opioid-<date>_<AREA>.csv)
#   EMS encounters for opioid overdose per 100,000 (NEMSIS)
#
# No automated download exists -- place fresh Results_Opioid-<date>_<AREA>.csv
# exports in raw/ by hand before running.
#
# Caveats behind the parsing choices below: outcomes are binned ranges only (no
# recoverable point value); `time` is the last date of the period represented
# (01/01/2022 - 07/31/2025) rather than the extract date; demographic enrichment columns
# are dropped. Geography type is "county" (ZCTA3 codes are not FIPS and are
# flagged by `geography_type`).
# =============================================================================

library(dplyr)
library(tibble)
library(stringr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Configuration
# -----------------------------------------------------------------------------
TOPIC <- "opioid"
FILE_PREFIX <- "Opioid"
MEASURE_NAME <- "ems_rate"
GEO_TYPE <- "county"
MID_PREFIX <- "COUNTY "
MAX_VALUE <- NA
TIME <- as.Date("2025-07-31")
AREA_CORRECTIONS <- NULL

# -----------------------------------------------------------------------------
# 2. Change detection on raw files
# -----------------------------------------------------------------------------
raw_files <- list.files(
  "raw",
  pattern = paste0("^Results_", FILE_PREFIX, "[-_][0-9]{4}-[0-9]{2}-[0-9]{2}_.*\\.csv$"),
  full.names = TRUE
)
if (length(raw_files) == 0) {
  stop("No raw/Results_", FILE_PREFIX, "-<date>_<AREA>.csv files found -- place fresh exports in raw/ before running.")
}
# National file first so its rows win any tie when the same county/ZCTA3 also
# appears in a per-area file (see distinct() below).
raw_files <- raw_files[order(!grepl("_National\\.csv$", basename(raw_files)))]
raw_state <- as.list(tools::md5sum(raw_files))
names(raw_state) <- basename(raw_files)

if (!identical(process$raw_state, raw_state)) {
  dir.create("standard", showWarnings = FALSE)

  # ---------------------------------------------------------------------------
  # 3. FIPS lookup (state abbreviation -> state FIPS)
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    distinct(state, .keep_all = TRUE) %>%
    transmute(state, state_fips = geography)

  # ---------------------------------------------------------------------------
  # 4. Parse each area export
  # ---------------------------------------------------------------------------
  parse_one <- function(path) {
    fname <- basename(path)
    m <- str_match(
      fname,
      paste0("^Results_", FILE_PREFIX, "[-_]([0-9]{4}-[0-9]{2}-[0-9]{2})_([A-Za-z]+)\\.csv$")
    )
    if (is.na(m[1, 1])) stop("Unrecognized raw filename pattern: ", fname)
    area <- m[1, 3]
    if (area %in% names(AREA_CORRECTIONS)) area <- AREA_CORRECTIONS[[area]]

    df <- vroom::vroom(path, show_col_types = FALSE)

    range_col <- grep("^Range for (Rate|Aggregate)", names(df), value = TRUE)
    if (length(range_col) != 1) {
      stop("Could not resolve the outcome range column by pattern in ", fname, " -- upstream column may have been renamed.")
    }

    # Bounds are usually plain decimals ("192.3 to 243.9") or percents
    # ("7.2% to 7.7%"), but the top bucket can use "k" shorthand for
    # thousands (e.g. "708.0 to 5.0k High Outlier" -> 708.0 to 5000).
    parsed_range <- str_match(df[[range_col]], "^([0-9.]+)(k)?%? to ([0-9.]+)(k)?%?\\s*(.*)$")
    to_number <- function(digits, k_suffix) as.numeric(digits) * ifelse(is.na(k_suffix), 1, 1000)

    out <- tibble(
      mid = df$MID,
      population = df$POPULATION,
      value_low = to_number(parsed_range[, 2], parsed_range[, 3]),
      value_high = to_number(parsed_range[, 4], parsed_range[, 5]),
      value_label = ifelse(is.na(parsed_range[, 1]), trimws(df[[range_col]]), trimws(parsed_range[, 6])),
      time = TIME
    )

    if (identical(area, "National")) {
      total_geo <- "00"
      total_type <- "national"
    } else {
      total_geo <- state_fips$state_fips[state_fips$state == area]
      if (length(total_geo) != 1) {
        stop("No state FIPS match for area '", area, "' in ", fname, " -- check the all_fips crosswalk.")
      }
      total_type <- "state"
    }
    out %>%
      mutate(
        geography = ifelse(mid == "TOTAL", total_geo, sub(paste0("^", MID_PREFIX), "", mid)),
        geography_type = ifelse(mid == "TOTAL", total_type, GEO_TYPE)
      )
  }

  rows <- bind_rows(lapply(raw_files, parse_one)) %>%
    distinct(geography, geography_type, time, .keep_all = TRUE)

  low_col <- paste0(TOPIC, "_", MEASURE_NAME, "_low")
  high_col <- paste0(TOPIC, "_", MEASURE_NAME, "_high")

  data_standard <- rows %>%
    select(geography, geography_type, time, population, value_low, value_high, value_label) %>%
    rename(
      !!paste0(TOPIC, "_population") := population,
      !!low_col := value_low,
      !!high_col := value_high,
      !!paste0(TOPIC, "_", MEASURE_NAME, "_label") := value_label
    )

  # ---------------------------------------------------------------------------
  # 5. Validate
  # ---------------------------------------------------------------------------
  dup <- data_standard %>% count(geography, geography_type, time) %>% filter(n > 1)
  if (nrow(dup) > 0) stop("Duplicate (geography, geography_type, time) rows found")
  if (any(is.na(data_standard$geography) | is.na(data_standard$time))) stop("Missing geography or time")
  if (any(data_standard[[paste0(TOPIC, "_population")]] < 0, na.rm = TRUE)) stop("Negative population value")
  if (any(data_standard[[low_col]] < 0, na.rm = TRUE) || any(data_standard[[high_col]] < 0, na.rm = TRUE)) {
    stop("Negative value bound")
  }
  if (any(data_standard[[low_col]] > data_standard[[high_col]], na.rm = TRUE)) {
    stop("Low bound exceeds high bound in at least one row")
  }
  if (!is.na(MAX_VALUE) && any(data_standard[[high_col]] > MAX_VALUE, na.rm = TRUE)) {
    stop("Value bound exceeds expected max (", MAX_VALUE, ")")
  }

  # ---------------------------------------------------------------------------
  # 6. Write standardized output and record processed state
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
