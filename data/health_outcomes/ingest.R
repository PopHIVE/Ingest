# =============================================================================
# Health Outcomes Data Ingestion
# Source: area-level snapshot exports (Results_<Topic>-<date>_<AREA>.csv)
#   alcohol  -- EMS encounters for alcohol-related emergencies (NHTSA/NEMSIS)
#   ascvd    -- 10-year ASCVD risk, ages 40-70 (Veradigm)
#   maternal -- severe obstetric complications (HealthVerity)
#   opioid   -- EMS encounters for opioid overdose (NEMSIS)
#
# No automated download exists -- place fresh Results_<Topic>-<date>_<AREA>.csv
# exports in raw/ by hand before running.
#
# Caveats behind the parsing choices below: outcomes are binned ranges only (no
# recoverable point value); `time` is the last date of the period represented
# rather than the extract date; demographic enrichment columns are dropped.
# alcohol/opioid geography is real county FIPS; ascvd/maternal geography is
# ZCTA3 (not FIPS) and is flagged by `geography_type`.
#
# Each topic is written to its own standard file (standard/data_<topic>.csv.gz).
# =============================================================================

library(dplyr)
library(tibble)
library(stringr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Topic configuration
# -----------------------------------------------------------------------------
TOPICS <- list(
  alcohol = list(
    file_prefix = "Alcohol", measure = "ems_rate", geo_type = "county",
    mid_prefix = "COUNTY ", max_value = NA, time = "2025-07-31"
  ),
  ascvd = list(
    file_prefix = "ASCVD", measure = "risk", geo_type = "zcta3",
    mid_prefix = "ZCTA3 ", max_value = 100, time = "2025-07-31"
  ),
  maternal = list(
    file_prefix = "Maternal", measure = "soc_rate", geo_type = "zcta3",
    mid_prefix = "ZCTA3 ", max_value = NA, time = "2025-05-31",
    # The area export named "AR" actually contains ZCTA3 850xxx-865xxx
    # (Arizona's ZIP prefixes, confirmed against the National file) totaling
    # 7,152,939 -- Arizona's population, not Arkansas's. Correct the mislabeled
    # area code before FIPS lookup so this data lands on the right state.
    area_corrections = c(AR = "AZ")
  ),
  opioid = list(
    file_prefix = "Opioid", measure = "ems_rate", geo_type = "county",
    mid_prefix = "COUNTY ", max_value = NA, time = "2025-07-31"
  )
)

# -----------------------------------------------------------------------------
# 2. Change detection on raw files
# -----------------------------------------------------------------------------
raw_files_all <- list.files("raw", pattern = "^Results_.*\\.csv$", full.names = TRUE)
if (length(raw_files_all) == 0) {
  stop("No raw/Results_<Topic>_<date>_<AREA>.csv files found -- place fresh exports in raw/ before running.")
}
raw_state <- as.list(tools::md5sum(raw_files_all))
names(raw_state) <- basename(raw_files_all)

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
  # 4. Helpers
  # ---------------------------------------------------------------------------
  parse_one <- function(path, cfg) {
    fname <- basename(path)
    m <- str_match(
      fname,
      paste0("^Results_", cfg$file_prefix, "[-_]([0-9]{4}-[0-9]{2}-[0-9]{2})_([A-Za-z]+)\\.csv$")
    )
    if (is.na(m[1, 1])) stop("Unrecognized raw filename pattern: ", fname)
    area <- m[1, 3]
    if (area %in% names(cfg$area_corrections)) area <- cfg$area_corrections[[area]]

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
      time = as.Date(cfg$time)
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
        geography = ifelse(mid == "TOTAL", total_geo, sub(paste0("^", cfg$mid_prefix), "", mid)),
        geography_type = ifelse(mid == "TOTAL", total_type, cfg$geo_type)
      )
  }

  validate <- function(d, out_file, max_value) {
    dup <- d %>% count(geography, geography_type, time) %>% filter(n > 1)
    if (nrow(dup) > 0) stop("Duplicate (geography, geography_type, time) rows found in ", out_file)
    if (any(is.na(d$geography) | is.na(d$time))) stop("Missing geography or time in ", out_file)
    pop_col <- grep("_population$", names(d), value = TRUE)
    if (length(pop_col) && any(d[[pop_col]] < 0, na.rm = TRUE)) {
      stop("Negative population value in ", out_file)
    }
    low_col <- grep("_low$", names(d), value = TRUE)
    high_col <- grep("_high$", names(d), value = TRUE)
    if (any(d[[low_col]] < 0, na.rm = TRUE) || any(d[[high_col]] < 0, na.rm = TRUE)) {
      stop("Negative value bound in ", out_file)
    }
    if (any(d[[low_col]] > d[[high_col]], na.rm = TRUE)) {
      stop("Low bound exceeds high bound in at least one row in ", out_file)
    }
    if (!is.na(max_value) && any(d[[high_col]] > max_value, na.rm = TRUE)) {
      stop("Value bound exceeds expected max (", max_value, ") in ", out_file)
    }
  }

  # ---------------------------------------------------------------------------
  # 5. Process each topic and write its own standard file
  # ---------------------------------------------------------------------------
  for (topic in names(TOPICS)) {
    cfg <- TOPICS[[topic]]
    files <- list.files(
      "raw",
      pattern = paste0("^Results_", cfg$file_prefix, "[-_][0-9]{4}-[0-9]{2}-[0-9]{2}_.*\\.csv$"),
      full.names = TRUE
    )
    if (length(files) == 0) {
      stop("No raw files found for topic '", topic, "' (Results_", cfg$file_prefix, "_<date>_<AREA>.csv)")
    }
    # National file first so its rows win any tie when the same county/ZCTA3
    # also appears in a per-area file (see distinct() below).
    files <- files[order(!grepl("_National\\.csv$", basename(files)))]

    rows <- bind_rows(lapply(files, parse_one, cfg = cfg)) %>%
      distinct(geography, geography_type, time, .keep_all = TRUE)

    data_standard <- rows %>%
      select(geography, geography_type, time, population, value_low, value_high, value_label) %>%
      rename(
        !!paste0(topic, "_population") := population,
        !!paste0(topic, "_", cfg$measure, "_low") := value_low,
        !!paste0(topic, "_", cfg$measure, "_high") := value_high,
        !!paste0(topic, "_", cfg$measure, "_label") := value_label
      )

    out_file <- paste0("standard/data_", topic, ".csv.gz")
    validate(data_standard, out_file, cfg$max_value)
    vroom::vroom_write(data_standard, out_file, delim = ",")
  }

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
