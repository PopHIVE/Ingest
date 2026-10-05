# =============================================================================
# VSD: Weekly Influenza and RSV Vaccination Coverage among Pregnant Women 18-49
# Sources (CDC FluVaxView / RSVVaxView, Vaccine Safety Datalink):
#   8fbp-accd  influenza, by race and ethnicity, 2019-20 to present
#   uqxy-gepz  RSV, by race and ethnicity, 2025-26
#
# Electronic health record data from the integrated health systems that
# participate in the Vaccine Safety Datalink (10 sites). National only.
# Flu: share of women pregnant during the season (August-March) who received a
# flu vaccine since July 1. RSV: share of women reaching 32 weeks' gestation
# since September 1 who received an RSV vaccine during pregnancy. Cumulative
# coverage can fall from one week to the next as newly identified pregnancies
# enter the denominator.
#
# CDC has issued a new dataset ID for each RSV season; check for one each fall.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

DATASETS <- c(flu = "8fbp-accd", rsv = "uqxy-gepz")
new_state <- lapply(DATASETS, function(id) dcf::dcf_download_cdc(id, "raw", process$raw_state[[id]]))
names(new_state) <- DATASETS

read_raw <- function(id, needed) {
  d <- vroom::vroom(
    sprintf("raw/%s.csv.xz", id), delim = ",",
    col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
  )
  absent <- setdiff(needed, names(d))
  if (length(absent) > 0) stop(id, " columns not found: ", paste(absent, collapse = ", "))
  d
}
# CDC date columns arrive as "YYYY-MM-DD ...", "MM/DD/YYYY ...", or
# "YYYY Mon DD ...", depending on how the file was exported
parse_cdc_date <- function(x) {
  out <- as.Date(rep(NA_character_, length(x)))
  for (fmt in c("%Y-%m-%d", "%m/%d/%Y", "%Y %b %d")) {
    i <- is.na(out) & !is.na(x)
    out[i] <- as.Date(x[i], format = fmt)
  }
  bad <- is.na(out) & !is.na(x)
  if (any(bad)) stop("unparsed dates: ", paste(head(unique(x[bad])), collapse = ", "))
  out
}

# CDC publishes each RSV season as a new dataset ID. From November to March the
# newest RSV date should fall in the current season; if it does not, the ID
# list above needs the new season's dataset. message() rather than warning()
# so the text lands in the dcf process log.
check_rsv_current <- function(dates, ids, today = Sys.Date()) {
  m <- as.integer(format(today, "%m")); y <- as.integer(format(today, "%Y"))
  season_start <- as.Date(sprintf("%d-10-01", if (m >= 7) y else y - 1))
  latest <- suppressWarnings(max(dates, na.rm = TRUE))
  if ((m >= 11 || m <= 3) && is.finite(latest) && latest < season_start) {
    message("Warning: latest RSV data is ", latest, "; CDC has probably published ",
            "the current season under a new dataset ID (current: ",
            paste(ids, collapse = ", "), ")")
  }
}

if (!identical(process$raw_state, new_state)) {

  norm_season <- function(x) sub("^(\\d{4})-(\\d{2})?(\\d{2})$", "\\1-\\3", x)
  season_from_date <- function(d) {
    y <- as.integer(format(d, "%Y")); m <- as.integer(format(d, "%m"))
    y1 <- ifelse(m >= 7, y, y - 1)
    sprintf("%d-%02d", y1, (y1 + 1) %% 100)
  }
  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }

  flu <- read_raw("8fbp-accd", c("Week_Ending_Date", "Race and Ethnicity", "Percent", "Flu Season", "Denominator")) %>%
    transmute(
      vaccine = "flu", time = parse_cdc_date(Week_Ending_Date),
      season = norm_season(`Flu Season`), race_ethnicity = `Race and Ethnicity`,
      vsd_coverage = as.numeric(Percent), vsd_denominator = as.numeric(gsub(",", "", Denominator))
    )
  rsv <- read_raw("uqxy-gepz", c("Week_Ending_Date", "Race and Ethnicity", "Percent", "Denominator")) %>%
    transmute(
      vaccine = "rsv", time = parse_cdc_date(Week_Ending_Date),
      season = season_from_date(time), race_ethnicity = `Race and Ethnicity`,
      vsd_coverage = as.numeric(Percent), vsd_denominator = as.numeric(gsub(",", "", Denominator))
    )

  data <- bind_rows(flu, rsv)
  if (any(format(data$time, "%u") != "6")) stop("week ending dates are not all Saturdays")
  data <- data %>%
    mutate(geography = "00", time = format(time, "%Y-%m-%d")) %>%
    select(geography, time, season, vaccine, race_ethnicity, vsd_coverage, vsd_denominator) %>%
    arrange(vaccine, race_ethnicity, time) %>%
    check_unique(c("geography", "time", "vaccine", "race_ethnicity"), "data")

  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- new_state
  dcf::dcf_process_record(updated = process)
}

rsv_raw <- read_raw(DATASETS[["rsv"]], "Week_Ending_Date")
check_rsv_current(parse_cdc_date(rsv_raw$Week_Ending_Date), DATASETS[["rsv"]])
