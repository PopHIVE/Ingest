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

if (!identical(process$raw_state, new_state)) {

  read_raw <- function(id, needed) {
    d <- vroom::vroom(
      sprintf("raw/%s.csv.xz", id), delim = ",",
      col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
    )
    absent <- setdiff(needed, names(d))
    if (length(absent) > 0) stop(id, " columns not found: ", paste(absent, collapse = ", "))
    d
  }
  parse_cdc_date <- function(x) {
    x <- substr(x, 1, 10)
    out <- as.Date(x, format = "%Y-%m-%d")
    i <- is.na(out)
    out[i] <- as.Date(x[i], format = "%m/%d/%Y")
    if (any(is.na(out) & !is.na(x))) stop("unparsed dates")
    out
  }
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
