# =============================================================================
# Medicare FFS: Weekly Cumulative Influenza and RSV Vaccination Coverage
# Sources (CDC FluVaxView / RSVVaxView, CMS Chronic Conditions Warehouse):
#   agz7-4mvg  influenza, Medicare fee-for-service beneficiaries 65+, by race
#              and ethnicity, 2019-20 to present (seasonal: doses since Aug 1)
#   msnx-y6hi  RSV, Medicare fee-for-service beneficiaries 75+ enrolled in
#              Part D, by race and ethnicity (cumulative ever-vaccinated since
#              Aug 1, 2023; only the current season's weeks are published)
#
# Administrative claims; weekly Kaplan-Meier estimates among beneficiaries
# enrolled as of August 1. National only. "Overall" includes people with
# unknown race and ethnicity. CDC issues a new dataset ID each season for the
# RSV series and has done so for flu in the past; check for new IDs each fall.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

DATASETS <- c(flu = "agz7-4mvg", rsv = "msnx-y6hi")
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
parse_cdc_date <- function(x) {
  x <- substr(x, 1, 10)
  out <- as.Date(x, format = "%Y-%m-%d")
  i <- is.na(out)
  out[i] <- as.Date(x[i], format = "%m/%d/%Y")
  if (any(is.na(out) & !is.na(x))) stop("unparsed dates")
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
  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }

  flu <- read_raw("agz7-4mvg", c("Week Ending", "Influenza_Season", "Estimate", "Race and ethnicity")) %>%
    transmute(
      vaccine = "flu", age = "65+", time = parse_cdc_date(`Week Ending`),
      season = norm_season(Influenza_Season), race_ethnicity = `Race and ethnicity`,
      medicare_coverage = as.numeric(Estimate)
    )
  # RSV coverage is cumulative since 2023-08-01, so it is not tied to a season
  rsv <- read_raw("msnx-y6hi", c("Week Ending", "Estimate", "Race and ethnicity")) %>%
    transmute(
      vaccine = "rsv", age = "75+", time = parse_cdc_date(`Week Ending`),
      season = NA_character_, race_ethnicity = `Race and ethnicity`,
      medicare_coverage = as.numeric(Estimate)
    )

  data <- bind_rows(flu, rsv)
  if (any(format(data$time, "%u") != "6")) stop("week ending dates are not all Saturdays")
  data <- data %>%
    mutate(geography = "00", time = format(time, "%Y-%m-%d")) %>%
    select(geography, time, season, vaccine, age, race_ethnicity, medicare_coverage) %>%
    arrange(vaccine, race_ethnicity, time) %>%
    check_unique(c("geography", "time", "vaccine", "age", "race_ethnicity"), "data")

  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- new_state
  dcf::dcf_process_record(updated = process)
}

rsv_raw <- read_raw(DATASETS[["rsv"]], "Week Ending")
check_rsv_current(parse_cdc_date(rsv_raw[["Week Ending"]]), DATASETS[["rsv"]])
