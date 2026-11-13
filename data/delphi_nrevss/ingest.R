# =============================================================================
# Delphi NREVSS Respiratory Virus Laboratory Data Ingestion
# Source: CMU Delphi cast API (v5), nrevss
#   https://cmu-delphi.github.io/delphi-epidata/api/v5-signals/nrevss.html
# Upstream: CDC National Respiratory and Enteric Virus Surveillance System
#   (Socrata dataset rgnm-fkqb)
# =============================================================================

library(epidatr) # requires >= 1.4.0: the cast (v5) API rejects older clients
library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# The national and state series carry different signals, so each level has its
# own signal list and its own standard file:
# - nation and HHS region: weekly CDC test counts, positive detections and
#   percent positivity for all seven pathogens. The HHS-region RSV positives
#   replace the retired NREVSS source (CDC dataset 3cxc-4k8q) in
#   bundle_respiratory/dist/rsv_positive_tests.parquet
# - state: CDC publishes state data only for RSV and SARS-CoV-2, and only as
#   3-week centered averages (tests, percent positive) and 5-week trailing
#   averages (positives), from 2024-02.
# Not kept:
# - the Delphi-computed national 3-week averages (*_positive_tests_3wav),
#   which are derived from the weekly counts kept here
# - Delphi's deconvolved state estimate *_pct_positive_est: on the 2026-11
#   pull, 53% of the COVID values were exactly 0 or 100 and 27 of 52 states
#   swung by 50+ points week to week at least 5 times (vs 58 such swings in
#   total for CDC's 3-week average), so it is not a usable weekly series
pathogens <- c("adv", "covid", "evrv", "hcov", "hmpv", "hpiv", "rsv")
nation_signals <- as.vector(outer(
  pathogens, c("tests", "positive_tests", "pct_positive"),
  paste, sep = "_"
))
names(nation_signals) <- paste0("delphi_nrevss_", nation_signals)

state_signals <- as.vector(outer(
  c("covid", "rsv"),
  c("tests_3wav", "positive_tests_5wav", "pct_positive_3wav"),
  paste, sep = "_"
))
names(state_signals) <- paste0("delphi_nrevss_", state_signals)

# Recorded for provenance only; the md5 of the raw file decides whether to
# reprocess (as in the other delphi_* sources).
# epidatr >= 1.4.0 returns the metadata without a source-name wrapper; older
# versions nested it under the source name, so accept both
delphi_meta <- epidatr::epidata_meta(source = "nrevss")
delphi_maxdate <- delphi_meta$latest_update
if (is.null(delphi_maxdate)) delphi_maxdate <- delphi_meta$nrevss$latest_update
if (is.null(delphi_maxdate)) {
  warning("could not read latest_update from the nrevss metadata")
}

# Without DELPHI_EPIDATA_KEY (e.g. a local run) the API allows 3 requests per
# minute, so wait and retry on HTTP 429 instead of failing
snapshot_with_retry <- function(..., tries = 4) {
  for (i in seq_len(tries)) {
    res <- tryCatch(epidatr::epidata_snapshot(...), error = function(e) e)
    if (!inherits(res, "error")) return(res)
    rate_limited <- inherits(res, "httr2_http_429") ||
      inherits(res$parent, "httr2_http_429")
    if (!rate_limited || i == tries) stop(res)
    message("Delphi API rate limit reached; retrying in ", 30 * i, " seconds")
    Sys.sleep(30 * i)
  }
}

# epidatr >= 1.4.0 sends several signals in one request; the snapshot holds
# the latest value of every week (back to 2019-07-06 nationally)
all <- bind_rows(
  snapshot_with_retry(
    source = "nrevss", signals = unname(nation_signals), geo_type = "nation"
  ),
  snapshot_with_retry(
    source = "nrevss", signals = unname(nation_signals), geo_type = "hhs"
  ),
  snapshot_with_retry(
    source = "nrevss", signals = unname(state_signals), geo_type = "state"
  )
) %>%
  select(signal, geo_type, geo_value, reference_time, fill_method, report_time, value) %>%
  # rows come back in a different order on each call; sort so an unchanged
  # pull writes identical bytes
  arrange(signal, geo_type, geo_value, reference_time, fill_method, report_time)

if (nrow(all) == 0) stop("the nrevss snapshot returned no rows")

vroom::vroom_write(all, "raw/data.csv.xz", ",")

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "^data\\.csv\\.xz$",
  full.names = TRUE
)))

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # covers 'us' -> "00", the 50 states + DC, and the territories
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state)

  data <- vroom::vroom(
    "raw/data.csv.xz",
    col_types = vroom::cols(geo_value = "c", fill_method = "c"),
    show_col_types = FALSE
  ) %>%
    mutate(state = if_else(geo_type == "hhs", NA_character_, toupper(geo_value))) %>%
    left_join(state_fips_lookup, by = "state") %>%
    # HHS regions have no FIPS code; use the repo's hhs_1 ... hhs_10 convention
    mutate(geography = if_else(geo_type == "hhs", paste0("hhs_", geo_value), geography))

  unmatched <- unique(data$geo_value[is.na(data$geography)])
  if (length(unmatched) > 0) {
    stop("geo_value(s) with no FIPS code: ", paste(unmatched, collapse = ", "))
  }

  # NREVSS weeks are MMWR weeks and reference_time is already the Saturday
  # that ends the week; anything else means the API changed its convention
  if (any(lubridate::wday(data$reference_time, week_start = 7) != 7)) {
    stop("reference_time values that are not Saturdays")
  }

  # one value per signal/geography/week; anything else means the API added
  # another fill_method or vintage that needs an explicit rule
  dups <- data %>% count(signal, geography, reference_time) %>% filter(n > 1)
  if (nrow(dups) > 0) {
    stop(
      nrow(dups), " signal/geography/week rows have more than one value (fill_method: ",
      paste(unique(data$fill_method), collapse = ", "), ")"
    )
  }

  # wide table for one level; every geography gets every week from the first
  # to the last week of the level, so missing and suppressed weeks are NA rows
  # rather than absent rows. No values are imputed: NREVSS suppresses weekly
  # positives of 1-4 (and the matching test counts), and those weeks are left
  # NA.
  make_wide <- function(level, signals) {
    d <- data %>%
      filter(geo_type == level, signal %in% signals) %>%
      mutate(signal = names(signals)[match(signal, signals)]) %>%
      select(geography, time = reference_time, signal, value)

    weeks <- seq(min(d$time), max(d$time), by = "week")

    wide <- d %>%
      pivot_wider(names_from = signal, values_from = value) %>%
      complete(geography, time = weeks)

    # a published column with no values at all means the pull changed shape
    empty <- names(signals)[
      vapply(names(signals), function(s) !s %in% names(wide) || all(is.na(wide[[s]])), logical(1))
    ]
    if (length(empty) > 0) {
      stop("no ", level, " values in standard output for: ", paste(empty, collapse = ", "))
    }

    wide %>%
      select(geography, time, all_of(names(signals))) %>%
      mutate(time = format(time, "%Y-%m-%d")) %>%
      arrange(time, geography)
  }

  data_nation <- make_wide("nation", nation_signals)
  data_region <- make_wide("hhs", nation_signals)
  data_state <- make_wide("state", state_signals)

  if (!setequal(unique(data_region$geography), paste0("hhs_", 1:10))) {
    stop("expected HHS regions 1-10, got: ",
         paste(sort(unique(data_region$geography)), collapse = ", "))
  }

  vroom::vroom_write(data_nation, "standard/data_nation.csv.gz", ",")
  vroom::vroom_write(data_region, "standard/data_region.csv.gz", ",")
  vroom::vroom_write(data_state, "standard/data_state.csv.gz", ",")

  # record processed raw state
  process$raw_state <- raw_state
  process$delphi_maxdate <- delphi_maxdate
  dcf::dcf_process_record(updated = process)
}

#to edit API key:
#library("usethis")
#edit_r_environ()
##add
#DELPHI_EPIDATA_KEY="XXXXXXXXXX"
