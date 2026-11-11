# =============================================================================
# Delphi Sleep Cycle Nocturnal Cough Data Ingestion
# Source: CMU Delphi cast API (v5), sleepcycle
#   https://cmu-delphi.github.io/delphi-epidata/api/v5-signals/sleepcycle.html
# Upstream: Sleep Cycle sleep-tracking app (de-identified, differentially
#   private nocturnal cough detections)
# License: CC BY-NC 4.0 (non-commercial use only)
# =============================================================================

library(epidatr) # requires >= 1.4.0: the cast (v5) API rejects older clients
library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# Both signals are daily 7-day trailing averages (reference_time = last day of
# the window). They are kept as weekly series: the value on each week-ending
# Saturday is the average over that Sunday-Saturday MMWR week. "_7dav" is
# dropped from the column names for that reason.
select_signals <- c(
  delphi_sleepcycle_pct_cough_2_plus = "pct_cough_2_plus_7dav",
  delphi_sleepcycle_pct_cough_10_plus = "pct_cough_10_plus_7dav"
)

# Recorded for provenance only; the md5 of the raw file decides whether to
# reprocess (as in the other delphi_* sources).
# epidatr >= 1.4.0 returns the metadata without a source-name wrapper; older
# versions nested it under the source name, so accept both
delphi_meta <- epidatr::epidata_meta(source = "sleepcycle")
delphi_maxdate <- delphi_meta$latest_update
if (is.null(delphi_maxdate)) delphi_maxdate <- delphi_meta$sleepcycle$latest_update
if (is.null(delphi_maxdate)) {
  warning("could not read latest_update from the sleepcycle metadata")
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

# epidatr >= 1.4.0 sends several signals in one request; one request per
# geography level. Delphi builds state and nation by summing daily county
# counts, so the three levels are consistent.
all <- c("nation", "state", "county") %>%
  lapply(function(geo_type) {
    snapshot_with_retry(
      source = "sleepcycle", signals = unname(select_signals), geo_type = geo_type
    )
  }) %>%
  bind_rows() %>%
  # Only week-ending Saturdays are used, so only they are stored: the full
  # daily county series is ~17 MB compressed (vs ~2.5 MB) and changes every
  # day, which would rewrite the raw file on every scheduled run
  filter(lubridate::wday(reference_time, week_start = 7) == 7) %>%
  select(signal, geo_type, geo_value, reference_time, fill_method, report_time,
         value, ci_lower, ci_upper) %>%
  # rows come back in a different order on each call; sort so an unchanged
  # pull writes identical bytes
  arrange(signal, geo_type, geo_value, reference_time, fill_method, report_time)

if (nrow(all) == 0) stop("the sleepcycle snapshot returned no rows")

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
  county_fips <- all_fips$geography[nchar(all_fips$geography) == 5]

  data <- vroom::vroom(
    "raw/data.csv.xz",
    col_types = vroom::cols(geo_value = "c", fill_method = "c"),
    show_col_types = FALSE
  ) %>%
    mutate(state = if_else(geo_type == "county", NA_character_, toupper(geo_value))) %>%
    left_join(state_fips_lookup, by = "state") %>%
    # county geo_values are already 5-digit FIPS codes
    mutate(geography = if_else(
      geo_type == "county" & geo_value %in% county_fips,
      geo_value,
      geography
    ))

  unmatched <- unique(data$geo_value[is.na(data$geography)])
  if (length(unmatched) > 0) {
    stop("geo_value(s) with no FIPS code: ", paste(unmatched, collapse = ", "))
  }

  # one value per signal/geography/day ("source" is the only fill_method);
  # anything else needs an explicit rule
  dups <- data %>% count(signal, geography, reference_time) %>% filter(n > 1)
  if (nrow(dups) > 0) {
    stop(
      nrow(dups), " signal/geography/day rows have more than one value (fill_method: ",
      paste(unique(data$fill_method), collapse = ", "), ")"
    )
  }

  # values and 90% confidence intervals are already percents (0-100)
  wide <- data %>%
    mutate(signal = names(select_signals)[match(signal, select_signals)]) %>%
    select(geography, time = reference_time, signal, value,
           lcl = ci_lower, ucl = ci_upper) %>%
    pivot_wider(names_from = signal, values_from = c(value, lcl, ucl),
                names_glue = "{signal}__{.value}")
  names(wide) <- sub("__value$", "", names(wide))
  names(wide) <- sub("__(lcl|ucl)$", "_\\1", names(wide))

  # every geography gets every Saturday from 2023-01-07 to the latest week, so
  # weeks lost to app outages or to Delphi's sparsity suppression are NA rows
  # rather than absent rows. Locations Delphi never reports are not added.
  weeks <- seq(min(wide$time), max(wide$time), by = "week")
  wide <- wide %>% complete(geography, time = weeks)

  out_cols <- as.vector(t(outer(names(select_signals), c("", "_lcl", "_ucl"), paste0)))

  # a published column with no values at all means the pull changed shape
  empty_cols <- out_cols[
    vapply(out_cols, function(s) !s %in% names(wide) || all(is.na(wide[[s]])), logical(1))
  ]
  if (length(empty_cols) > 0) {
    stop("no values in standard output for: ", paste(empty_cols, collapse = ", "))
  }

  data_main <- wide %>%
    select(geography, time, all_of(out_cols)) %>%
    mutate(time = format(time, "%Y-%m-%d")) %>%
    arrange(time, geography)

  vroom::vroom_write(data_main, "standard/data.csv.gz", ",")

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
