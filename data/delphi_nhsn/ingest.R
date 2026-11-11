# =============================================================================
# Delphi NHSN Hospital Respiratory Data Ingestion
# Source: CMU Delphi cast API (v5), nhsn
#   https://delphi.cmu.edu/epidata/v5/
# =============================================================================

library(epidatr) # requires >= 1.4.0: the cast (v5) API rejects older clients
library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# Weekly confirmed hospital admissions (week ending Saturday)
select_signals <- c(
  delphi_nhsn_covid = "confirmed_admissions_covid_ew",
  delphi_nhsn_flu = "confirmed_admissions_flu_ew",
  delphi_nhsn_rsv = "confirmed_admissions_rsv_ew"
)

# Before 2026-09 this used pub_covidcast(source = "nhsn"), but the legacy
# covidcast endpoint stopped updating on 2026-09-18 (last week 2026-09-12)
# when Delphi moved NHSN to the cast API. Its latest update time is recorded
# for provenance only; the pull is small, so it runs every time and the md5 of
# the raw file decides whether to reprocess (as in delphi_*_claims).
# epidatr >= 1.4.0 returns the metadata without a source-name wrapper; older
# versions nested it under the source name, so accept both
delphi_meta <- epidatr::epidata_meta(source = "nhsn")
delphi_maxdate <- delphi_meta$latest_update
if (is.null(delphi_maxdate)) delphi_maxdate <- delphi_meta$nhsn$latest_update
if (is.null(delphi_maxdate)) {
  warning("could not read latest_update from the nhsn metadata")
}

# epidatr joins multiple signals into one comma-separated parameter, which the
# cast API matches to nothing, so each signal/geography is requested on its own.
# The snapshot holds the latest value of every week back to 2020-08-08.
all <- tidyr::expand_grid(
  signal = unname(select_signals),
  geo_type = c("nation", "state")
) %>%
  purrr::pmap(function(signal, geo_type) {
    epidatr::epidata_snapshot(
      source = "nhsn",
      signals = signal,
      geo_type = geo_type
    )
  }) %>%
  bind_rows() %>%
  select(signal, geo_type, geo_value, reference_time, fill_method, report_time, value) %>%
  # rows come back in a different order on each call; sort so an unchanged
  # pull writes identical bytes
  arrange(signal, geo_type, geo_value, reference_time, fill_method, report_time)

if (nrow(all) == 0) stop("the nhsn snapshot returned no rows")

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

  # covers 'us' -> "00", the 50 states + DC, and the 5 territories
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state)

  data <- vroom::vroom(
    "raw/data.csv.xz",
    col_types = vroom::cols(geo_value = "c", fill_method = "c"),
    show_col_types = FALSE
  ) %>%
    mutate(state = toupper(geo_value)) %>%
    left_join(state_fips_lookup, by = "state")

  unmatched <- unique(data$geo_value[is.na(data$geography)])
  if (length(unmatched) > 0) {
    stop("geo_value(s) with no FIPS code: ", paste(unmatched, collapse = ", "))
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

  data_main <- data %>%
    mutate(
      signal = names(select_signals)[match(signal, select_signals)],
      # weeks end on Saturday; shift any Sunday-start date to its Saturday
      time = if_else(
        lubridate::wday(reference_time, week_start = 7) == 1,
        reference_time + 6,
        reference_time
      )
    ) %>%
    # flu and RSV reporting was voluntary before the 2024-11-01 mandate, so
    # those earlier weeks are incomplete and are dropped (COVID is kept)
    filter(signal == "delphi_nhsn_covid" | time >= as.Date("2024-10-31")) %>%
    select(geography, time, signal, value) %>%
    pivot_wider(names_from = signal, values_from = value) %>%
    select(geography, time, all_of(names(select_signals))) %>%
    arrange(time, geography)

  # a published column with no values at all means the pull changed shape
  empty_cols <- names(select_signals)[
    vapply(data_main[names(select_signals)], function(x) all(is.na(x)), logical(1))
  ]
  if (length(empty_cols) > 0) {
    stop("no values in standard output for: ", paste(empty_cols, collapse = ", "))
  }

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
