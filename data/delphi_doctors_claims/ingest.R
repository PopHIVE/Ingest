# =============================================================================
# Delphi Doctor Visits (outpatient claims) Data Ingestion
# Source: CMU Delphi cast API (v5), claims_outpatient
#   https://delphi.cmu.edu/epidata/v5/
# =============================================================================

library(epidatr) # requires >= 1.3.0 for the cast (v5) endpoints
library(tidyverse)

process <- dcf::dcf_process_record()

# claims_outpatient daily values are already moving averages: the national series
# has no weekday profile (means are ~0.20-0.205 across all seven days) and a
# lag-1 autocorrelation of 0.998, so the week-ending Saturday value is already
# a smoothed weekly figure and is taken as-is rather than re-averaged.
select_signals <- c(
  delphi_doc_covid_smooth = "claims_outpatient_ov_pct_claims_covid"
)

end.date <- lubridate::floor_date(Sys.Date(), 'week') - 1 #most recent saturday

# Recorded for provenance only. This is NOT a usable change signal: on
# 2026-08-19 the metadata reported a latest report_time of 2026-08-14 while the
# snapshot served 26,729 rows stamped as late as 2026-08-17, so gating on it
# silently skips backfills. The pull itself is the only reliable signal.
# epidatr >= 1.4.0 returns the metadata without a source-name wrapper; older
# versions nested it under the source name, so accept both
delphi_meta <- epidatr::epidata_meta(source = "claims_outpatient")
delphi_maxdate <- delphi_meta$report_time_range$latest
if (is.null(delphi_maxdate)) {
  delphi_maxdate <- delphi_meta$claims_outpatient$report_time_range$latest
}
if (is.null(delphi_maxdate)) {
  warning("could not read the latest report_time from the claims_outpatient metadata")
}

# epidatr joins multiple signals into one comma-separated parameter, which the
# cast API matches to nothing, so each signal/geography is requested on its own
all <- tidyr::expand_grid(
  signal = unname(select_signals),
  geo_type = c("nation", "state", "county")
) %>%
  purrr::pmap(function(signal, geo_type) {
    epidatr::epidata_snapshot(
      source = "claims_outpatient",
      signals = signal,
      geo_type = geo_type
    )
  }) %>%
  bind_rows() %>%
  # the API returns counties in a different order on every call, which changes
  # the compressed bytes and makes the file look modified when it is not;
  # fill_method and report_time break ties between the variants of one value
  arrange(signal, geo_type, geo_value, reference_time, fill_method, report_time)

# One raw file per reference year. A single file passed GitHub's 50 MB limit,
# and the whole of it was rewritten on every update; with sorted, per-year
# files only the years that actually changed produce a new blob. No rows are
# dropped. Clear the old files first so a retired layout can't be read twice.
unlink(list.files("raw", "^data(_[0-9]{4})?\\.csv\\.xz$", full.names = TRUE))
all %>%
  split(format(.$reference_time, "%Y")) %>%
  purrr::iwalk(function(d, year) {
    vroom::vroom_write(d, sprintf("raw/data_%s.csv.xz", year), ",")
  })


# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv.xz",
  recursive = TRUE,
  full.names = TRUE
)))

#process raw if state has changed
if (!identical(process$raw_state, raw_state)) {

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

# covers 'us' -> "00" as well as the 50 states + DC
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, state)

  data <- vroom::vroom(
      list.files("raw", "^data_[0-9]{4}\\.csv\\.xz$", full.names = TRUE),
      col_types = vroom::cols(geo_value = "c", fill_method = "c"),
      show_col_types = FALSE
    ) %>%
    mutate(state = toupper(geo_value)) %>%
    left_join(state_fips_lookup, by = "state") %>%
    mutate(
      geography = if_else(geo_type == "county", geo_value, geography),
      time = reference_time
    ) %>%
    # claims_outpatient county rows occasionally carry the literal string "NA"
    # as their geo_value (read as a missing geo_value/geography here), which
    # is not a real FIPS code and must be dropped rather than kept as a row
    filter(!is.na(geography)) %>%
    # the already-smoothed value reported on each week-ending Saturday
    filter(lubridate::wday(time, week_start = 7) == 7, time <= end.date) %>%
    # keep the Saturday report of each variant: the pull now returns the old
    # unlabeled series (fill_method NA, published through 2026-09-30) alongside
    # labeled variants ("source" = no imputation, "zero" = nulls filled with
    # zero). Keeping them apart, and only the latest report_time within each,
    # leaves one value per geography/week/variant.
    select(geography, time, signal, fill_method, report_time, value) %>%
    mutate(
      fill_method = coalesce(as.character(fill_method), "unlabeled"),
      signal = names(select_signals)[match(signal, select_signals)]
    ) %>%
    group_by(geography, time, signal, fill_method) %>%
    slice_max(report_time, n = 1, with_ties = FALSE) %>%
    ungroup()

  labeled <- data %>% filter(fill_method != "unlabeled")
  unlabeled <- data %>% filter(fill_method == "unlabeled")

  # a geography/week with more than one labeled variant would need an explicit
  # rule for which to show; fail loudly instead of guessing or dropping one
  ambiguous <- labeled %>% count(geography, time, signal) %>% filter(n > 1)
  if (nrow(ambiguous) > 0) {
    stop(
      nrow(ambiguous), " geography/week/signal rows have more than one labeled ",
      "fill_method variant (", paste(unique(labeled$fill_method), collapse = ", "),
      "); decide which variant to publish before processing"
    )
  }

  # value = the labeled variant when there is one, else the unlabeled value, so
  # no week loses its value; the fill_method flag records which one was used
  combined <- full_join(
    labeled %>% select(geography, time, signal, fill_method, value_labeled = value),
    unlabeled %>% select(geography, time, signal, value_unlabeled = value),
    by = c("geography", "time", "signal")
  ) %>%
    mutate(
      value = coalesce(value_labeled, value_unlabeled),
      fill_method = coalesce(fill_method, "unlabeled")
    )

  stopifnot(!anyDuplicated(combined[c("geography", "time", "signal")]))

  data_main <- combined %>%
    pivot_wider(
      names_from = signal,
      values_from = value,
      id_cols = c(geography, time)
    ) %>%
    arrange(time, geography)

  # the flag and the pre-labeling value go in their own file so the main file
  # keeps one numeric column per measure (bundle_respiratory melts every column)
  data_fill <- combined %>%
    pivot_wider(
      names_from = signal,
      values_from = c(fill_method, value_unlabeled),
      id_cols = c(geography, time),
      names_glue = "{signal}_{sub('value_', '', .value)}"
    ) %>%
    arrange(time, geography)

  if (any(vapply(data_main, is.list, logical(1)))) {
    stop("list column in standard output: duplicate rows reached pivot_wider")
  }

  # a published column with no values at all means the pull changed shape again
  # (this is how the 2026-10-02 blank output went unnoticed); fail the build
  empty_cols <- names(select_signals)[
    vapply(data_main[names(select_signals)], function(x) all(is.na(x)), logical(1))
  ]
  if (length(empty_cols) > 0) {
    stop("no values in standard output for: ", paste(empty_cols, collapse = ", "))
  }

  vroom::vroom_write(data_main, "standard/data.csv.gz", ",")
  vroom::vroom_write(data_fill, "standard/data_fill_method.csv.gz", ",")

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
