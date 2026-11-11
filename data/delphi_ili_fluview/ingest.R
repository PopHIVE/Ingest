# =============================================================================
# Delphi FluView ILI Data Ingestion
# Source: CMU Delphi cast API (v5), fluview_ilinet
#   https://delphi.cmu.edu/epidata/v5/
# =============================================================================

library(epidatr) # requires >= 1.4.0: the cast (v5) API rejects older clients
library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# CDC ILINet outpatient influenza-like illness, week ending Saturday
select_signals <- c(
  delphi_fluview_wili = "wili",
  delphi_fluview_ili = "ili",
  delphi_fluview_num_ili = "num_ili",
  delphi_fluview_num_patients = "num_patients",
  delphi_fluview_num_providers = "num_providers"
)

# Before 2026-11 this used pub_fluview(), Delphi's V4 endpoint, which Delphi
# deprecated in October 2026 ("new ingestion will end"). The latest update time
# is recorded for provenance only; the pull is small, so it runs every time and
# the md5 of the raw file decides whether to reprocess (as in delphi_nhsn).
# epidatr >= 1.4.0 returns the metadata without a source-name wrapper; older
# versions nested it under the source name, so accept both
delphi_meta <- epidatr::epidata_meta(source = "fluview_ilinet")
delphi_maxdate <- delphi_meta$latest_update
if (is.null(delphi_maxdate)) delphi_maxdate <- delphi_meta$fluview_ilinet$latest_update
if (is.null(delphi_maxdate)) {
  warning("could not read latest_update from the fluview_ilinet metadata")
}

# epidatr joins multiple signals into one comma-separated parameter, which the
# cast API matches to nothing, so each signal/geography is requested on its own.
# wili is published for the nation only (CDC weights by state population only
# when combining states), so it is not requested by state.
requests <- tidyr::expand_grid(
  signal = unname(select_signals),
  geo_type = c("nation", "state")
) %>%
  filter(!(signal == "wili" & geo_type == "state"))

all <- purrr::pmap(requests, function(signal, geo_type) {
  epidatr::epidata_snapshot(
    source = "fluview_ilinet",
    signals = signal,
    geo_type = geo_type
  )
}) %>%
  bind_rows() %>%
  select(signal, geo_type, geo_value, age_group, reference_time, fill_method, report_time, value) %>%
  # rows come back in a different order on each call; sort so an unchanged
  # pull writes identical bytes
  arrange(signal, geo_type, geo_value, age_group, reference_time, fill_method, report_time)

if (nrow(all) == 0) stop("the fluview_ilinet snapshot returned no rows")

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

  # covers 'us' -> "00", the 50 states + DC, Puerto Rico and the Virgin Islands
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    mutate(state = tolower(state)) %>%
    select(geography, state)

  data <- vroom::vroom(
    "raw/data.csv.xz",
    col_types = vroom::cols(geo_value = "c", fill_method = "c", age_group = "c"),
    show_col_types = FALSE
  ) %>%
    # age groups are published for national ILI counts only; keep all ages.
    # New York is published as "ny" (New York City plus the rest of the state,
    # as in the old FluView feed) and as its two parts; keep the state total.
    filter(age_group == "all", !geo_value %in% c("nyc", "ny_minus_nyc")) %>%
    left_join(state_fips_lookup, by = c("geo_value" = "state"))

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
    select(geography, time, signal, value) %>%
    pivot_wider(names_from = signal, values_from = value) %>%
    mutate(
      # CDC does not population-weight a single state, so a state's weighted
      # ILI is its unweighted ILI (the old FluView feed published them as
      # identical values); keep the column filled so bundles keep state data
      delphi_fluview_wili = if_else(geography == "00", delphi_fluview_wili, delphi_fluview_ili),
      # a week with no patients seen has no ILI percentage (0/0), not 0%
      no_patients = !is.na(delphi_fluview_num_patients) & delphi_fluview_num_patients == 0,
      delphi_fluview_ili = if_else(no_patients, NA_real_, delphi_fluview_ili),
      delphi_fluview_wili = if_else(no_patients, NA_real_, delphi_fluview_wili)
    ) %>%
    select(geography, time, all_of(names(select_signals))) %>%
    arrange(geography, time)

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
