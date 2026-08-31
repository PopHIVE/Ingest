# =============================================================================
# Delphi Hospital Claims (inpatient) Data Ingestion
# Source: CMU Delphi cast API (v5), claims_inpatient
#   https://delphi.cmu.edu/epidata/v5/
# =============================================================================

library(epidatr) # requires >= 1.3.0 for the cast (v5) endpoints
library(tidyverse)

process <- dcf::dcf_process_record()

# claims_inpatient daily values are already moving averages: the national series
# has no weekday profile (2024+ means are 0.153-0.156 across all seven days) and
# a lag-1 autocorrelation of 0.999, so the week-ending Saturday value is already
# a smoothed weekly figure and is taken as-is rather than re-averaged.
select_signals <- c(
  delphi_hospital_covid_smooth = "claims_inpatient_adm_pct_claims_covid",
  delphi_hospital_flu_smooth = "claims_inpatient_adm_pct_claims_flu"
)

end.date <- lubridate::floor_date(Sys.Date(), 'week') - 1 #most recent saturday

# Recorded for provenance only. This is NOT a usable change signal: on
# 2026-08-19 the metadata reported a latest report_time of 2026-08-14 while the
# snapshot served 26,729 rows stamped as late as 2026-08-17, so gating on it
# silently skips backfills. The pull itself is the only reliable signal.
delphi_maxdate <- epidatr::epidata_meta(
  source = "claims_inpatient"
)$claims_inpatient$report_time_range$latest

# epidatr joins multiple signals into one comma-separated parameter, which the
# cast API matches to nothing, so each signal/geography is requested on its own
all <- tidyr::expand_grid(
  signal = unname(select_signals),
  geo_type = c("nation", "state", "county")
) %>%
  purrr::pmap(function(signal, geo_type) {
    epidatr::epidata_snapshot(
      source = "claims_inpatient",
      signals = signal,
      geo_type = geo_type
    )
  }) %>%
  bind_rows() %>%
  # the API returns counties in a different order on every call, which changes
  # the compressed bytes and makes the file look modified when it is not
  arrange(signal, geo_type, geo_value, reference_time) %>%
  vroom::vroom_write(., "raw/data.csv.xz", ",")


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
      './raw/data.csv.xz',
      col_types = vroom::cols(geo_value = "c"),
      show_col_types = FALSE
    ) %>%
    mutate(state = toupper(geo_value)) %>%
    left_join(state_fips_lookup, by = "state") %>%
    mutate(
      geography = if_else(geo_type == "county", geo_value, geography),
      time = reference_time
    ) %>%
    # the already-smoothed value reported on each week-ending Saturday
    filter(lubridate::wday(time, week_start = 7) == 7, time <= end.date) %>%
    select(geography, time, signal, value) %>%
    pivot_wider(
      names_from = signal,
      values_from = value,
      id_cols = c(geography, time)
    ) %>%
    rename(!!!select_signals) %>%
    arrange(time, geography)


  vroom::vroom_write(data, "standard/data.csv.gz", ",")

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
