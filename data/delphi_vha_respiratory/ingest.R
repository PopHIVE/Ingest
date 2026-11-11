# =============================================================================
# Delphi VHA Respiratory Cases and Vaccinations Data Ingestion
# Source: CMU Delphi cast API (v5), vha_respiratory_cases and
#   vha_respiratory_vaccinations
#   https://cmu-delphi.github.io/delphi-epidata/api/v5-signals/vha_respiratory.html
# Upstream: Veterans Health Administration Seasonal Disease Review
#   (VA Access to Care portal)
# =============================================================================

library(epidatr) # requires >= 1.4.0: the cast (v5) API rejects older clients
library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

# Both Delphi sources are daily and begin 2026-05-27. They are kept as weekly
# series: the value on each week-ending Saturday. For the 7-day trailing
# averages that is the average over the Sunday-Saturday MMWR week; for the
# cumulative counts it is the total through the end of that week.
cases_signals <- c(
  "covid_cases_cumulative", "flu_cases_cumulative", "rsv_cases_cumulative",
  "covid_cases_per_100k_7dav", "flu_cases_per_100k_7dav", "rsv_cases_per_100k_7dav"
)
vax_signals <- c(
  "covid_vaccines_season_cumulative", "covid_vaccines_season_cumulative_per_100k",
  "flu_vaccines_season_cumulative", "flu_vaccines_season_cumulative_per_100k",
  "rsv_vaccines_season_cumulative", "rsv_vaccines_season_cumulative_per_100k",
  "rsv_vaccines_lifetime_cumulative", "rsv_vaccines_lifetime_cumulative_per_100k"
)
select_signals <- c(cases_signals, vax_signals)
names(select_signals) <- paste0("delphi_vha_", select_signals)

# Recorded for provenance only; the md5 of the raw file decides whether to
# reprocess (as in the other delphi_* sources).
# epidatr >= 1.4.0 returns the metadata without a source-name wrapper; older
# versions nested it under the source name, so accept both
latest_update <- function(source) {
  meta <- epidatr::epidata_meta(source = source)
  out <- meta$latest_update
  if (is.null(out)) out <- meta[[source]]$latest_update
  if (is.null(out)) warning("could not read latest_update from the ", source, " metadata")
  out
}
delphi_maxdate <- list(
  vha_respiratory_cases = latest_update("vha_respiratory_cases"),
  vha_respiratory_vaccinations = latest_update("vha_respiratory_vaccinations")
)

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
# source and geography level
all <- tidyr::expand_grid(
  source = c("vha_respiratory_cases", "vha_respiratory_vaccinations"),
  geo_type = c("nation", "state")
) %>%
  purrr::pmap(function(source, geo_type) {
    signals <- if (source == "vha_respiratory_cases") cases_signals else vax_signals
    withCallingHandlers(
      snapshot_with_retry(source = source, signals = signals, geo_type = geo_type),
      warning = function(w) {
        # Delphi withholds a national cumulative case total whenever any state
        # is missing that day, so flu/RSV (and, since 2026-10, COVID)
        # cumulative cases often have no national rows at all; that warning is
        # expected here and would otherwise appear in every build log
        if (source == "vha_respiratory_cases" && geo_type == "nation" &&
            grepl("No data returned", conditionMessage(w))) {
          invokeRestart("muffleWarning")
        }
      }
    )
  }) %>%
  bind_rows() %>%
  # Only week-ending Saturdays are used, so only they are stored: the daily
  # series is ~7x larger and changes every day, which would rewrite the raw
  # file on every scheduled run
  filter(lubridate::wday(reference_time, week_start = 7) == 7) %>%
  select(signal, geo_type, geo_value, reference_time, fill_method, report_time,
         value, ci_lower, ci_upper) %>%
  # rows come back in a different order on each call; sort so an unchanged
  # pull writes identical bytes
  arrange(signal, geo_type, geo_value, reference_time, fill_method, report_time)

if (nrow(all) == 0) stop("the vha_respiratory snapshot returned no rows")

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
    mutate(state = toupper(geo_value)) %>%
    left_join(state_fips_lookup, by = "state")

  unmatched <- unique(data$geo_value[is.na(data$geography)])
  if (length(unmatched) > 0) {
    stop("geo_value(s) with no FIPS code: ", paste(unmatched, collapse = ", "))
  }

  # one value per signal/geography/day: each signal comes with a single
  # fill_method per level ("source" = native grain, "ave" = enrollee-weighted
  # roll-up); anything else needs an explicit rule
  dups <- data %>% count(signal, geography, reference_time) %>% filter(n > 1)
  if (nrow(dups) > 0) {
    stop(
      nrow(dups), " signal/geography/day rows have more than one value (fill_method: ",
      paste(unique(data$fill_method), collapse = ", "), ")"
    )
  }

  data <- data %>%
    mutate(signal = names(select_signals)[match(signal, select_signals)]) %>%
    select(geography, time = reference_time, signal, value,
           lcl = ci_lower, ucl = ci_upper)

  # Bad upstream days: on some dates (e.g. 2026-06-29, 2026-07-15) every state
  # reports exactly 5 COVID vaccine doses against cumulative totals in the
  # thousands. That is a broken portal file, not censoring, so when most states
  # report exactly 5 for a vaccine count on a date, that date's count and its
  # per-100k rate are set to missing for every state and the nation
  vax_count_signals <- vax_signals[!grepl("per_100k", vax_signals)]
  bad_days <- data %>%
    filter(signal %in% paste0("delphi_vha_", vax_count_signals), geography != "00") %>%
    group_by(signal, time) %>%
    filter(mean(value == 5) > 0.5) %>%
    distinct(signal, time) %>%
    ungroup()
  if (nrow(bad_days) > 0) {
    message(
      "setting ", nrow(bad_days), " signal-week(s) with a broken upstream file to NA: ",
      paste(bad_days$signal, bad_days$time, collapse = "; ")
    )
    bad_keys <- bind_rows(
      bad_days,
      bad_days %>% mutate(signal = paste0(signal, "_per_100k"))
    ) %>% mutate(bad = TRUE)
    data <- data %>%
      left_join(bad_keys, by = c("signal", "time")) %>%
      mutate(across(c(value, lcl, ucl), ~ if_else(coalesce(bad, FALSE), NA_real_, .x))) %>%
      select(-bad)
  }

  # VHA censors counts under 11 upstream and reports them as 5. State is the
  # native grain of the vaccination counts, so a state value of exactly 5 is
  # a censored count, and the matching per-100k rate is computed from it. The
  # national totals sum censored state values, so the flag is NA there.
  # (Case counts are summed from facilities, so a censored facility cannot be
  # identified from the state total and case counts are not flagged.)
  vax_count_cols <- names(select_signals)[
    select_signals %in% vax_signals & !grepl("per_100k", select_signals)
  ]
  flags <- data %>%
    filter(signal %in% vax_count_cols) %>%
    mutate(
      flag = case_when(
        geography == "00" | is.na(value) ~ NA_integer_,
        value == 5 ~ 1L,
        TRUE ~ 0L
      ),
      signal = paste0(signal, "_suppressed_flag")
    ) %>%
    select(geography, time, signal, flag) %>%
    pivot_wider(names_from = signal, values_from = flag)

  # rates come with 90% Wilson confidence intervals; counts have none
  rate_cols <- names(select_signals)[grepl("per_100k", select_signals)]
  wide <- data %>%
    pivot_wider(names_from = signal, values_from = c(value, lcl, ucl),
                names_glue = "{signal}__{.value}") %>%
    left_join(flags, by = c("geography", "time"))

  # every geography gets every Saturday from the start of the data to the
  # latest week, so missing days are NA rows rather than absent rows
  weeks <- seq(min(wide$time), max(wide$time), by = "week")
  wide <- wide %>% complete(geography, time = weeks)

  out_cols <- unlist(lapply(names(select_signals), function(s) {
    cols <- s
    if (s %in% rate_cols) cols <- c(cols, paste0(s, c("_lcl", "_ucl")))
    if (s %in% vax_count_cols) cols <- c(cols, paste0(s, "_suppressed_flag"))
    cols
  }))
  names(wide) <- sub("__value$", "", names(wide))
  names(wide) <- sub("__(lcl|ucl)$", "_\\1", names(wide))

  # a published column with no values at all means the pull changed shape
  missing_cols <- setdiff(out_cols, names(wide))
  if (length(missing_cols) > 0) {
    stop("columns missing from standard output: ", paste(missing_cols, collapse = ", "))
  }
  empty_cols <- names(select_signals)[
    vapply(wide[names(select_signals)], function(x) all(is.na(x)), logical(1))
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
