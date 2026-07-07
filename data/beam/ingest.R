# =============================================================================
# BEAM Dashboard - Report Data
# Source: CDC BEAM (Bacteria, Enterics, Amoeba, and Mycotics) Dashboard, powered
#   by SEDRIC (System for Enteric Disease Response, Investigation, and
#   Coordination). https://data.cdc.gov/d/jbhn-e8xn
# Note: the state-level "Report Data" dataset (jbhn-e8xn) is used here instead
# of "Isolates by HHS Region" (khic-yj26) because it reports at state
# resolution, which maps cleanly to FIPS codes without fabricating precision
# an HHS-region value doesn't have.
# =============================================================================

library(dplyr)

#
# Download
#
process <- dcf::dcf_process_record()

raw_state <- dcf::dcf_download_cdc(
  "jbhn-e8xn",
  "raw",
  process$raw_state
)

#
# Reformat
#
if (!identical(process$raw_state, raw_state)) {

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state)

  # 2021 Census population, used as a single-year reference denominator for
  # rates (same convention as nhtsa_crash/vaccine_exemptions_fattah ingests).
  # Not all reporting jurisdictions have a population figure here (e.g. Guam,
  # US Virgin Islands), so their rate columns come out NA rather than fabricated.
  pop_state <- vroom::vroom(
    "../../resources/census_population_2021.csv.xz",
    show_col_types = FALSE
  ) %>%
    filter(nchar(GEOID) == 2) %>%
    select(geography = GEOID, population = Total)

  pop_national <- pop_state %>%
    summarize(population = sum(population)) %>%
    mutate(geography = "00")

  pop_all <- bind_rows(pop_state, pop_national)

  data_raw <- vroom::vroom("raw/jbhn-e8xn.csv.xz", show_col_types = FALSE)

  # Aggregate over source type/site and serotype: this source tracks overall
  # isolate counts by pathogen, matching the resolution of the BEAM dashboard's
  # headline pathogen trend view. Rows with unrecognized/garbled state codes
  # (a small number of data quality issues in the raw feed) are dropped by the
  # inner_join against the FIPS lookup.
  data_state <- data_raw %>%
    filter(State != "US") %>%
    transmute(
      state = State,
      pathogen = tolower(Pathogen),
      time = format(
        lubridate::ceiling_date(
          as.Date(sprintf("%04d-%02d-01", Year, Month)),
          "month"
        ) - 1,
        "%Y-%m-%d"
      ),
      n_isolates = `Number of isolates`,
      n_outbreak_isolates = `Outbreak associated isolates`
    ) %>%
    inner_join(state_fips_lookup, by = "state") %>%
    group_by(geography, time, pathogen) %>%
    summarize(
      n_isolates = sum(n_isolates, na.rm = TRUE),
      n_outbreak_isolates = sum(n_outbreak_isolates, na.rm = TRUE),
      .groups = "drop"
    )

  # National totals (BEAM does not publish these directly at state resolution)
  data_national <- data_state %>%
    group_by(time, pathogen) %>%
    summarize(
      n_isolates = sum(n_isolates, na.rm = TRUE),
      n_outbreak_isolates = sum(n_outbreak_isolates, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(geography = "00")

  # Jurisdictions with no 2021 Census population figure (e.g. Guam, US Virgin
  # Islands) never get a rate value, even for pathogen/months absent from the
  # raw data and therefore zero-filled below.
  geo_no_pop <- setdiff(
    unique(c(data_state$geography, data_national$geography)),
    pop_all$geography
  )

  data_standard <- bind_rows(data_state, data_national) %>%
    rename(isolates = n_isolates, outbreak_isolates = n_outbreak_isolates) %>%
    left_join(pop_all, by = "geography") %>%
    mutate(isolates_rate = round(isolates / population * 100000, 2)) %>%
    select(-population) %>%
    tidyr::pivot_wider(
      id_cols = c(geography, time),
      names_from = pathogen,
      values_from = c(isolates, outbreak_isolates, isolates_rate),
      names_glue = "beam_{.value}_{pathogen}",
      values_fill = 0
    ) %>%
    mutate(across(
      starts_with("beam_isolates_rate_"),
      ~ if_else(geography %in% geo_no_pop, NA_real_, .x)
    )) %>%
    arrange(geography, time) %>%
    select(geography:beam_isolates_vibrio,
          beam_isolates_rate_campylobacter:beam_isolates_rate_vibrio,
          beam_outbreak_isolates_campylobacter:beam_outbreak_isolates_vibrio)

  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    ","
  )

  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

