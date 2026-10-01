# =============================================================================
# NHSN Hospital Bed Capacity Data Ingestion
# Source: Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction,
#         National Healthcare Safety Network (NHSN)
#         https://data.cdc.gov/d/ua7e-t2fy
#
# Output: standard/data.csv.gz        - national, states, DC and territories
#         standard/data_region.csv.gz - HHS regions (geography hhs_1 ... hhs_10)
#
# Through the week ending 2024-10-05 the source reports weekly averages of
# daily values; from 2024-10-12 it reports the value for the Wednesday of the
# week. Reporting was voluntary from 2024-05-01 to 2024-10-31, so the
# nhsn_pct_hosp_reporting_* columns are kept to identify low-coverage weeks.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc("ua7e-t2fy", "raw", process$raw_state)

if (!identical(process$raw_state, raw_state)) {

  raw <- vroom::vroom(
    "raw/ua7e-t2fy.csv.xz", delim = ",",
    col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
  )

  # Standard name = source column label. Labels are written out in full because
  # the source is not consistent (e.g. "Inpatient beds", "TotalPatients").
  VALUE_COLS <- c(
    nhsn_inpt_beds           = "Number of Inpatient Beds",
    nhsn_inpt_beds_adult     = "Number of Adult Inpatient Beds",
    nhsn_inpt_beds_ped       = "Number of Pediatric Inpatient beds",
    nhsn_inpt_beds_occ       = "Number of Inpatient Beds Occupied",
    nhsn_inpt_beds_occ_adult = "Number of Adult Inpatient Beds Occupied",
    nhsn_inpt_beds_occ_ped   = "Number of Pediatric Inpatient Beds Occupied",
    nhsn_icu_beds            = "Number of ICU Beds",
    nhsn_icu_beds_adult      = "Number of Adult ICU Beds",
    nhsn_icu_beds_ped        = "Number of Pediatric ICU Beds",
    nhsn_icu_beds_occ        = "Number of ICU Beds Occupied",
    nhsn_icu_beds_occ_adult  = "Number of Adult ICU Beds Occupied",
    nhsn_icu_beds_occ_ped    = "Number of Pediatric ICU Beds Occupied",

    nhsn_pct_inpt_beds_occ       = "Percent Inpatient Beds Occupied",
    nhsn_pct_inpt_beds_occ_adult = "Percent Adult Inpatient Beds Occupied",
    nhsn_pct_inpt_beds_occ_ped   = "Percent Pediatric Inpatient Beds Occupied",
    nhsn_pct_icu_beds_occ        = "Percent ICU Beds Occupied",
    nhsn_pct_icu_beds_occ_adult  = "Percent Adult ICU Beds Occupied",
    nhsn_pct_icu_beds_occ_ped    = "Percent Pediatric ICU Beds Occupied",

    nhsn_pct_hosp_reporting_inpt_beds     = "Percent Hospitals Reporting Number of Inpatient Beds",
    nhsn_pct_hosp_reporting_inpt_beds_occ = "Percent Hospitals Reporting Number of Inpatient Beds Occupied",
    nhsn_pct_hosp_reporting_icu_beds      = "Percent Hospitals Reporting Number of ICU Beds",
    nhsn_pct_hosp_reporting_icu_beds_occ  = "Percent Hospitals Reporting Number of ICU Beds Occupied",

    nhsn_pct_inpt_beds_covid = "Percent Inpatient Beds Occupied by COVID-19 Patients",
    nhsn_pct_inpt_beds_flu   = "Percent Inpatient Beds Occupied by Influenza Patients",
    nhsn_pct_inpt_beds_rsv   = "Percent Inpatient Beds Occupied by RSV Patients",
    nhsn_pct_icu_beds_covid  = "Percent ICU Beds Occupied by COVID-19 Patients",
    nhsn_pct_icu_beds_flu    = "Percent ICU Beds Occupied by Influenza Patients",
    nhsn_pct_icu_beds_rsv    = "Percent ICU Beds Occupied by RSV Patients",

    nhsn_hosp_pats_covid = "Total Patients Hospitalized with COVID-19",
    nhsn_hosp_pats_flu   = "Total Patients Hospitalized with Influenza",
    nhsn_hosp_pats_rsv   = "Total Patients Hospitalized with RSV",
    nhsn_icu_pats_covid  = "Total ICU Patients Hospitalized with COVID-19",
    nhsn_icu_pats_flu    = "Total ICU Patients Hospitalized with Influenza",
    nhsn_icu_pats_rsv    = "Total ICU Patients Hospitalized with RSV",

    nhsn_pct_hosp_reporting_hosp_pats_covid = "Percent Hospitals Reporting Total Patients Hospitalized with COVID-19",
    nhsn_pct_hosp_reporting_hosp_pats_flu   = "Percent Hospitals Reporting TotalPatients Hospitalized with Influenza",
    nhsn_pct_hosp_reporting_hosp_pats_rsv   = "Percent Hospitals Reporting Total Patients Hospitalized with RSV",
    nhsn_pct_hosp_reporting_icu_pats_covid  = "Percent Hospitals Reporting ICU Patients Hospitalized with COVID-19",
    nhsn_pct_hosp_reporting_icu_pats_flu    = "Percent Hospitals Reporting ICU Patients Hospitalized with Influenza",
    nhsn_pct_hosp_reporting_icu_pats_rsv    = "Percent Hospitals Reporting ICU Patients Hospitalized with RSV"
  )

  needed <- c("Week Ending Date", "Geographic aggregation", VALUE_COLS)
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) stop("ua7e-t2fy columns not found: ", paste(absent, collapse = ", "))

  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }

  # Jurisdictions are postal abbreviations, "USA", or "Region N" (HHS regions)
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state) %>%
    distinct(state, .keep_all = TRUE)

  time <- as.Date(substr(raw[["Week Ending Date"]], 1, 10), format = "%Y-%m-%d")
  if (any(is.na(time))) stop("unparsed dates")
  if (any(format(time, "%u") != "6")) stop("week ending dates not on Saturday")

  all <- raw %>%
    transmute(
      jurisdiction = `Geographic aggregation`,
      time = format(time, "%Y-%m-%d"),
      across(all_of(VALUE_COLS), as.numeric)
    ) %>%
    left_join(state_fips_lookup, by = c("jurisdiction" = "state")) %>%
    mutate(
      geography = case_when(
        jurisdiction == "USA" ~ "00",
        grepl("^Region [0-9]+$", jurisdiction) ~ sub("^Region ", "hhs_", jurisdiction),
        TRUE ~ geography
      )
    )

  unmapped <- unique(all$jurisdiction[is.na(all$geography)])
  if (length(unmapped) > 0) stop("Jurisdictions not mapped: ", paste(unmapped, collapse = ", "))

  all <- all %>%
    select(geography, time, all_of(names(VALUE_COLS))) %>%
    arrange(geography, time)

  data <- all %>%
    filter(!grepl("^hhs_", geography)) %>%
    check_unique(c("geography", "time"), "data")
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  data_region <- all %>%
    filter(grepl("^hhs_", geography)) %>%
    check_unique(c("geography", "time"), "data_region")
  vroom::vroom_write(data_region, "standard/data_region.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
