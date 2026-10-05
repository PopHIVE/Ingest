# =============================================================================
# NHSN Hospital Bed Capacity and Respiratory Admissions Data Ingestion
# Source: Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction,
#         National Healthcare Safety Network (NHSN)
#         https://data.cdc.gov/d/ua7e-t2fy
#
# Output: standard/data.csv.gz            - national, states, DC and territories
#         standard/data_age.csv.gz        - same geographies, admissions by age
#         standard/data_region.csv.gz     - HHS regions (hhs_1 ... hhs_10)
#         standard/data_region_age.csv.gz - HHS regions, admissions by age
#
# Through the week ending 2024-10-05 the source reports bed and patient values
# as weekly averages of daily values; from 2024-10-12 it reports the value for
# the Wednesday of the week. Admissions are weekly totals throughout. Reporting
# was voluntary from 2024-05-01 to 2024-10-31, so the nhsn_pct_hosp_reporting_*
# columns are kept to identify low-coverage weeks.
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
  BED_COLS <- c(
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

  # New admissions: the labels follow one pattern per virus
  VIRUSES <- c(covid = "COVID-19", flu = "Influenza", rsv = "RSV")
  ADM_COLS <- unlist(lapply(names(VIRUSES), function(v) {
    V <- VIRUSES[[v]]
    setNames(
      c(
        paste0("Total ", V, " Admissions"),
        paste0("Total Adult ", V, " Admissions"),
        paste0("Total Pediatric ", V, " Admissions"),
        paste0("Total number of ", V, " Admissions per 100,000 population"),
        paste0("Total Number of Adult ", V, " Admissions per 100,000 population"),
        paste0("Total Number of Pediatric ", V, " Admissions per 100,000 population"),
        paste0("Percent Hospitals Reporting ", V, " Admissions"),
        paste0("Percent Hospitals Reporting Adult ", V, " Admissions"),
        paste0("Percent Hospitals Reporting Pediatric ", V, " Admissions")
      ),
      paste0(
        c("nhsn_adm_", "nhsn_adm_", "nhsn_adm_", "nhsn_adm_rate_", "nhsn_adm_rate_", "nhsn_adm_rate_",
          "nhsn_pct_hosp_reporting_adm_", "nhsn_pct_hosp_reporting_adm_", "nhsn_pct_hosp_reporting_adm_"),
        v, c("", "_adult", "_ped")
      )
    )
  }))
  VALUE_COLS <- c(BED_COLS, ADM_COLS)

  # Admissions by age band; CDC publishes no rate for unknown age
  AGE_BANDS <- c(
    "0-4" = "Pediatric", "5-17" = "Pediatric",
    "18-49" = "Adult", "50-64" = "Adult", "65-74" = "Adult", "75+" = "Adult"
  )
  age_label <- function(V, age, rate = FALSE) {
    if (age == "Unknown") return(paste0("Number of ", V, " Admissions, unknown age"))
    paste0("Number of ", AGE_BANDS[[age]], " ", V, " Admissions, ", age, " years",
           if (rate) ", per 100,000 population")
  }
  AGE_LABELS <- unlist(lapply(VIRUSES, function(V) c(
    sapply(names(AGE_BANDS), age_label, V = V),
    sapply(names(AGE_BANDS), age_label, V = V, rate = TRUE),
    age_label(V, "Unknown")
  )))

  needed <- c("Week Ending Date", "Geographic aggregation", VALUE_COLS, AGE_LABELS)
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) stop("ua7e-t2fy columns not found: ", paste(absent, collapse = ", "))

  # CDC date columns arrive as "YYYY-MM-DD ...", "MM/DD/YYYY ...", or
  # "YYYY Mon DD ...", depending on how the file was exported
  parse_cdc_date <- function(x) {
    out <- as.Date(rep(NA_character_, length(x)))
    for (fmt in c("%Y-%m-%d", "%m/%d/%Y", "%Y %b %d")) {
      i <- is.na(out) & !is.na(x)
      out[i] <- as.Date(x[i], format = fmt)
    }
    bad <- is.na(out) & !is.na(x)
    if (any(bad)) stop("unparsed dates: ", paste(head(unique(x[bad])), collapse = ", "))
    out
  }
  # Numbers arrive with or without thousands separators
  parse_cdc_number <- function(x) {
    out <- suppressWarnings(as.numeric(gsub(",", "", x, fixed = TRUE)))
    bad <- is.na(out) & !is.na(x)
    if (any(bad)) stop("unparsed numbers: ", paste(head(unique(x[bad])), collapse = ", "))
    out
  }
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

  time <- parse_cdc_date(raw[["Week Ending Date"]])
  if (any(format(time, "%u") != "6")) stop("week ending dates not on Saturday")

  all <- raw %>%
    transmute(
      jurisdiction = `Geographic aggregation`,
      time = format(time, "%Y-%m-%d"),
      across(all_of(unname(c(VALUE_COLS, AGE_LABELS))), parse_cdc_number)
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

  wide <- all %>%
    select(geography, time, all_of(VALUE_COLS)) %>%
    arrange(geography, time)

  ages <- c(names(AGE_BANDS), "Unknown")
  by_age <- bind_rows(lapply(ages, function(a) {
    out <- all %>% transmute(geography, time, age = a)
    for (v in names(VIRUSES)) {
      out[[paste0("nhsn_adm_", v)]] <- all[[age_label(VIRUSES[[v]], a)]]
      out[[paste0("nhsn_adm_rate_", v)]] <-
        if (a == "Unknown") NA_real_ else all[[age_label(VIRUSES[[v]], a, rate = TRUE)]]
    }
    out
  })) %>%
    select(geography, time, age, starts_with("nhsn_adm_")) %>%
    arrange(geography, time, match(age, ages))

  is_region <- function(d) grepl("^hhs_", d$geography)

  wide %>%
    filter(!is_region(.)) %>%
    check_unique(c("geography", "time"), "data") %>%
    vroom::vroom_write("standard/data.csv.gz", ",")
  wide %>%
    filter(is_region(.)) %>%
    check_unique(c("geography", "time"), "data_region") %>%
    vroom::vroom_write("standard/data_region.csv.gz", ",")
  by_age %>%
    filter(!is_region(.)) %>%
    check_unique(c("geography", "time", "age"), "data_age") %>%
    vroom::vroom_write("standard/data_age.csv.gz", ",")
  by_age %>%
    filter(is_region(.)) %>%
    check_unique(c("geography", "time", "age"), "data_region_age") %>%
    vroom::vroom_write("standard/data_region_age.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
