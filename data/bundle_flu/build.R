# =============================================================================
# Bundle: Influenza
# Combines (flu variables only): nssp, epic_resp_infections, respnet,
#   nhsn_hospital_capacity, delphi_nhsn, delphi_hospital_claims,
#   delphi_ili_fluview, kinsa_ili, wastewater, fluvaxview, nis_flu_rsv, iis_vax,
#   medicare_vax, vsd_pregnancy_vax, flu_doses_distributed,
#   iqvia_vax_administered, cdc_cfa_rt, nchs_mortality, nnds
#
# Output: parquet files in dist/
#   Surveillance (weekly)
#     flu_overall_trends.parquet       state + national activity, one row per source
#     flu_trends_by_age.parquet        state + national activity by age group
#     flu_ed_visits_by_county.parquet  NSSP ED visits, county
#     flu_hospital_capacity.parquet    NHSN flu hospital measures, long
#     flu_rt_and_mortality.parquet     CFA Rt, NCHS deaths, NNDSS counts, long
#   Vaccination
#     flu_vax_coverage.parquet         coverage, national + state, all strata
#     flu_vax_substate.parquet         coverage, county / HHS region / substate
#     flu_vax_doses.parquet            doses administered and distributed
#
# Only influenza variables are carried. Where a standard file mixes viruses
# (nssp, epic, respnet, wastewater, nhsn, delphi_nhsn, cfa_rt, nis, iis, ...)
# the RSV and COVID columns are dropped. "Influenza and pneumonia" mortality
# (NCHS) is a combined cause and is labelled as such. Haemophilus influenzae
# (nnds) is a bacterium, not influenza, and is excluded.
# =============================================================================

library(dplyr)
library(tidyr)
library(arrow)

process <- dcf::dcf_process_record()

# Trend files start here (the respiratory bundle's start_time). Set to NULL to
# keep every year each source publishes.
TREND_START <- as.Date("2020-01-01")

# -----------------------------------------------------------------------------
# 0. Helpers
# -----------------------------------------------------------------------------
all_fips <- vroom::vroom(
  "../../resources/all_fips.csv.gz",
  show_col_types = FALSE,
  col_types = vroom::cols(.default = vroom::col_character())
)

# all_fips has no name for the territories, only their abbreviation
territory_names <- c(AS = "American Samoa", GU = "Guam",
                     MP = "Northern Mariana Islands", PR = "Puerto Rico",
                     UM = "U.S. Minor Outlying Islands", VI = "U.S. Virgin Islands")

state_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  transmute(geography_fips = geography,
            geography_name = coalesce(geography_name, unname(territory_names[state])))

county_lookup <- all_fips %>%
  filter(nchar(geography) == 5) %>%
  transmute(geography_fips = geography,
            geography_name = paste0(geography_name, ", ", state))

# The respiratory bundle's trend files are limited to these (no territories)
keep_states <- c(state.name, "District of Columbia", "United States")

# vroom guesses FIPS as integers and drops the leading zero, so the id columns
# are pinned to character, but only when the file has them
read_std <- function(path) {
  path <- file.path("..", path)
  have <- names(vroom::vroom(path, n_max = 0, show_col_types = FALSE))
  pin <- c(geography = "c", state_fips = "c", time = "D")
  pin <- pin[names(pin) %in% have]
  vroom::vroom(
    path,
    show_col_types = FALSE,
    col_types = do.call(vroom::cols, c(as.list(pin), list(.default = vroom::col_guess())))
  )
}

# State / national display names. A FIPS with no name is a build error rather
# than a silent NA, since `geography` is what the website shows.
add_state_names <- function(df) {
  out <- df %>%
    rename(geography_fips = geography) %>%
    left_join(state_lookup, by = "geography_fips") %>%
    mutate(geography = if_else(geography_fips == "00", "United States",
                               geography_name)) %>%
    select(-geography_name) %>%
    relocate(geography, geography_fips)
  if (anyNA(out$geography)) {
    stop("no name in all_fips for FIPS: ",
         paste(unique(out$geography_fips[is.na(out$geography)]), collapse = ", "))
  }
  out
}

# vroom attaches `spec`/`problems` attributes that get serialised into the
# parquet, which makes two builds of identical data differ byte for byte
write_dist <- function(df, file) {
  df <- as.data.frame(df)
  attr(df, "spec") <- NULL
  attr(df, "problems") <- NULL
  arrow::write_parquet(df, file.path("dist", file))
}

min_max_100 <- function(x) {
  r <- suppressWarnings(range(x, na.rm = TRUE))
  if (!all(is.finite(r)) || r[2] == r[1]) return(rep(NA_real_, length(x)))
  (x - r[1]) / (r[2] - r[1]) * 100
}

# value_smooth: 3-week trailing mean. value_scale / value_smooth_scale: min-max
# to 0-100 within each series. `presmoothed` sources (Delphi) are not smoothed
# a second time. Series with fewer than 52 observations are dropped.
add_trend_measures <- function(df, by, presmoothed = character()) {
  df %>%
    filter(!is.na(value)) %>%
    group_by(across(all_of(by))) %>%
    filter(n() >= 52) %>%
    arrange(date, .by_group = TRUE) %>%
    mutate(
      value_smooth = zoo::rollapplyr(value, 3, mean, partial = TRUE, na.rm = TRUE),
      value_smooth = if_else(source %in% presmoothed, value, value_smooth),
      value_scale = min_max_100(value),
      value_smooth_scale = min_max_100(value_smooth)
    ) %>%
    ungroup()
}

# -----------------------------------------------------------------------------
# 1. Overall trends: one flu activity series per source, state + national
# -----------------------------------------------------------------------------
# Kinsa is daily; it is averaged to the Saturday that ends each week (weeks with
# fewer than four days, i.e. the one in progress, are dropped).
kinsa_weekly <- function(d) {
  d %>%
    mutate(time = time + (6L - as.integer(format(time, "%u"))) %% 7L) %>%
    group_by(geography, time) %>%
    summarise(kinsa_cough_cold_flu = mean(kinsa_cough_cold_flu, na.rm = TRUE),
              n_days = n(), .groups = "drop") %>%
    filter(n_days >= 4) %>%
    select(-n_days)
}

trend_series <- list(
  list(file = "epic_resp_infections/standard/weekly.csv.gz", col = "epic_pct_flu",
       source = "Epic Cosmos, ED", flag = "epic_suppressed_flag_flu", total_age = TRUE),
  list(file = "nssp/standard/data.csv.gz", col = "percent_visits_flu",
       source = "CDC NSSP"),
  list(file = "respnet/standard/data.csv.gz", col = "rate_flu",
       source = "CDC RespNET", total_age = TRUE),
  list(file = "wastewater/standard/data.csv.gz", col = "wastewater_flua",
       source = "CDC NWSS"),
  list(file = "delphi_nhsn/standard/data.csv.gz", col = "delphi_nhsn_flu",
       source = "CDC NHSN"),
  list(file = "delphi_hospital_claims/standard/data.csv.gz",
       col = "delphi_hospital_flu_smooth", source = "Delphi Hospital Claims"),
  list(file = "delphi_ili_fluview/standard/data.csv.gz", col = "delphi_fluview_wili",
       source = "CDC ILINet"),
  list(file = "kinsa_ili/standard/data.csv.gz", col = "kinsa_cough_cold_flu",
       source = "Kinsa", prep = kinsa_weekly)
)
PRESMOOTHED <- "Delphi Hospital Claims"

load_trend_series <- function(s, by_age = FALSE) {
  d <- read_std(s$file)
  if (!is.null(s$prep)) d <- s$prep(d)
  if (isTRUE(s$total_age)) d <- filter(d, age == "Total")
  d <- d %>% filter(nchar(geography) == 2)
  if (!is.null(TREND_START)) d <- filter(d, time >= TREND_START)
  d$suppressed_flag <- if (!is.null(s$flag)) d[[s$flag]] else 0
  d %>%
    transmute(geography, date = time, source = s$source,
              value = suppressWarnings(as.numeric(.data[[s$col]])), suppressed_flag)
}

flu_overall_trends <- bind_rows(lapply(trend_series, load_trend_series)) %>%
  add_state_names() %>%
  filter(geography %in% keep_states) %>%
  add_trend_measures(by = c("geography", "source"), presmoothed = PRESMOOTHED) %>%
  mutate(suppressed_flag = tidyr::replace_na(suppressed_flag, 0)) %>%
  select(geography, geography_fips, date, source, value, value_smooth,
         value_scale, value_smooth_scale, suppressed_flag) %>%
  arrange(source, geography, date)

write_dist(flu_overall_trends, "flu_overall_trends.parquet")

# -----------------------------------------------------------------------------
# 2. Trends by age group
#    Age labels are each source's own (Epic and RESP-NET: "<1 Years", "1-4
#    Years"; NHSN: "0-4", "5-17"), because the bands do not line up.
# -----------------------------------------------------------------------------
age_series <- list(
  list(file = "epic_resp_infections/standard/weekly.csv.gz", col = "epic_pct_flu",
       source = "Epic Cosmos, ED", flag = "epic_suppressed_flag_flu"),
  list(file = "respnet/standard/data.csv.gz", col = "rate_flu",
       source = "CDC RespNET"),
  list(file = "nhsn_hospital_capacity/standard/data_age.csv.gz",
       col = "nhsn_adm_rate_flu", source = "CDC NHSN")
)

load_age_series <- function(s) {
  d <- read_std(s$file) %>% filter(nchar(geography) == 2, age != "Unknown")
  if (!is.null(TREND_START)) d <- filter(d, time >= TREND_START)
  d$suppressed_flag <- if (!is.null(s$flag)) d[[s$flag]] else 0
  d %>%
    transmute(geography, age, date = time, source = s$source,
              value = suppressWarnings(as.numeric(.data[[s$col]])), suppressed_flag)
}

flu_trends_by_age <- bind_rows(lapply(age_series, load_age_series)) %>%
  add_state_names() %>%
  filter(geography %in% keep_states) %>%
  add_trend_measures(by = c("geography", "age", "source")) %>%
  mutate(suppressed_flag = tidyr::replace_na(suppressed_flag, 0)) %>%
  select(geography, geography_fips, date, age, source, value, value_smooth,
         value_scale, value_smooth_scale, suppressed_flag) %>%
  arrange(source, geography, age, date)

write_dist(flu_trends_by_age, "flu_trends_by_age.parquet")

# -----------------------------------------------------------------------------
# 3. NSSP flu ED visits by county
#    `is_state_estimate` marks counties where NSSP substitutes the state value.
# -----------------------------------------------------------------------------
flu_ed_visits_by_county <- read_std("nssp/standard/data.csv.gz") %>%
  filter(nchar(geography) == 5, !is.na(percent_visits_flu)) %>%
  rename(geography_fips = geography) %>%
  left_join(county_lookup, by = "geography_fips") %>%
  mutate(geography = coalesce(geography_name, geography_fips),
         source = "CDC NSSP") %>%
  select(geography, geography_fips, date = time, source,
         value = percent_visits_flu, is_state_estimate) %>%
  arrange(geography_fips, date)

write_dist(flu_ed_visits_by_county, "flu_ed_visits_by_county.parquet")

# -----------------------------------------------------------------------------
# 4. NHSN flu hospital measures (state, national and HHS region), long
#    `measure` is the NHSN column name. Units differ by measure (patients,
#    admissions, admissions per 100,000, percent of beds, percent of hospitals
#    reporting), so filter on `measure` before comparing or summing.
# -----------------------------------------------------------------------------
cap_measures <- c(
  "nhsn_hosp_pats_flu", "nhsn_icu_pats_flu",
  "nhsn_adm_flu", "nhsn_adm_flu_adult", "nhsn_adm_flu_ped",
  "nhsn_adm_rate_flu", "nhsn_adm_rate_flu_adult", "nhsn_adm_rate_flu_ped",
  "nhsn_pct_inpt_beds_flu", "nhsn_pct_icu_beds_flu",
  "nhsn_pct_hosp_reporting_hosp_pats_flu", "nhsn_pct_hosp_reporting_icu_pats_flu",
  "nhsn_pct_hosp_reporting_adm_flu", "nhsn_pct_hosp_reporting_adm_flu_adult",
  "nhsn_pct_hosp_reporting_adm_flu_ped"
)

cap_long <- function(d) {
  d %>%
    select(geography, time, all_of(cap_measures)) %>%
    pivot_longer(all_of(cap_measures), names_to = "measure", values_to = "value") %>%
    filter(!is.na(value))
}

cap_state <- read_std("nhsn_hospital_capacity/standard/data.csv.gz") %>%
  cap_long() %>%
  add_state_names() %>%
  mutate(geography_level = if_else(geography_fips == "00", "National", "State"))

cap_region <- read_std("nhsn_hospital_capacity/standard/data_region.csv.gz") %>%
  cap_long() %>%
  mutate(geography_fips = geography,
         geography = paste("HHS Region", sub("^hhs_", "", geography)),
         geography_level = "HHS region")

flu_hospital_capacity <- bind_rows(cap_state, cap_region) %>%
  select(geography, geography_fips, geography_level, date = time, measure, value) %>%
  arrange(measure, geography_fips, date)

write_dist(flu_hospital_capacity, "flu_hospital_capacity.parquet")

# -----------------------------------------------------------------------------
# 5. Rt, mortality and notifiable flu, long
#    CDC CFA Rt        epidemic growth (Rt) with its interval and P(growing)
#    NCHS              quarterly death rate, "influenza and pneumonia" combined
#    NNDSS             influenza-associated pediatric deaths, novel influenza A
#
#    NNDSS publishes a year-to-date count that resets each MMWR year, so each
#    series is emitted twice: `_cumulative` as published and `_weekly`
#    de-accumulated (the two are not additive). NNDSS sometimes revises a
#    cumulative count downward, which shows up as a negative weekly increment;
#    these are kept as reported (bundle_respiratory / bundle_measles convention),
#    so plots should cut the y axis at 0.
# -----------------------------------------------------------------------------
cfa_cols <- c("cdc_rt_flu", "cdc_rt_flu_lower", "cdc_rt_flu_upper", "cdc_rt_flu_p_growing")

cfa_long <- read_std("cdc_cfa_rt/standard/data.csv.gz") %>%
  select(geography, time, all_of(cfa_cols)) %>%
  pivot_longer(all_of(cfa_cols), names_to = "measure", values_to = "value") %>%
  mutate(source = "CDC CFA Rt")

nchs_long <- read_std("nchs_mortality/standard/data_state_21_causes.csv.gz") %>%
  transmute(geography, time, measure = "rate_influenza_and_pneumonia",
            value = suppressWarnings(as.numeric(rate_influenza_and_pneumonia)),
            source = "CDC NCHS Mortality")

nnds_cols <- c("influenza_associated_pediatric_mortality",
               "novel_influenza_a_virus_infections",
               "novel_influenza_a_virus_infections_confirmed",
               "novel_influenza_a_virus_infections_total")

nnds_long <- read_std("nnds/standard/data.csv.gz") %>%
  filter(!is.na(geography)) %>%
  select(geography, time, mmwr_year, mmwr_week, all_of(nnds_cols)) %>%
  pivot_longer(all_of(nnds_cols), names_to = "series", values_to = "cumulative") %>%
  arrange(geography, series, mmwr_year, mmwr_week) %>%
  group_by(geography, series, mmwr_year) %>%
  mutate(weekly = cumulative - lag(cumulative, default = 0)) %>%
  ungroup()

n_negative <- sum(nnds_long$weekly < 0, na.rm = TRUE)
if (n_negative > 0) {
  message("NNDSS: ", n_negative, " of ", nrow(nnds_long),
          " weekly increments are negative (downward revisions); kept as reported.")
}

nnds_long <- nnds_long %>%
  pivot_longer(c(cumulative, weekly), names_to = "form", values_to = "value") %>%
  transmute(geography, time, value, source = "CDC NNDSS",
            measure = paste0(series, "_", form))

flu_rt_and_mortality <- bind_rows(cfa_long, nchs_long, nnds_long) %>%
  filter(!is.na(value)) %>%
  add_state_names() %>%
  select(geography, geography_fips, date = time, source, measure, value) %>%
  arrange(source, measure, geography_fips, date)

write_dist(flu_rt_and_mortality, "flu_rt_and_mortality.parquet")

# -----------------------------------------------------------------------------
# 6. Vaccination coverage
#    One schema for every coverage source. A dimension a row is not stratified
#    on carries "Total" (stratum_type "Overall"), so each column can be filtered
#    without handling NA. Suppressed cells keep value = NA with
#    suppressed_flag = 1; nothing is imputed.
#
#    `age` is the age group, `population` a qualifier beyond age ("Pregnant").
#    Labels are each source's own. `measure` is "coverage" for every source;
#    NIS also reports vaccination intent among the unvaccinated.
# -----------------------------------------------------------------------------
COV_COLS <- c("geography", "geography_fips", "date", "season", "source", "age",
              "population", "stratum_type", "stratum", "measure", "value",
              "value_lcl", "value_ucl", "denominator", "n_vaccinated",
              "partial_state_flag", "suppressed_flag")

SUB_COLS <- c("geography", "geography_fips", "geography_level", "state_fips",
              "date", "season", "source", "age", "population", "measure", "value",
              "value_lcl", "value_ucl", "denominator", "n_vaccinated",
              "suppressed_flag")

fill_cols <- function(d, cols) {
  defaults <- list(season = NA_character_, age = "Total", population = "Total",
                   stratum_type = "Overall", stratum = "Total",
                   value_lcl = NA_real_, value_ucl = NA_real_,
                   denominator = NA_real_, n_vaccinated = NA_real_,
                   partial_state_flag = NA_real_,
                   # sources with no suppression mechanism (IIS, Medicare, VSD)
                   suppressed_flag = 0,
                   state_fips = NA_character_)
  for (n in setdiff(names(defaults), names(d))) d[[n]] <- defaults[[n]]
  d %>% select(all_of(cols))
}

# NIS: coverage plus three intent measures. Intent has no interval.
nis_long <- function(d) {
  d <- d %>% filter(vaccine == "flu") %>% mutate(denominator = nis_sample_size)
  bind_rows(
    d %>% mutate(measure = "coverage", value = nis_coverage,
                 value_lcl = nis_coverage_lcl, value_ucl = nis_coverage_ucl),
    d %>% mutate(measure = "intent_definitely", value = nis_intent_definitely),
    d %>% mutate(measure = "intent_probably", value = nis_intent_probably),
    d %>% mutate(measure = "intent_no", value = nis_intent_no)
  ) %>%
    mutate(source = "CDC NIS-Flu")
}

# Medicare and VSD publish race with an "Overall" row
overall_or_race <- function(d, col) {
  d %>% mutate(
    stratum_type = if_else(.data[[col]] == "Overall", "Overall", "Race and Ethnicity"),
    stratum = if_else(.data[[col]] == "Overall", "Total", .data[[col]])
  )
}

# --- national + state --------------------------------------------------------
fvv_state <- read_std("fluvaxview/standard/data_state.csv.gz") %>%
  transmute(geography, time, season, age, source = "CDC FluVaxView",
            measure = "coverage", value = fluvaxview_coverage,
            value_lcl = fluvaxview_coverage_lcl, value_ucl = fluvaxview_coverage_ucl,
            denominator = fluvaxview_sample_size, suppressed_flag)

fvv_race <- read_std("fluvaxview/standard/data_race.csv.gz") %>%
  transmute(geography, time, season, source = "CDC FluVaxView",
            stratum_type = "Race and Ethnicity", stratum = race_ethnicity,
            measure = "coverage", value = fluvaxview_coverage,
            value_lcl = fluvaxview_coverage_lcl, value_ucl = fluvaxview_coverage_ucl,
            denominator = fluvaxview_sample_size, suppressed_flag)

fvv_setting <- read_std("fluvaxview/standard/data_setting.csv.gz") %>%
  transmute(geography, time, season, age, source = "CDC FluVaxView",
            stratum_type = "Vaccination setting", stratum = setting,
            measure = "coverage", value = fluvaxview_coverage,
            value_lcl = fluvaxview_coverage_lcl, value_ucl = fluvaxview_coverage_ucl,
            denominator = fluvaxview_sample_size, suppressed_flag)

nis_overall <- read_std("nis_flu_rsv/standard/data.csv.gz") %>%
  nis_long() %>%
  mutate(age = population, population = "Total")

nis_age <- read_std("nis_flu_rsv/standard/data_age.csv.gz") %>%
  nis_long() %>%
  mutate(population = "Total")

nis_demo <- read_std("nis_flu_rsv/standard/data_demographics.csv.gz") %>%
  nis_long() %>%
  mutate(age = population, population = "Total")

iis_state <- read_std("iis_vax/standard/data.csv.gz") %>%
  filter(vaccine == "flu") %>%
  transmute(geography, time, season, age, source = "IIS", measure = "coverage",
            value = iis_coverage, denominator = iis_population,
            n_vaccinated = iis_n_vaccinated, partial_state_flag)

medicare <- read_std("medicare_vax/standard/data.csv.gz") %>%
  filter(!is.na(geography), vaccine == "flu") %>%
  overall_or_race("race_ethnicity") %>%
  transmute(geography, time, season, age, source = "CMS Medicare",
            stratum_type, stratum, measure = "coverage", value = medicare_coverage)

vsd <- read_std("vsd_pregnancy_vax/standard/data.csv.gz") %>%
  filter(vaccine == "flu") %>%
  overall_or_race("race_ethnicity") %>%
  transmute(geography, time, season, population = "Pregnant",
            source = "CDC Vaccine Safety Datalink", stratum_type, stratum,
            measure = "coverage", value = vsd_coverage, denominator = vsd_denominator)

# The blocks above still call the date `time`, and geography_fips is added with
# the names after the stack
COV_IN <- sub("^date$", "time", setdiff(COV_COLS, "geography_fips"))
SUB_IN <- sub("^date$", "time", SUB_COLS)

flu_vax_coverage <- bind_rows(
  fill_cols(fvv_state, COV_IN), fill_cols(fvv_race, COV_IN),
  fill_cols(fvv_setting, COV_IN), fill_cols(nis_overall, COV_IN),
  fill_cols(nis_age, COV_IN), fill_cols(nis_demo, COV_IN),
  fill_cols(iis_state, COV_IN), fill_cols(medicare, COV_IN),
  fill_cols(vsd, COV_IN)
) %>%
  rename(date = time) %>%
  add_state_names() %>%
  select(all_of(COV_COLS)) %>%
  arrange(source, measure, geography_fips, date, age, stratum_type, stratum)

if (anyDuplicated(flu_vax_coverage[c("geography_fips", "date", "source", "age",
                                     "population", "stratum_type", "stratum",
                                     "measure")])) {
  stop("flu_vax_coverage: duplicate index rows.")
}

write_dist(flu_vax_coverage, "flu_vax_coverage.parquet")

# --- county / HHS region / substate -----------------------------------------
# `geography_fips` is a county FIPS for counties; for the other levels it is the
# source's own id ("hhs_1", "il_city_of_chicago"), which is not a FIPS code.
fvv_county <- read_std("fluvaxview/standard/data_county.csv.gz") %>%
  rename(geography_fips = geography) %>%
  left_join(county_lookup, by = "geography_fips") %>%
  transmute(geography = coalesce(geography_name, geography_fips), geography_fips,
            geography_level = "County", state_fips = substr(geography_fips, 1, 2),
            time, age, source = "CDC FluVaxView", measure = "coverage",
            value = fluvaxview_coverage, value_lcl = fluvaxview_coverage_lcl,
            value_ucl = fluvaxview_coverage_ucl, suppressed_flag)

as_region <- function(d) {
  d %>% mutate(geography_fips = geography,
               geography = paste("HHS Region", sub("^hhs_", "", geography)),
               geography_level = "HHS region", state_fips = NA_character_)
}

as_substate <- function(d) {
  d %>% mutate(geography_fips = geography, geography = geography_name,
               state_fips = if_else(is.na(state_fips), NA_character_,
                                    sprintf("%02d", suppressWarnings(as.integer(state_fips)))))
}

fvv_values <- function(d) {
  d %>% mutate(source = "CDC FluVaxView", measure = "coverage",
               value = fluvaxview_coverage, value_lcl = fluvaxview_coverage_lcl,
               value_ucl = fluvaxview_coverage_ucl, denominator = fluvaxview_sample_size)
}

fvv_region <- read_std("fluvaxview/standard/data_region.csv.gz") %>%
  as_region() %>% fvv_values()

fvv_substate <- read_std("fluvaxview/standard/data_substate.csv.gz") %>%
  as_substate() %>% fvv_values() %>% mutate(geography_level = "Substate")

nis_region <- read_std("nis_flu_rsv/standard/data_region.csv.gz") %>%
  nis_long() %>% as_region() %>% mutate(age = population, population = "Total")

nis_substate <- read_std("nis_flu_rsv/standard/data_substate.csv.gz") %>%
  nis_long() %>% as_substate() %>% mutate(age = population, population = "Total")

iis_substate <- read_std("iis_vax/standard/data_substate.csv.gz") %>%
  filter(vaccine == "flu") %>%
  as_substate() %>%
  mutate(source = "IIS", measure = "coverage", value = iis_coverage,
         denominator = iis_population, n_vaccinated = iis_n_vaccinated)

flu_vax_substate <- bind_rows(
  fill_cols(fvv_county, SUB_IN), fill_cols(fvv_region, SUB_IN),
  fill_cols(fvv_substate, SUB_IN), fill_cols(nis_region, SUB_IN),
  fill_cols(nis_substate, SUB_IN), fill_cols(iis_substate, SUB_IN)
) %>%
  rename(date = time) %>%
  arrange(source, measure, geography_level, geography_fips, date, age)

if (anyNA(flu_vax_substate[c("geography", "geography_fips", "geography_level")])) {
  stop("flu_vax_substate: NA in a geography column.")
}
if (anyDuplicated(flu_vax_substate[c("geography_fips", "geography_level", "date",
                                     "source", "age", "measure")])) {
  stop("flu_vax_substate: duplicate index rows.")
}

write_dist(flu_vax_substate, "flu_vax_substate.parquet")

# -----------------------------------------------------------------------------
# 7. Doses administered (IQVIA) and distributed (CDC), national
#    Every row is a national total for one season week.
# -----------------------------------------------------------------------------
iqvia <- read_std("iqvia_vax_administered/standard/data.csv.gz") %>%
  filter(vaccine == "flu") %>%
  select(geography, time, season, setting, age,
         doses_administered = iqvia_doses,
         doses_administered_cumulative = iqvia_cumulative_doses) %>%
  pivot_longer(c(doses_administered, doses_administered_cumulative),
               names_to = "measure", values_to = "value") %>%
  mutate(source = "IQVIA")

distributed <- read_std("flu_doses_distributed/standard/data.csv.gz") %>%
  transmute(geography, time, season, setting = "Total", age = "Total",
            measure = "doses_distributed_cumulative_millions",
            value = flu_doses_cumulative_millions, source = "CDC Doses Distributed")

flu_vax_doses <- bind_rows(iqvia, distributed) %>%
  filter(!is.na(value)) %>%
  add_state_names() %>%
  select(geography, geography_fips, date = time, season, source, setting, age,
         measure, value) %>%
  arrange(source, measure, setting, age, date)

write_dist(flu_vax_doses, "flu_vax_doses.parquet")
