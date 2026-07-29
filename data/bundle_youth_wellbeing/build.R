# =============================================================================
# Bundle: youth_wellbeing
#
# Organized by DATASET (one block per source), not by dashboard page. Every row
# carries `section` and `focus_area`, so a page selects its content with a
# single filter and a dataset feeding several pages is still read only once.
#
# The section / focus_area / variable mapping comes from the 'Overview' tab of
# 'dashboard overview for dataface.xlsx' in this folder. It is transcribed into
# the SPEC table below rather than read at build time, so the pipeline does not
# depend on readxl or on the workbook staying in place. Update SPEC when the
# sheet changes.
#
# Four corrections applied to the sheet:
#   * epic_rate_ed_opioid / epic_n_ed_opioid are listed under "Epic Chronic" but
#     actually live in epic_injury, alongside the firearm and heat measures.
#   * Row 29 (Preventative -> Vaccinations) listed no variables; it is sourced
#     from the childhood immunization bundle's combined dist file.
#   * chr_food_environment_index and chr_limited_access_to_healthy_foods each
#     appear under two section/focus_area pairs. That is intentional -- those
#     rows are emitted twice, once per pair.
#   * wisqars_{rate,deaths}_firearm_legal_intervention are dropped: WISQARS
#     reports them for only 3 geography-years nationally across all ages, and
#     for none at youth ages.
#
# Scope: youth only. Age bands entirely under 18 are kept, plus bands that
# straddle 18 where the source cannot split them any finer (WISQARS/NHTSA
# 15-24, Epic Injury 15-25). Datasets with no age dimension are kept whole and
# carry age = 'Not stratified'.
#
# Output naming: dist/<dataset>_<geography>_<demographics>.parquet
# =============================================================================

library(tidyverse)
library(arrow)

# =============================================================================
# 0. SHARED SETUP
# =============================================================================

all_fips <- vroom::vroom('../../resources/all_fips.csv.gz', show_col_types = FALSE)

# 50 states + DC + national, keyed on FIPS. Excludes territories and the DC
# county record (11001), which would otherwise duplicate DC.
state_cw <- all_fips %>%
  filter(geography == '00' |
           (geography_name %in% c(state.name, 'District of Columbia') & geography != '11001')) %>%
  mutate(geography_name = if_else(geography == '00', 'United States', geography_name)) %>%
  select(fips = geography, geography_name, state)

county_cw <- all_fips %>%
  filter(nchar(geography) == 5) %>%
  select(fips = geography, geography_name, state)

# Harmonized youth age labels. Bands that are genuinely the same range across
# sources get the same label: WISQARS/NHTSA '0-14' and Epic Injury '<15 Years'
# are both '0-14 years'. Epic Injury's 15-25 is wider than WISQARS/NHTSA's
# 15-24, so it keeps its own label. 'Not stratified' marks a source with no age
# dimension at all -- deliberately not 'Overall', which would read as a youth
# total it is not.
AGE_WISQARS <- c('0-14 Years' = '0-14 years', '15-24 Years' = '15-24 years')
AGE_NHTSA   <- c('0-14'       = '0-14 years', '15-24'       = '15-24 years')
AGE_EPIC_I  <- c('<15 Years'  = '0-14 years', '15-25 Years' = '15-25 years')
AGE_EPIC_C  <- c('<18 Years'  = '0-17 years')
AGE_YRBSS   <- c('14' = '14 years', '15' = '15 years', '16' = '16 years',
                 '17' = '17 years', 'Overall' = 'Overall')

# YRBSS reports race and Hispanic ethnicity in one column; WISQARS keeps them
# separate. Map YRBSS onto the WISQARS scheme so both use `race` / `ethnicity`.
# The schemes are not exactly equivalent: YRBSS race levels are single-race
# non-Hispanic, while WISQARS reports race regardless of ethnicity.
YRBSS_RACE <- c('AI/AN'    = 'American Indian/Alaska Native',
                'Asian'    = 'Asian',
                'Black'    = 'Black',
                'NH/PI'    = 'Native Hawaiian/Pacific Islander',
                'White'    = 'White',
                'Multiple' = 'More than one race')

# Canonical column order; each file carries the subset that applies to it.
COL_ORDER <- c('geography', 'fips', 'state', 'time', 'year',
               'age', 'sex', 'race', 'ethnicity', 'payer', 'domain',
               'vaccine', 'product', 'diagnosis', 'forecast_day',
               'section', 'focus_area', 'source', 'measure', 'rank', 'value',
               'pct_of_all', 'lcl', 'ucl', 'pct_25', 'pct_75', 'sample_size', 'n_sampled',
               'suppressed', 'not_asked', 'low_coverage_flag', 'unstable_flag',
               'percent_surveyed', 'survey_type')

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

# Pivot wide measure columns to tall: one row per stratum x measure.
tall_simple <- function(df, measures, strata) {
  measures <- intersect(measures, names(df))
  df %>%
    select(all_of(c(strata, measures))) %>%
    pivot_longer(all_of(measures), names_to = 'measure', values_to = 'value',
                 values_transform = list(value = as.numeric))
}

# Pivot wide measure columns to tall, carrying per-measure companion columns
# named <measure><suffix>. Each suffix becomes a column of its own, e.g.
# '_lcl' -> lcl. Measures are matched longest-first so that overlapping names
# (pct_no_breakfast vs pct_no_breakfast_7days) resolve correctly.
tall_flagged <- function(df, measures, strata, suffixes) {
  measures <- intersect(measures, names(df))
  measures <- measures[order(-nchar(measures))]
  wanted <- c(measures, paste0(rep(measures, each = length(suffixes)),
                               rep(suffixes, length(measures))))
  df %>%
    select(all_of(c(strata, intersect(wanted, names(df))))) %>%
    # Suffix the bare measure column so every column reads <measure>_<field>
    # and pivot_longer's .value can split them in one pass.
    rename_with(~ paste0(.x, '_value'), all_of(measures)) %>%
    pivot_longer(
      cols          = -all_of(strata),
      names_to      = c('measure', '.value'),
      names_pattern = sprintf('^(%s)_(%s)$',
                              paste(measures, collapse = '|'),
                              paste(c('value', sub('^_', '', suffixes)), collapse = '|'))
    )
}

# Attach section / focus_area and tidy up. A measure mapped to two
# section/focus_area pairs is emitted once per pair (many-to-many is expected).
label <- function(df, dataset, source) {
  sp <- SPEC %>% filter(dataset == !!dataset) %>% select(section, focus_area, measure)
  df %>%
    inner_join(sp, by = 'measure', relationship = 'many-to-many') %>%
    mutate(source = source, year = as.integer(lubridate::year(time))) %>%
    relocate(any_of(COL_ORDER)) %>%
    arrange(section, focus_area, measure, fips, time)
}

# Join state names onto a FIPS-keyed frame, dropping territories.
as_state <- function(df) {
  df %>%
    inner_join(state_cw, by = 'fips') %>%
    rename(geography = geography_name) %>%
    select(-state)
}

# =============================================================================
# SPEC -- variable to section / focus_area, transcribed from the Overview tab
# =============================================================================

sp <- function(dataset, section, focus_area, measure) {
  tibble(dataset = dataset, section = section, focus_area = focus_area, measure = measure)
}
# WISQARS ships each cause as a rate_ and a deaths_ column.
wq <- function(causes) as.vector(outer(c('wisqars_rate_', 'wisqars_deaths_'), causes, paste0))

SPEC <- bind_rows(
  # ---- WISQARS ----
  sp('wisqars', 'Injury and violence', 'Motor vehicle accidents',
     wq(c('pedal_cyclist_mv_traffic', 'pedestrian_mv_traffic', 'motor_vehicle_traffic'))),
  # firearm_legal_intervention is omitted: WISQARS reports it for only 3
  # geography-years nationally across all ages, and none at youth ages.
  sp('wisqars', 'Injury and violence', 'Firearms',
     wq(c('firearm_accident', 'firearm_homicide', 'firearm_suicide',
          'firearm_intentional'))),
  sp('wisqars', 'Substance abuse', 'Overdose trends', wq('drug_poisoning')),

  # ---- NHTSA ----
  sp('nhtsa', 'Injury and violence', 'Motor vehicle accidents', 'nhtsa_fatalities'),

  # ---- NEISS (not in the workbook; added for the injury page) ----
  # `product` / `diagnosis` are dimensions, so both datasets carry the same
  # single measure and are separated by focus_area.
  sp('neiss_product', 'Injury and violence', 'Consumer product injuries',
     'neiss_injuries_weighted'),
  sp('neiss_diagnosis', 'Injury and violence', 'Injury diagnoses',
     'neiss_injuries_weighted'),

  # ---- YRBSS ----
  sp('yrbss', 'Injury and violence', 'Dangerous driving',
     c('pct_no_seatbelt', 'pct_rode_drinking_driver', 'pct_drove_drinking',
       'pct_text_while_driving')),
  sp('yrbss', 'Injury and violence', 'Concussions', 'pct_sports_concussion'),
  sp('yrbss', 'Injury and violence', 'Firearms',
     c('pct_carried_weapon_school', 'pct_carried_gun', 'pct_threatened_weapon_school')),
  sp('yrbss', 'Substance abuse', 'Substances used',
     c('pct_ever_cigarette', 'pct_ever_vape', 'pct_current_smokeless_tobacco',
       'pct_current_alcohol', 'pct_binge_drinking', 'pct_ever_marijuana',
       'pct_early_marijuana', 'pct_current_marijuana', 'pct_ever_rx_opioid_misuse',
       'pct_current_rx_opioid_misuse', 'pct_ever_cocaine', 'pct_ever_inhalants',
       'pct_ever_heroin', 'pct_ever_methamphetamines', 'pct_ever_ecstasy',
       'pct_ever_hallucinogens', 'pct_ever_inject_drug', 'pct_ever_illicit_drug')),
  sp('yrbss', 'Mental health', 'Suicide and suicidal ideation',
     c('pct_considered_suicide', 'pct_planned_suicide', 'pct_attempted_suicide',
       'pct_injurious_suicide_attempt')),
  sp('yrbss', 'Mental health', 'Bullying', c('pct_bullied_at_school', 'pct_bullied_electronic')),
  sp('yrbss', 'Mental health', 'General mental health measures',
     c('pct_social_media_daily', 'pct_poor_mental_health', 'pct_insufficient_sleep',
       'pct_not_close_at_school')),
  sp('yrbss', 'Preventative health and wellness', 'Activity levels',
     c('pct_inactive_60min_5days', 'pct_no_pe_classes', 'pct_no_sports_team',
       'pct_no_daily_pe', 'pct_inactive_all_days')),
  sp('yrbss', 'Preventative health and wellness', 'Nutrition',
     c('pct_no_breakfast', 'pct_no_breakfast_7days', 'pct_no_fruit', 'pct_no_vegetables')),

  # ---- Epic Injury (includes the opioid measures the sheet filed under Epic Chronic) ----
  sp('epic_injury', 'Injury and violence', 'Firearms',
     c('epic_n_ed_firearm', 'epic_rate_ed_firearm')),
  sp('epic_injury', 'Substance abuse', 'Overdose trends',
     c('epic_rate_ed_opioid', 'epic_n_ed_opioid')),
  sp('epic_injury', 'Chronic disease', 'Environmental health',
     c('epic_rate_ed_heat', 'epic_n_ed_heat')),

  # ---- Epic Chronic ----
  sp('epic_chronic', 'Chronic disease', 'Obesity',
     c('obesity_bmi', 'obesity_dx_ccw', 'n_patients_chronic')),
  sp('epic_chronic', 'Chronic disease', 'Diabetes', c('diabetes_a1c_6_5', 'diabetes_dx_ccw')),

  # ---- medicaid_quality ----
  sp('medicaid', 'Substance abuse', 'Follow up for ED visits for drug/alcohol related reasons',
     c('medicaid_fua_ch_30d_rate', 'medicaid_fua_ch_7d_rate')),
  sp('medicaid', 'Mental health', 'Care seeking',
     c('medicaid_fuh_ch_30d_rate', 'medicaid_fuh_ch_7d_rate')),
  sp('medicaid', 'Chronic disease', 'Asthma',
     c('medicaid_mma_ch_rate', 'medicaid_amr_ch_rate')),
  sp('medicaid', 'Chronic disease', 'Lead testing', 'medicaid_lsc_ch_rate'),
  sp('medicaid', 'Preventative health and wellness', 'Pregnancy',
     c('medicaid_fpc_ch_rate', 'medicaid_ppc_ch_rate')),
  sp('medicaid', 'Preventative health and wellness', 'Contraception', 'medicaid_ccp_ch_rate'),
  sp('medicaid', 'Preventative health and wellness', 'Preventative health',
     c('medicaid_ima_ch_rate', 'medicaid_w34_ch_rate', 'medicaid_awc_ch_rate',
       'medicaid_w15_ch_rate', 'medicaid_w30_ch_rate', 'medicaid_oev_ch_rate',
       'medicaid_cap_ch_rate', 'medicaid_dev_ch_rate')),
  sp('medicaid', 'Preventative health and wellness', 'Nutrition', 'medicaid_wcc_ch_rate'),

  # ---- County Health Rankings ----
  sp('chr', 'Mental health', 'General mental health measures', 'chr_disconnected_youth'),
  sp('chr', 'Chronic disease', 'Lead testing', 'chr_lead_poisoned_children'),
  sp('chr', 'Chronic disease', 'Environmental health',
     c('chr_air_pollution_ozone_days', 'chr_air_pollution_particulate_matter_days',
       'chr_contaminants_in_municipal_water_wi', 'chr_air_pollution_particulate_matter',
       'chr_adverse_climate_events')),
  sp('chr', 'Preventative health and wellness', 'Pregnancy', 'chr_teen_births'),
  sp('chr', 'Preventative health and wellness', 'Nutrition',
     c('chr_limited_access_to_healthy_foods', 'chr_food_environment_index')),
  sp('chr', 'Social determinants of health', 'Schools',
     c('chr_high_school_completion', 'chr_school_segregation', 'chr_school_funding_adequacy',
       'chr_child_care_centers', 'chr_high_school_graduation', 'chr_math_scores',
       'chr_reading_scores')),
  sp('chr', 'Social determinants of health', 'Access to care',
     c('chr_uninsured_children', 'chr_primary_care_physicians', 'chr_inadequate_social_support',
       'chr_other_primary_care_providers')),
  sp('chr', 'Social determinants of health', 'Poverty measures',
     c('chr_children_in_poverty', 'chr_income_inequality', 'chr_food_insecurity')),
  sp('chr', 'Social determinants of health', 'Housing',
     c('chr_single_parent_households', 'chr_high_housing_costs',
       'chr_severe_housing_problems', 'chr_severe_housing_cost_burden')),
  sp('chr', 'Social determinants of health', 'Surrounding community',
     c('chr_access_to_recreational_facilities', 'chr_limited_access_to_healthy_foods',
       'chr_access_to_parks', 'chr_access_to_exercise_opportunities',
       'chr_food_environment_index', 'chr_residential_segregation_black_white')),
  sp('chr', 'Social determinants of health', 'Crime/violence',
     c('chr_violent_crime', 'chr_juvenile_arrests', 'chr_firearm_fatalities')),

  # ---- NOAA heat risk ----
  sp('noaa_heat_risk', 'Chronic disease', 'Environmental health', 'heat_risk'),

  # ---- Childhood immunizations ----
  sp('immunizations', 'Preventative health and wellness', 'Vaccinations', 'pct_vaccinated')
)

stopifnot(!anyDuplicated(SPEC[c('dataset', 'section', 'focus_area', 'measure')]))


# =============================================================================
# 1. WISQARS -- injury deaths, state, age x (sex | race | ethnicity)
#
#   dist/wisqars_state_age_demographics.parquet
#
# WISQARS ships the demographic dimensions partially crossed: age alone,
# age x sex, age x race and age x ethnicity all exist, but race x ethnicity
# does not. Unstratified dimensions are already labelled 'All' at source.
# =============================================================================

wisqars_measures <- SPEC %>% filter(dataset == 'wisqars') %>% pull(measure) %>% unique()

vroom::vroom('../wisqars/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(age %in% names(AGE_WISQARS)) %>%
  mutate(age = unname(AGE_WISQARS[age])) %>%
  tall_simple(wisqars_measures, c('geography', 'time', 'age', 'sex', 'race', 'ethnicity')) %>%
  rename(fips = geography) %>%
  filter(!is.na(value)) %>%
  as_state() %>%
  label('wisqars', 'CDC/WISQARS') %>%
  write_parquet('dist/wisqars_state_age_demographics.parquet')


# =============================================================================
# 2. NHTSA FARS -- motor vehicle fatalities, state + county, age x sex
#
#   dist/nhtsa_state_age_sex.parquet
#   dist/nhtsa_county_age_sex.parquet
# =============================================================================

nhtsa_youth <- vroom::vroom('../nhtsa_crash/standard/data_age_sex.csv.gz', show_col_types = FALSE) %>%
  filter(age %in% names(AGE_NHTSA)) %>%
  mutate(age = unname(AGE_NHTSA[age]))

# FARS supplies Male and Female only, with no sex total, so sum the two.
# Fatalities with unknown age or sex are absent from this file entirely (~0.5%
# nationally), so these totals sit slightly below the unstratified data.csv.gz.
nhtsa_tall <- nhtsa_youth %>%
  bind_rows(
    nhtsa_youth %>%
      group_by(geography, time, age) %>%
      summarize(nhtsa_fatalities = sum(nhtsa_fatalities, na.rm = TRUE), .groups = 'drop') %>%
      mutate(sex = 'All')
  ) %>%
  tall_simple('nhtsa_fatalities', c('geography', 'time', 'age', 'sex')) %>%
  rename(fips = geography) %>%
  filter(!is.na(value))

nhtsa_tall %>%
  filter(nchar(fips) == 2) %>%
  as_state() %>%
  label('nhtsa', 'NHTSA FARS') %>%
  write_parquet('dist/nhtsa_state_age_sex.parquet')

nhtsa_tall %>%
  filter(nchar(fips) == 5) %>%
  inner_join(county_cw, by = 'fips') %>%
  rename(geography = geography_name) %>%
  label('nhtsa', 'NHTSA FARS') %>%
  write_parquet('dist/nhtsa_county_age_sex.parquet')


# =============================================================================
# 3. YRBSS -- high school risk behaviors, state, age x (sex | race/ethnicity)
#
#   dist/yrbss_state_age_demographics.parquet
#
# YRBSS crosses one dimension at a time: single-year ages 14-17 exist only
# unstratified, and sex / race / ethnicity exist only at age == 'Overall'.
#
# NOTE ON `value` FOR FLAGGED ROWS: the yrbss source writes value = 0 (not NA)
# when a row is suppressed or the question was not asked, and that is preserved
# here so the bundle stays faithful to the source. A large share of YRBSS rows
# is flagged, so anything reading `value` MUST filter on
# suppressed == 0 & not_asked == 0 or it will plot spurious zeros.
# =============================================================================

yrbss_measures <- SPEC %>% filter(dataset == 'yrbss') %>% pull(measure) %>% unique()
YRBSS_FLAGS <- c('_lcl', '_ucl', '_suppressed', '_not_asked')

yrbss_read <- function(file, strata) {
  vroom::vroom(file.path('../yrbss/standard', file), show_col_types = FALSE) %>%
    filter(age %in% names(AGE_YRBSS)) %>%
    mutate(age = unname(AGE_YRBSS[age])) %>%
    tall_flagged(yrbss_measures, c('geography', 'time', 'age', strata), YRBSS_FLAGS)
}

# The sex and race/ethnicity files repeat the unstratified rows under an
# 'Overall' level; drop those so they aren't duplicated against the age file.
bind_rows(
  yrbss_read('data_age.csv.gz', character()),
  yrbss_read('data_age_sex.csv.gz', 'sex') %>% filter(sex != 'Overall'),
  yrbss_read('data_age_ethnicity.csv.gz', 'race_ethnicity') %>%
    filter(race_ethnicity != 'Overall') %>%
    mutate(race      = replace_na(unname(YRBSS_RACE[race_ethnicity]), 'All'),
           ethnicity = if_else(race_ethnicity == 'Hispanic', 'Hispanic', 'All')) %>%
    select(-race_ethnicity)
) %>%
  mutate(across(c(sex, race, ethnicity), ~ replace_na(.x, 'All'))) %>%
  rename(fips = geography) %>%
  as_state() %>%
  label('yrbss', 'CDC/YRBSS') %>%
  write_parquet('dist/yrbss_state_age_demographics.parquet')


# =============================================================================
# 4. Epic Injury -- ED visits for firearm injury, opioid overdose, heat illness
#
#   dist/epic_injury_state_age_year.parquet
#   dist/epic_injury_state_age_month.parquet
#
# All states are retained, including Alaska. (bundle_injury_overdose nulls out
# Alaska for these measures; this bundle deliberately does not, so the data is
# left as the source reports it.)
#
# Suppression is keyed on topic (firearm / opioid / heat) and shared by that
# topic's n and rate columns. CAVEAT: when a cell is suppressed the source
# imputes n = 5, so the derived rate is meaningless -- with a small denominator
# it can reach tens of thousands per 100,000. Alaska, DC and Utah are the worst
# affected. Filter on suppressed == 0 before using `value` for any rate
# measure: doing so drops the monthly maximum from ~41,700 to ~1,300 per
# 100,000. Suppression is heavy at monthly resolution for the youngest band, so
# prefer the annual file there.
# =============================================================================

epic_injury_measures <- SPEC %>% filter(dataset == 'epic_injury') %>% pull(measure) %>% unique()

epic_injury_tall <- function(file) {
  raw <- vroom::vroom(file.path('../epic_injury/standard', file), show_col_types = FALSE) %>%
    filter(age %in% names(AGE_EPIC_I), !is.na(time)) %>%
    mutate(age = unname(AGE_EPIC_I[age]))
  keys <- c('geography', 'time', 'age')
  suppressed <- raw %>%
    select(all_of(keys), starts_with('suppressed_')) %>%
    pivot_longer(starts_with('suppressed_'), names_to = 'topic',
                 names_prefix = 'suppressed_', values_to = 'suppressed')
  raw %>%
    tall_simple(epic_injury_measures, keys) %>%
    mutate(topic = sub('^epic_(n|rate)_ed_', '', measure)) %>%
    left_join(suppressed, by = c(keys, 'topic')) %>%
    select(-topic) %>%
    rename(fips = geography) %>%
    filter(!is.na(value)) %>%
    as_state() %>%
    label('epic_injury', 'Epic Cosmos')
}

epic_injury_tall('yearly_injury.csv.gz') %>%
  write_parquet('dist/epic_injury_state_age_year.parquet')
epic_injury_tall('monthly_injury.csv.gz') %>%
  write_parquet('dist/epic_injury_state_age_month.parquet')


# =============================================================================
# 5. Epic Chronic -- obesity and diabetes, state + county, age
#
#   dist/epic_chronic_state_age.parquet
#   dist/epic_chronic_county_age.parquet
#
# Restricted to '<18 Years'. The source's next band up, '18-24 Years', begins
# at 18 and is entirely adult, so it is out of scope. Suppression flags are
# named suppressed_<measure> at source and are renamed to <measure>_suppressed
# so they pivot alongside their measure.
# =============================================================================

epic_chronic_measures <- SPEC %>% filter(dataset == 'epic_chronic') %>% pull(measure) %>% unique()

epic_chronic_tall <- function(file) {
  vroom::vroom(file.path('../epic_chronic/standard', file), show_col_types = FALSE) %>%
    filter(age %in% names(AGE_EPIC_C)) %>%
    mutate(age = unname(AGE_EPIC_C[age])) %>%
    rename_with(~ paste0(sub('^suppressed_', '', .x), '_suppressed'),
                any_of(paste0('suppressed_', epic_chronic_measures))) %>%
    tall_flagged(epic_chronic_measures, c('geography', 'time', 'age'), '_suppressed') %>%
    rename(fips = geography) %>%
    filter(!is.na(value))
}

epic_chronic_tall('state_year.csv.gz') %>%
  as_state() %>%
  label('epic_chronic', 'Epic Cosmos') %>%
  write_parquet('dist/epic_chronic_state_age.parquet')

epic_chronic_tall('county_year.csv.gz') %>%
  inner_join(county_cw, by = 'fips') %>%
  rename(geography = geography_name) %>%
  label('epic_chronic', 'Epic Cosmos') %>%
  write_parquet('dist/epic_chronic_county_age.parquet')


# =============================================================================
# 6. medicaid_quality -- Child Core Set quality measures, state, payer
#
#   dist/medicaid_state_payer.parquet
#
# This source carries no demographic stratification at all: age, sex and
# race_ethnicity are 'Total' on every row. Its real dimensions are payer
# (Medicaid / CHIP / Total) and domain. The requested measures are all Child
# Core Set (_ch_) rates, so the population is children by construction; age is
# set to 'Not stratified'.
#
# Each rate has 25th and 75th percentile companions (medicaid_<stem>_pct_25 /
# _pct_75) describing the spread across states; they are carried as pct_25 /
# pct_75. Geography arrives as state names, and the source uses both
# 'Dist. of Col.' and 'District of Columbia' -- these are folded together.
# Puerto Rico is dropped for consistency with the other datasets.
# =============================================================================

medicaid_measures <- SPEC %>% filter(dataset == 'medicaid') %>% pull(measure) %>% unique()
medicaid_stems <- sub('_rate$', '', medicaid_measures)

vroom::vroom('../medicaid_quality/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(geography_level == 's') %>%
  mutate(geography = if_else(geography == 'Dist. of Col.', 'District of Columbia', geography)) %>%
  rename_with(~ sub('_rate$', '_value', .x), all_of(medicaid_measures)) %>%
  select(geography, time, payer, domain,
         all_of(intersect(as.vector(outer(medicaid_stems,
                                          c('_value', '_pct_25', '_pct_75'), paste0)),
                          names(.)))) %>%
  pivot_longer(
    cols          = -c(geography, time, payer, domain),
    names_to      = c('measure', '.value'),
    names_pattern = sprintf('^(%s)_(value|pct_25|pct_75)$',
                            paste(medicaid_stems[order(-nchar(medicaid_stems))], collapse = '|'))
  ) %>%
  mutate(measure = paste0(measure, '_rate'), age = 'Not stratified') %>%
  filter(!is.na(value)) %>%
  inner_join(state_cw %>% select(fips, geography = geography_name), by = 'geography') %>%
  label('medicaid', 'Medicaid/CHIP Child Core Set') %>%
  write_parquet('dist/medicaid_state_payer.parquet')


# =============================================================================
# 7. County Health Rankings -- state + county, no demographic stratification
#
#   dist/chr_state.parquet
#   dist/chr_county.parquet
#
# CHR has no age, sex or race dimension, so age is 'Not stratified'. Some
# measures cover children by definition (children in poverty, teen births,
# uninsured children) even though the file is not stratified.
#
# UNITS ARE NOT UNIFORM across CHR measures: some are proportions on a 0-1
# scale (children_in_poverty ~0.07-0.34, uninsured_children ~0.01-0.18), some
# are rates per 100,000 (teen_births, violent_crime, firearm_fatalities), some
# are dollar amounts that can be negative (school_funding_adequacy), and some
# are provider-to-population ratios (~0.001). Read the unit per measure rather
# than assuming a common scale.
#
# chr_primary_care_physicians additionally mixes units WITHIN the column: most
# values are ratios around 0.001 but a handful are per-100,000 rates up to
# ~214. That is an upstream inconsistency, left as-is here.
#
# Two requested measures, chr_lead_poisoned_children and
# chr_contaminants_in_municipal_water_wi, are present as columns but completely
# empty at source (0 non-missing values in either the state or county file), so
# they produce no rows.
# =============================================================================

chr_measures <- SPEC %>% filter(dataset == 'chr') %>% pull(measure) %>% unique()

# Read every column as character so sparse measure columns aren't mistyped;
# tall_simple coerces value to numeric.
chr_read <- function(file) {
  vroom::vroom(file.path('../county_health_rankings/standard', file),
               col_types = vroom::cols(.default = 'c'), show_col_types = FALSE) %>%
    tall_simple(chr_measures, c('geography', 'time')) %>%
    mutate(time = as.Date(time), age = 'Not stratified') %>%
    filter(!is.na(value))
}

chr_read('data_state.csv.gz') %>%
  rename(fips = geography) %>%
  as_state() %>%
  label('chr', 'County Health Rankings') %>%
  write_parquet('dist/chr_state.parquet')

chr_read('data_county.csv.gz') %>%
  mutate(fips = formatC(as.integer(geography), width = 5, flag = '0')) %>%
  select(-geography) %>%
  inner_join(county_cw, by = 'fips') %>%
  rename(geography = geography_name) %>%
  label('chr', 'County Health Rankings') %>%
  write_parquet('dist/chr_county.parquet')


# =============================================================================
# 8. NOAA heat risk -- daily HeatRisk index, state + county
#
#   dist/noaa_heat_risk_state.parquet
#   dist/noaa_heat_risk_county.parquet
#
# Daily data, kept at full resolution. forecast_day 0 is the observed/current
# day and 1-5 are forecast days. low_coverage_flag exists only at county level
# and is NA for most rows. No age dimension, so age is 'Not stratified'. The
# state file covers the 50 states only -- no DC and no national row.
# =============================================================================

noaa_read <- function(file) {
  vroom::vroom(file.path('../noaa_heat_risk/standard', file), show_col_types = FALSE) %>%
    rename(fips = geography) %>%
    mutate(measure = 'heat_risk', age = 'Not stratified') %>%
    filter(!is.na(value))
}

noaa_read('data_state.csv.gz') %>%
  as_state() %>%
  label('noaa_heat_risk', 'NOAA/NWS HeatRisk') %>%
  write_parquet('dist/noaa_heat_risk_state.parquet')

noaa_read('data_county.csv.gz') %>%
  inner_join(county_cw, by = 'fips') %>%
  rename(geography = geography_name) %>%
  label('noaa_heat_risk', 'NOAA/NWS HeatRisk') %>%
  write_parquet('dist/noaa_heat_risk_county.parquet')


# =============================================================================
# 9. Childhood immunizations -- state, age x vaccine
#
#   dist/immunizations_state_age_vaccine.parquet
#
# Sourced from the childhood immunization bundle's combined dist file, which
# already harmonizes CDC NIS and CDC SchoolVaxView. `vaccine` is a dimension
# rather than a measure, so measure is the single value 'pct_vaccinated' and
# the vaccine sits in its own column. `source` is passed through from the file
# (CDC NIS / CDC SchoolVaxView) rather than overwritten. Every age level is a
# young child (0-1 Days through 5 years), so no age filter is needed.
#
# KNOWN UPSTREAM DUPLICATES: the immunization bundle emits 11 geography/year/
# vaccine/age/source groups twice with different values, all of them Varicella
# at age '5 years' from SchoolVaxView (e.g. Hawaii 2019 = 91.8 and 70.2). They
# are carried through rather than arbitrarily deduplicated -- picking a winner
# here would hide the upstream problem. 16 rows are affected.
# =============================================================================

read_parquet('../bundle_childhood_immunizations/dist/overall_rates_by_source.parquet') %>%
  transmute(
    geography, vaccine, age,
    time  = as.Date(paste0(year, '-12-31')),
    value = as.numeric(value),
    lcl   = as.numeric(value_lcl),
    ucl   = as.numeric(value_ucl),
    sample_size, percent_surveyed, survey_type,
    measure = 'pct_vaccinated',
    source
  ) %>%
  filter(!is.na(value), !is.na(time)) %>%
  inner_join(state_cw %>% select(fips, geography = geography_name), by = 'geography') %>%
  inner_join(SPEC %>% filter(dataset == 'immunizations') %>% select(section, focus_area, measure),
             by = 'measure', relationship = 'many-to-many') %>%
  mutate(year = as.integer(lubridate::year(time))) %>%
  relocate(any_of(COL_ORDER)) %>%
  arrange(section, focus_area, vaccine, fips, time, age) %>%
  write_parquet('dist/immunizations_state_age_vaccine.parquet')


# =============================================================================
# 10. NEISS -- ED injuries under 20, national, age x sex x year
#
#   dist/neiss_product_age_sex_year.parquet
#   dist/neiss_diagnosis_age_sex_year.parquet
#
# NEISS is a national probability sample of ~100 EDs, so there is one geography
# ('United States') and no state breakdown. The source files are stratified by
# age x sex x race x hispanic; race and ethnicity are summed away here, leaving
# age x sex x year.
#
# AGE LEVELS ARE MUTUALLY EXCLUSIVE, so `age` can be summed freely:
#   '00 months' through '23 months' -- completed months of age, under 2
#   '2-4 years' / '5-9 years' / '10-14 years' / '15-19 years'
# NEISS reports two age schemes and the under-2s appear in both; the coarse
# 'Under 2 years' band is dropped in favour of the 24 month rows that partition
# it, so nothing is double counted.
#
# ONE MEASURE, so there is no `measure` column: `value` is always the weighted
# national estimate (the sum of NEISS survey weights).
#
# `n_sampled` is the raw count of sampled ED records behind that estimate -- a
# sample size, not a national figure -- and `unstable_flag` is 1 where it is
# below 20, the threshold under which CPSC considers a NEISS estimate unstable.
# Flagged values are real estimates, not suppressed cells, so they are kept
# as-is; treat them as indicative only. The flag fires on most of the
# single-month-of-age rows (some rest on one record) and hardly ever on the wider
# year bands, so anything plotting the month rows should either filter on
# unstable_flag == 0 or mark them.
#
# TOP 10, PER CELL: the 10 categories are chosen independently within each
# age x sex x year cell, ranked on that cell's weighted estimate,
# so THE SET OF CATEGORIES VARIES FROM CELL TO CELL. Comparing one category
# across cells requires care: its absence from a cell means it was outside that
# cell's top 10, NOT that it was zero. `rank` (1 = most injuries) is the
# within-cell ordering; ties are broken by category name so the build is
# reproducible. Categories with no sampled records in a cell are dropped rather
# than ranked, so a sparse cell can carry fewer than 10.
#
# RANK FOLLOWS `value`, NOT `n_sampled`, and the two orderings can disagree,
# because NEISS weights vary by ED size stratum: large
# and children's hospitals are sampled at higher rates and so carry smaller
# weights. A category concentrated in small community EDs can therefore outrank
# one with more sampled records (10-14 years, all sexes, 2019: strain/sprain is
# 7,678 records -> 243,444 estimated, fracture 8,772 -> 228,631, so
# strain/sprain ranks first). The weighted estimate is the national figure, so it
# is the right thing to rank on; the top-ranked category would differ in 44 of
# 650 cells if ranked on the sample count instead.
#
# The catch-all buckets ('other/unspecified' products, 'other or not stated'
# diagnoses) are excluded from the ranking -- they are large but say nothing
# about what caused the injury -- so the 10 categories are all substantive.
#
# `pct_of_all` is `value` as a percent of ALL injuries in the same cell, on the
# weighted scale throughout. The denominator is the summed weighted estimate over
# every category, catch-all included, which is the cell's total ED-treated
# injuries -- so it answers "what share of this group's ED injuries came from X".
# The 10 retained percentages therefore sum to well under 100: that gap is the
# catch-all plus whatever fell outside the top 10.
#
# Sex is Male / Female / Unknown as reported, plus an 'All' total that includes
# Unknown.
# =============================================================================

# Under-20 age groups only; 'Unknown' and every adult band are dropped, as is
# 'Under 2' -- the infant file partitions it into single months. The infant file
# needs no age filter -- it is entirely under 2.
NEISS_AGE_GROUP <- c('2-4'   = '2-4 years',   '5-9'   = '5-9 years',
                     '10-14' = '10-14 years', '15-19' = '15-19 years')

# Catch-all buckets, excluded from the top-10 ranking (see header).
NEISS_CATCHALL <- c('other_unspecified', 'other_or_not_stated')

# Slugged category -> display label. The product groups are the NEISS code-band
# names from neiss/ingest.R; the diagnoses are the hadley/neiss code table.
# Anything not listed (e.g. a category CPSC adds later) falls back to a
# de-slugged label rather than failing the build.
NEISS_PRODUCT_LABEL <- c(
  general_household_appliances           = 'General household appliances',
  kitchen_appliances                     = 'Kitchen appliances',
  heating_cooling_ventilation            = 'Heating, cooling & ventilation',
  housewares                             = 'Housewares',
  home_communication_entertainment_hobby = 'Home communication, entertainment & hobby',
  home_furnishings_fixtures              = 'Home furnishings & fixtures',
  home_structures_construction_materials = 'Home structures & construction materials',
  home_workshop_equipment_tools          = 'Home workshop equipment & tools',
  chemicals                              = 'Chemicals',
  packaging_containers                   = 'Packaging & containers',
  sports_recreation_equipment_toys       = 'Sports/recreation equipment & toys',
  yard_garden_equipment                  = 'Yard & garden equipment',
  child_nursery_equipment                = 'Child nursery equipment',
  personal_use_drugs_misc                = 'Personal use, drugs & misc.',
  sports_recreation_activities           = 'Sports & recreation activities',
  other_unspecified                      = 'Other/unspecified'
)

NEISS_DIAGNOSIS_LABEL <- c(
  amputation         = 'Amputation',            anoxia          = 'Anoxia',
  aspirated_object   = 'Aspirated object',      avulsion        = 'Avulsion',
  burns_chemical     = 'Burns, chemical',       burns_elec      = 'Burns, electrical',
  burns_not_spec     = 'Burns, not specified',  burns_radiation = 'Burns, radiation',
  burns_scald        = 'Burns, scald',          burns_thermal   = 'Burns, thermal',
  concussion         = 'Concussion',            crushing        = 'Crushing',
  dental_injury      = 'Dental injury',         dislocation     = 'Dislocation',
  electric_shock     = 'Electric shock',        foreign_body    = 'Foreign body',
  fracture           = 'Fracture',              hematoma        = 'Hematoma',
  hemorrhage         = 'Hemorrhage',            laceration      = 'Laceration',
  ingested_object    = 'Ingested object',       nerve_damage    = 'Nerve damage',
  poisoning          = 'Poisoning',             puncture        = 'Puncture',
  submersion         = 'Submersion',            strain_sprain   = 'Strain or sprain',
  contusion_or_abrasion = 'Contusion or abrasion',
  dermat_or_conj        = 'Dermatitis or conjunctivitis',
  inter_organ_injury    = 'Internal organ injury',
  other_or_not_stated   = 'Other or not stated'
)

deslug <- function(x) sub('^(.)', '\\U\\1', gsub('_', ' ', x), perl = TRUE)

# Read one wide NEISS count file, sum race x hispanic away, and go tall on the
# breakdown category. neiss_n_<cat> / neiss_wt_<cat> become columns n / wt.
neiss_read <- function(file, scheme) {
  d <- vroom::vroom(file.path('../neiss/standard', file), show_col_types = FALSE)
  if (scheme == 'Age group') {
    d <- d %>%
      filter(age %in% names(NEISS_AGE_GROUP)) %>%
      mutate(age = unname(NEISS_AGE_GROUP[age]))
  }
  d <- d %>%
    group_by(time, age, sex) %>%
    summarize(across(starts_with('neiss_'), ~ sum(.x, na.rm = TRUE)), .groups = 'drop') %>%
    pivot_longer(starts_with('neiss_'), names_to = c('.value', 'category'),
                 names_pattern = '^neiss_(n|wt)_(.+)$')
  bind_rows(
    d,
    d %>%
      group_by(time, age, category) %>%
      summarize(n = sum(n), wt = sum(wt), .groups = 'drop') %>%
      mutate(sex = 'All')
  )
}

NEISS_CELL <- c('age', 'sex', 'time')

# Youngest to oldest. `age` is left as character, so ordering is applied at
# arrange() time rather than by making it a factor (which arrow would write as a
# dictionary-encoded column).
NEISS_AGE_LEVELS <- c(sprintf('%02d months', 0:23),
                      '2-4 years', '5-9 years', '10-14 years', '15-19 years')

# Share of the cell total, as a percent. The denominator is every category in
# the cell -- catch-all included -- and is guarded against an all-zero cell.
pct_of <- function(x) if (sum(x) > 0) round(100 * x / sum(x), 2) else NA_real_

neiss_build <- function(agegroup_file, infant_file, dataset, cat_col, labels) {
  d <- bind_rows(neiss_read(agegroup_file, 'Age group'),
                 neiss_read(infant_file,   'Age in months'))
  stopifnot(all(d$age %in% NEISS_AGE_LEVELS))
  d %>%
    # Percents first, while the catch-all bucket is still present.
    group_by(across(all_of(NEISS_CELL))) %>%
    mutate(pct_of_all = pct_of(wt)) %>%
    ungroup() %>%
    # Then rank the substantive categories within each cell and keep the top 10.
    filter(!category %in% NEISS_CATCHALL, n > 0) %>%
    group_by(across(all_of(NEISS_CELL))) %>%
    arrange(desc(wt), category, .by_group = TRUE) %>%
    mutate(rank = row_number()) %>%
    ungroup() %>%
    filter(rank <= 10) %>%
    mutate(!!cat_col := coalesce(unname(labels[category]), deslug(category))) %>%
    select(-category) %>%
    # One measure, so no `measure` column in the output; it is carried only long
    # enough for label() to join section / focus_area on, then dropped.
    mutate(value         = as.numeric(wt),
           n_sampled     = n,
           unstable_flag = as.integer(n < 20),
           time          = as.Date(time),
           fips          = '00',
           measure       = 'neiss_injuries_weighted') %>%
    select(-n, -wt) %>%
    as_state() %>%
    label(dataset, 'CPSC NEISS') %>%
    select(-measure) %>%
    arrange(match(age, NEISS_AGE_LEVELS), sex, time, rank)
}

neiss_build('data_agegroup_product.csv.gz', 'data_infant_product.csv.gz',
            'neiss_product', 'product', NEISS_PRODUCT_LABEL) %>%
  write_parquet('dist/neiss_product_age_sex_year.parquet')

neiss_build('data_agegroup_diagnosis.csv.gz', 'data_infant_diagnosis.csv.gz',
            'neiss_diagnosis', 'diagnosis', NEISS_DIAGNOSIS_LABEL) %>%
  write_parquet('dist/neiss_diagnosis_age_sex_year.parquet')


# =============================================================================
# 11. MAIN PAGE -- PLACEHOLDER
# =============================================================================
# TODO: to be defined; see the 'Main page' tab of the workbook.
# Expected output: dist/main_*.parquet
