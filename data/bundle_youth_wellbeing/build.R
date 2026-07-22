library(tidyverse)
library(arrow)

# read data from Medicaid/ CMS and save to dist directory

all_fips <- vroom::vroom('../../resources/all_fips.csv.gz')
medicaid_data <- vroom::vroom('../medicaid_quality/standard/data.csv.gz')

## Medicaid youth wellbeing
medicaid_data %>%
  filter(geography_level == 's') %>%
  dplyr::select(geography, time, age, sex, race_ethnicity, payer,
                medicaid_awc_ch_rate,
                medicaid_dev_ch_rate,
                medicaid_wcc_ch_rate,
                medicaid_w15_ch_rate,
                medicaid_w34_ch_rate,
                medicaid_apc_ch_rate,
                medicaid_add_ch_30d_rate,
                medicaid_fum_ch_30d_rate,
                medicaid_fuh_ch_30d_rate) %>%
  pivot_longer(
    cols         = starts_with("medicaid_"),
    names_to     = "outcome_name",
    names_prefix = "medicaid_",
    values_to    = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "awc_ch_rate"      ~ "Adolescent Well-Care Visits",
      outcome_name == "dev_ch_rate"      ~ "Developmental Screening",
      outcome_name == "wcc_ch_rate"      ~ "Weight Assessment for Children",
      outcome_name == "w15_ch_rate"      ~ "Well-Child Visits (First 15 Months)",
      outcome_name == "w34_ch_rate"      ~ "Well-Child Visits (First 30 Months)",
      outcome_name == "apc_ch_rate"      ~ "Children's Access to Primary Care",
      outcome_name == "add_ch_30d_rate"  ~ "ADHD Medication Management",
      outcome_name == "fum_ch_30d_rate"  ~ "Follow-Up After ED Visit for Mental Illness",
      outcome_name == "fuh_ch_30d_rate"  ~ "Follow-Up After Hospitalization for Mental Illness"
    ),
    year   = lubridate::year(time),
    source = 'Medicaid'
  ) %>%
  filter(!is.na(value),
         geography %in% c(state.name, "District of Columbia")) %>%
  dplyr::select(geography, year, age, sex, race_ethnicity, payer, outcome_name, source, value) %>%
  arrow::write_parquet('dist/medicaid_youth_wellbeing.parquet')


## CMS state
cms_state <- vroom::vroom('../cms_mmd/standard/data_state_county_age.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age,
                cms_adhd,
                cms_anxiety,
                cms_depression,
                cms_depressive_disorder) %>%
  rename(fips = geography) %>%
  left_join(all_fips, by = c('fips' = 'geography')) %>%
  rename(geography = geography_name) %>%
  mutate(
    geography = if_else(fips == '00', 'United States', geography),
    year      = lubridate::year(time),
    source    = 'Medicare FFS',
    age       = if_else(age == '≥65 Years', '65+ Years', age),
    age       = if_else(age == 'All_Ages',  'Total',     age)
  ) %>%
  pivot_longer(
    cols      = starts_with("cms_"),
    names_to  = "outcome_name",
    names_prefix = "cms_",
    values_to = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "adhd"                ~ "ADHD",
      outcome_name == "anxiety"             ~ "Anxiety",
      outcome_name == "depression"          ~ "Depression",
      outcome_name == "depressive_disorder" ~ "Depressive Disorder",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, outcome_name, source, value) %>%
  filter(geography %in% c('United States', 'District of Columbia', state.name)) %>%
  filter(fips != '52')

arrow::write_parquet(cms_state, 'dist/cms_youth_wellbeing_state.parquet')


## CMS by sex
vroom::vroom('../cms_mmd/standard/data_state_county_age_by_sex.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age, sex,
                cms_adhd,
                cms_anxiety,
                cms_depression,
                cms_depressive_disorder) %>%
  rename(fips = geography) %>%
  left_join(all_fips, by = c('fips' = 'geography')) %>%
  rename(geography = geography_name) %>%
  mutate(
    geography = if_else(fips == '00', 'United States', geography),
    year      = lubridate::year(time),
    source    = 'Medicare FFS',
    age       = if_else(age == '≥65 Years', '65+ Years', age),
    age       = if_else(age == 'All_Ages',  'Total',     age)
  ) %>%
  pivot_longer(
    cols      = starts_with("cms_"),
    names_to  = "outcome_name",
    names_prefix = "cms_",
    values_to = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "adhd"                ~ "ADHD",
      outcome_name == "anxiety"             ~ "Anxiety",
      outcome_name == "depression"          ~ "Depression",
      outcome_name == "depressive_disorder" ~ "Depressive Disorder",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, sex, outcome_name, source, value) %>%
  filter(!is.na(value),
         geography %in% c('United States', 'District of Columbia', state.name),
         fips != '52') %>%
  arrow::write_parquet('dist/cms_youth_wellbeing_by_sex.parquet')


## CMS by race
vroom::vroom('../cms_mmd/standard/data_state_county_age_by_race.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age, race_ethnicity,
                cms_adhd,
                cms_anxiety,
                cms_depression,
                cms_depressive_disorder) %>%
  rename(fips = geography) %>%
  left_join(all_fips, by = c('fips' = 'geography')) %>%
  rename(geography = geography_name) %>%
  mutate(
    geography = if_else(fips == '00', 'United States', geography),
    year      = lubridate::year(time),
    source    = 'Medicare FFS',
    age       = if_else(age == '≥65 Years', '65+ Years', age),
    age       = if_else(age == 'All_Ages',  'Total',     age)
  ) %>%
  pivot_longer(
    cols      = starts_with("cms_"),
    names_to  = "outcome_name",
    names_prefix = "cms_",
    values_to = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "adhd"                ~ "ADHD",
      outcome_name == "anxiety"             ~ "Anxiety",
      outcome_name == "depression"          ~ "Depression",
      outcome_name == "depressive_disorder" ~ "Depressive Disorder",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, race_ethnicity, outcome_name, source, value) %>%
  filter(!is.na(value),
         geography %in% c('United States', 'District of Columbia', state.name),
         fips != '52') %>%
  arrow::write_parquet('dist/cms_youth_wellbeing_by_race.parquet')


## County Health Rankings -- social determinants of health (state + county)
# Read all columns as character so sparse measure columns aren't mistyped;
# numeric coercion happens in pivot_chr().
chr_state  <- vroom::vroom('../county_health_rankings/standard/data_state.csv.gz',
                            col_types = vroom::cols(.default = "c"), show_col_types = FALSE)
chr_county <- vroom::vroom('../county_health_rankings/standard/data_county.csv.gz',
                            col_types = vroom::cols(.default = "c"), show_col_types = FALSE)

CHR_MEASURES <- c(
  # environmental_health
  chr_drinking_water_violations        = "drinking_water_violations",
  chr_air_pollution_particulate_matter = "air_pollution_particulate_matter",
  chr_adverse_climate_events           = "adverse_climate_events",
  # nutrition_and_exercise
  chr_food_insecurity                  = "food_insecurity",
  chr_limited_access_to_healthy_foods  = "limited_access_to_healthy_foods",
  chr_food_environment_index           = "food_environment_index",
  chr_access_to_exercise_opportunities = "access_to_exercise_opportunities",
  chr_access_to_parks                  = "access_to_parks",
  # preventative_health
  chr_uninsured_children = "uninsured_children",
  # demographic
  chr_children_in_poverty                               = "children_in_poverty",
  chr_children_eligible_for_free_or_reduced_price_lunch  = "free_reduced_lunch_eligible",
  chr_children_in_single_parent_households               = "single_parent_households",
  chr_disconnected_youth                                  = "disconnected_youth",
  chr_child_care_cost_burden                              = "child_care_cost_burden",
  chr_child_care_centers                                  = "child_care_centers",
  chr_high_school_graduation                              = "high_school_graduation",
  chr_high_school_completion                              = "high_school_completion",
  chr_reading_scores                                      = "reading_scores",
  chr_math_scores                                         = "math_scores",
  chr_school_funding_adequacy                             = "school_funding_adequacy",
  chr_school_segregation                                  = "school_segregation",
  chr_severe_housing_problems                             = "severe_housing_problems",
  chr_severe_housing_cost_burden                          = "severe_housing_cost_burden"
)

# Pivot the wide CHR measure columns to tall (geography, time, measure, value).
pivot_chr <- function(df, mapping) {
  present <- intersect(names(mapping), colnames(df))
  df %>%
    dplyr::select(geography, time, all_of(present)) %>%
    rename(!!!setNames(present, unname(mapping[present]))) %>%
    pivot_longer(
      cols      = all_of(unname(mapping[present])),
      names_to  = "measure",
      values_to = "value"
    ) %>%
    mutate(value = suppressWarnings(as.numeric(value))) %>%
    filter(!is.na(value))
}

pivot_chr(chr_state, CHR_MEASURES) %>%
  mutate(time = as.Date(time), source = 'County Health Rankings') %>%
  dplyr::select(geography, time, measure, source, value) %>%
  arrange(measure, geography, time) %>%
  arrow::write_parquet('dist/chr_youth_wellbeing_state.parquet')

pivot_chr(chr_county, CHR_MEASURES) %>%
  mutate(
    geography = formatC(as.integer(geography), width = 5, flag = "0"),
    time      = as.Date(time),
    source    = 'County Health Rankings'
  ) %>%
  dplyr::select(geography, time, measure, source, value) %>%
  arrange(measure, geography, time) %>%
  arrow::write_parquet('dist/chr_youth_wellbeing_county.parquet')