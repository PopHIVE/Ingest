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