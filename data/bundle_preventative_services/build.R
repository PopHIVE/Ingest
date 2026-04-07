library(tidyverse)
library(arrow)

# read data from data source projects and write  `dist` directory

all_fips <- vroom::vroom('../../resources/all_fips.csv.gz')
medicaid_data <- vroom::vroom('../medicaid_quality/standard/data.csv.gz')

## Medicaid preventative services
medicaid_data %>%
  filter(geography_level == 's') %>%
  dplyr::select(geography, time, age, sex, race_ethnicity,
                medicaid_fva_ad_rate,
                medicaid_chl_ad_rate,
                medicaid_ha1c_ad_rate,
                medicaid_amm_ad_rate,
                medicaid_cbp_ad_rate) %>%
  pivot_longer(
    cols         = starts_with("medicaid_"),
    names_to     = "outcome_name",
    names_prefix = "medicaid_",
    values_to    = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "fva_ad_rate" ~ "Influenza Vaccine",
      outcome_name == "chl_ad_rate" ~ "Chlamydia Screening",
      outcome_name == "ha1c_ad_rate" ~ "Diabetes Screening",
      outcome_name == "amm_ad_rate" ~ "Depression Screening",
      outcome_name == "cbp_ad_rate" ~ "Cardiovascular Disease Screening"
    ),
    year = lubridate::year(time),
    source = 'Medicaid'
  ) %>%
  filter(!is.na(value),
         geography %in% c(state.name, "District of Columbia")) %>%
  dplyr::select(geography, year, age, sex, race_ethnicity, outcome_name, source, value) %>%
  arrow::write_parquet('dist/medicaid_preventative_services.parquet')


## CMS state
cms_state <- vroom::vroom('../cms_mmd/standard/data_state_county_age.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age,
                cms_scrn_prvnt_influenza_vaccine,
                cms_scrn_prvnt_cardiovascular_disease,
                cms_scrn_prvnt_diabetes,
                cms_scrn_prvnt_depression,
                cms_scrn_prvnt_sti,
                cms_scrn_prvnt_pneumococcal_vaccine,
                cms_scrn_prvnt_annual_wellness,
                cms_scrn_prvnt_pelvic_exam) %>%
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
    cols         = starts_with("cms_scrn_prvnt_"),
    names_to     = "outcome_name",
    names_prefix = "cms_scrn_prvnt_",
    values_to    = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "influenza_vaccine"      ~ "Influenza Vaccine",
      outcome_name == "cardiovascular_disease" ~ "Cardiovascular Disease Screening",
      outcome_name == "diabetes"               ~ "Diabetes Screening",
      outcome_name == "depression"             ~ "Depression Screening",
      outcome_name == "sti"                    ~ "Chlamydia Screening",
      outcome_name == "pneumococcal_vaccine"   ~ "Pneumococcal Vaccine",
      outcome_name == "annual_wellness"        ~ "Annual Wellness Visit",
      outcome_name == "pelvic_exam"            ~ "Pelvic Exam",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, outcome_name, source, value) %>%
  filter(geography %in% c('United States', 'District of Columbia', state.name)) %>%
  filter(fips != '52')

arrow::write_parquet(cms_state, 'dist/cms_preventative_services_state.parquet')


## CMS by sex
vroom::vroom('../cms_mmd/standard/data_state_county_age_by_sex.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age, sex,
                cms_scrn_prvnt_influenza_vaccine,
                cms_scrn_prvnt_cardiovascular_disease,
                cms_scrn_prvnt_diabetes,
                cms_scrn_prvnt_depression,
                cms_scrn_prvnt_sti,
                cms_scrn_prvnt_pneumococcal_vaccine,
                cms_scrn_prvnt_annual_wellness,
                cms_scrn_prvnt_pelvic_exam) %>%
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
    cols         = starts_with("cms_scrn_prvnt_"),
    names_to     = "outcome_name",
    names_prefix = "cms_scrn_prvnt_",
    values_to    = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "influenza_vaccine"      ~ "Influenza Vaccine",
      outcome_name == "cardiovascular_disease" ~ "Cardiovascular Disease Screening",
      outcome_name == "diabetes"               ~ "Diabetes Screening",
      outcome_name == "depression"             ~ "Depression Screening",
      outcome_name == "sti"                    ~ "Chlamydia Screening",
      outcome_name == "pneumococcal_vaccine"   ~ "Pneumococcal Vaccine",
      outcome_name == "annual_wellness"        ~ "Annual Wellness Visit",
      outcome_name == "pelvic_exam"            ~ "Pelvic Exam",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, sex, outcome_name, source, value) %>%
  filter(!is.na(value),
         geography %in% c('United States', 'District of Columbia', state.name),
         fips != '52') %>%
  arrow::write_parquet('dist/cms_preventative_services_by_sex.parquet')


## CMS by race
vroom::vroom('../cms_mmd/standard/data_state_county_age_by_race.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age, race_ethnicity,
                cms_scrn_prvnt_influenza_vaccine,
                cms_scrn_prvnt_cardiovascular_disease,
                cms_scrn_prvnt_diabetes,
                cms_scrn_prvnt_depression,
                cms_scrn_prvnt_sti,
                cms_scrn_prvnt_pneumococcal_vaccine,
                cms_scrn_prvnt_annual_wellness,
                cms_scrn_prvnt_pelvic_exam) %>%
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
    cols         = starts_with("cms_scrn_prvnt_"),
    names_to     = "outcome_name",
    names_prefix = "cms_scrn_prvnt_",
    values_to    = "value"
  ) %>%
  mutate(
    outcome_name = case_when(
      outcome_name == "influenza_vaccine"      ~ "Influenza Vaccine",
      outcome_name == "cardiovascular_disease" ~ "Cardiovascular Disease Screening",
      outcome_name == "diabetes"               ~ "Diabetes Screening",
      outcome_name == "depression"             ~ "Depression Screening",
      outcome_name == "sti"                    ~ "Chlamydia Screening",
      outcome_name == "pneumococcal_vaccine"   ~ "Pneumococcal Vaccine",
      outcome_name == "annual_wellness"        ~ "Annual Wellness Visit",
      outcome_name == "pelvic_exam"            ~ "Pelvic Exam",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, race_ethnicity, outcome_name, source, value) %>%
  filter(!is.na(value),
         geography %in% c('United States', 'District of Columbia', state.name),
         fips != '52') %>%
  arrow::write_parquet('dist/cms_preventative_services_by_race.parquet')


## Combined Medicare + Medicaid
medicare_long <- cms_state %>%
  filter(age == "Total") %>%
  dplyr::select(geography, fips, year, outcome_name, value_medicare = value)

medicaid_long <- arrow::read_parquet('dist/medicaid_preventative_services.parquet') %>%
  group_by(geography, year, outcome_name) %>%
  summarise(value_medicaid = mean(value, na.rm = TRUE), .groups = "drop")

medicare_long %>%
  left_join(medicaid_long, by = c("geography", "year", "outcome_name")) %>%
  arrow::write_parquet('dist/combined_preventative_services.parquet')