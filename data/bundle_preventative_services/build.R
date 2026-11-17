library(tidyverse)
library(arrow)

# read data from data source projects and write  `dist` directory

all_fips <- vroom::vroom('../../resources/all_fips.csv.gz')
## Medicaid preventative services
# Adult Core Set file: long, one value per state-year-measure taken from the
# reported population closest to non-dual Medicaid (population_match flags how
# close). Geography is a FIPS code; state names are joined on for this bundle.
vroom::vroom('../medicaid_quality/standard/data_adult_core_set.csv.gz',
             col_types = vroom::cols(geography = "c", time = "c"),
             show_col_types = FALSE) %>%
  filter(measure %in% c("medicaid_fva_ad", "medicaid_chl_ad", "medicaid_ha1c_ad",
                        "medicaid_amm_ad", "medicaid_cbp_ad")) %>%
  left_join(all_fips %>% filter(nchar(geography) == 2) %>%
              dplyr::select(geography, state_name = geography_name),
            by = "geography") %>%
  mutate(
    geography    = state_name,
    outcome_name = case_when(
      measure == "medicaid_fva_ad"  ~ "Influenza Vaccine",
      measure == "medicaid_chl_ad"  ~ "Chlamydia Screening",
      # HA1C, AMM and CBP measure care for people already diagnosed, not
      # screening, so they are named for what they count and do not pair
      # with the Medicare screening rows in the combined table below.
      measure == "medicaid_ha1c_ad" ~ "HbA1c Testing in Adults with Diabetes",
      measure == "medicaid_amm_ad"  ~ "Antidepressant Medication Management",
      measure == "medicaid_cbp_ad"  ~ "Controlling High Blood Pressure"
    ),
    year = lubridate::year(time),
    age = "Total", sex = "Total", race_ethnicity = "Total",
    source = 'Medicaid'
  ) %>%
  filter(!is.na(value),
         geography %in% c(state.name, "District of Columbia")) %>%
  dplyr::select(geography, year, age, sex, race_ethnicity, population_match, outcome_name, source, value) %>%
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