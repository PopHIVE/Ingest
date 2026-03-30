library(tidyverse)
library(arrow)

all_fips = vroom::vroom('../../resources/all_fips.csv.gz')


## Medicaid cancer screening
medicaid_data <- vroom::vroom('../medicaid_quality/standard/data.csv.gz')

medicaid_cancer <- medicaid_data %>%
  filter(grepl("Breast Cancer Screening|Cervical Cancer Screening|Colorectal Cancer Screening",
               measure_name)) %>%
  mutate(
    outcome_name = case_when(
      grepl("Breast",     measure_name) ~ "Breast Cancer Screening",
      grepl("Cervical",   measure_name) ~ "Cervical Cancer Screening",
      grepl("Colorectal", measure_name) ~ "Colorectal Cancer Screening"
    ),
    value = as.numeric(value),
    year  = as.integer(time_year)
  ) %>%
  filter(state %in% c(state.name, "District of Columbia")) %>%
  dplyr::select(
    geography = state,
    year,
    outcome_name,
    measure_abbr,
    reporting_program,
    population,
    source,
    value,
    median,
    pct25,
    pct75,
    n_states
  )

write_parquet(medicaid_cancer, './dist/medicaid_cancer_screening.parquet')


## CMS state
cms_state <- vroom::vroom('../cms_mmd/standard/data_state_county_age.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age,
                cms_scrn_prvnt_mammogram,
                cms_scrn_prvnt_colorectal_cancer,
                cms_scrn_prvnt_pap_test,
                cms_scrn_prvnt_prostate_cancer) %>%
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
      outcome_name == "mammogram"         ~ "Breast Cancer Screening",
      outcome_name == "colorectal_cancer" ~ "Colorectal Cancer Screening",
      outcome_name == "pap_test"          ~ "Cervical Cancer Screening",
      outcome_name == "prostate_cancer"   ~ "Prostate Cancer Screening",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, outcome_name, source, value) %>%
  filter(geography %in% c('United States', 'District of Columbia', state.name)) %>%
  filter(fips != '52')

write_parquet(cms_state, './dist/cms_cancer_screening_state.parquet')


## CMS by sex
cms_sex <- vroom::vroom('../cms_mmd/standard/data_state_county_age_by_sex.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age, sex,
                cms_scrn_prvnt_mammogram,
                cms_scrn_prvnt_colorectal_cancer,
                cms_scrn_prvnt_pap_test,
                cms_scrn_prvnt_prostate_cancer) %>%
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
      outcome_name == "mammogram"         ~ "Breast Cancer Screening",
      outcome_name == "colorectal_cancer" ~ "Colorectal Cancer Screening",
      outcome_name == "pap_test"          ~ "Cervical Cancer Screening",
      outcome_name == "prostate_cancer"   ~ "Prostate Cancer Screening",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, sex, outcome_name, source, value) %>%
  filter(!is.na(value),
         geography %in% c('United States', 'District of Columbia', state.name),
         fips != '52')

write_parquet(cms_sex, './dist/cms_cancer_screening_by_sex.parquet')


## CMS by race
cms_race <- vroom::vroom('../cms_mmd/standard/data_state_county_age_by_race.csv.gz') %>%
  filter(geography_level %in% c('n', 's')) %>%
  dplyr::select(geography, time, age, race_ethnicity,
                cms_scrn_prvnt_mammogram,
                cms_scrn_prvnt_colorectal_cancer,
                cms_scrn_prvnt_pap_test,
                cms_scrn_prvnt_prostate_cancer) %>%
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
      outcome_name == "mammogram"         ~ "Breast Cancer Screening",
      outcome_name == "colorectal_cancer" ~ "Colorectal Cancer Screening",
      outcome_name == "pap_test"          ~ "Cervical Cancer Screening",
      outcome_name == "prostate_cancer"   ~ "Prostate Cancer Screening",
      TRUE ~ tools::toTitleCase(gsub("_", " ", outcome_name))
    )
  ) %>%
  dplyr::select(geography, fips, year, age, race_ethnicity, outcome_name, source, value) %>%
  filter(!is.na(value),
         geography %in% c('United States', 'District of Columbia', state.name),
         fips != '52')

write_parquet(cms_race, './dist/cms_cancer_screening_by_race.parquet')


## Combined Medicare + Medicaid
medicare_long <- cms_state %>%
  filter(outcome_name != "Prostate Cancer Screening",
         age == "Total") %>%
  dplyr::select(geography, fips, year, outcome_name, value_medicare = value)

medicaid_long <- medicaid_cancer %>%
  group_by(geography, year, outcome_name) %>%
  summarise(value_medicaid = mean(value, na.rm = TRUE), .groups = "drop")

combined_screening <- medicare_long %>%
  left_join(medicaid_long, by = c("geography", "year", "outcome_name")) %>%

write_parquet(combined_screening, './dist/combined_cancer_screening.parquet')