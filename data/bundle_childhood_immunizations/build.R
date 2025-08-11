library(tidyverse)
library(arrow)
# read data from data source projects
# and write to this project's `dist` directory

vroom::vroom('../schoolvaxview/standard/data.csv.gz')%>%
  arrow::write_parquet( "dist/schoolvaxview_overall.parquet")

vroom::vroom('../schoolvaxview/standard/data_exemptions.csv.gz') %>%
  arrow::write_parquet( "dist/schoolvaxview_exemptions.parquet")

vroom::vroom('../nis/standard/data.csv.gz') %>%
    arrow::write_parquet( "dist/nis_overall.parquet")

vroom::vroom('../nis/standard/data_urban.csv.gz') %>%
    arrow::write_parquet( "dist/nis_urban.parquet")

nis_insurance <- vroom::vroom('../nis/standard/data_insurance.csv.gz') %>%
    arrow::write_parquet( "dist/nis_insurance.parquet")

epic <- vroom::vroom('../epic/standard/children.csv.gz') %>%
  rename(N_epic = n_vaccine_mmr,
         mmr_pct_epic = mmr_receipt)

arrow::write_parquet( epic, "dist/mmr_rates_epic.parquet")

############################################################
#Compare Epic Cosmos ( 1dose), NIS (1+ doses),and Epic (1+ dose)
vaxview <- vroom::vroom('../schoolvaxview/standard/data.csv.gz') %>%
  filter(grepl('mmr', vax) & time == '2023-09-01') %>%
  rename(value_vaxview = value,
         vaxview_survey_type = survey_type) %>%
  dplyr::select(value_vaxview, geography,vaxview_survey_type) %>%
  mutate(value_vaxview = as.numeric(value_vaxview),
         geography = sprintf("%02d", geography))

nis <- vroom::vroom('../nis/standard/data.csv.gz'
        ) %>%
  filter(
    vaccine == '≥1 Dose MMR' & age == "35 Months" & birth_year == 2021
  ) %>%
  rename(value_nis = pct_uptake,
         value_nis_lcl=pct_uptake_lcl,
         value_nis_ucl=pct_uptake_ucl) %>%
  dplyr::select(value_nis,value_nis_lcl,value_nis_ucl, geography)

vax_epic <- vroom::vroom('../epic/standard/children.csv.gz'
  ) %>%
  rename(value_epic = mmr_receipt,
         N_patients_epic = n_vaccine_mmr) %>%
  mutate(N_patients_epic = as.numeric(N_patients_epic)
  ) %>%
  filter(age == '3-4 Years') %>%
  dplyr::select(value_epic, geography,N_patients_epic)

vax_compare <- nis %>%
  full_join(vaxview, by = 'geography') %>%
  full_join(vax_epic, by = 'geography') %>%
  dplyr::select(geography, value_nis, value_nis_ucl,value_nis_lcl,value_vaxview, value_epic,vaxview_survey_type,N_patients_epic) %>%
  filter(geography!='NA' & !is.na(value_nis))
arrow::write_parquet(vax_compare, "dist/state_compare.parquet")
