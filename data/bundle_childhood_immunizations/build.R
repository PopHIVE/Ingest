library(tidyverse)
library(arrow)
# read data from data source projects
# and write to this project's `dist` directory

all_fips <-vroom::vroom('../../resources/all_fips.csv.gz')

vroom::vroom('../schoolvaxview/standard/data.csv.gz')%>%
  arrow::write_parquet( "dist/schoolvaxview_overall.parquet")

vroom::vroom('../schoolvaxview/standard/data_exemptions.csv.gz') %>%
  arrow::write_parquet( "dist/schoolvaxview_exemptions.parquet")

vroom::vroom('../nis/standard/data.csv.gz') %>%
    rename( pct_uptake = vax_uptake_overall,
            pct_uptake_lcl = vax_uptake_overall_lcl,
            pct_uptake_ucl = vax_uptake_overall_ucl,
            sample_size = sample_size_overall
            ) %>%
    arrow::write_parquet( "dist/nis_overall.parquet")

vroom::vroom('../nis/standard/data_urban.csv.gz') %>%
  rename( pct_uptake = vax_uptake_urban,
          pct_uptake_lcl = vax_uptake_urban_lcl,
          pct_uptake_ucl = vax_uptake_urban_ucl,
          sample_size = sample_size_urban
  ) %>%
    arrow::write_parquet( "dist/nis_urban.parquet")

nis_insurance <- vroom::vroom('../nis/standard/data_insurance.csv.gz') %>%
  rename( pct_uptake = vax_uptake_insurance,
          pct_uptake_lcl = vax_uptake_insurance_lcl,
          pct_uptake_ucl = vax_uptake_insurance_ucl,
          sample_size = sample_size_insurance
  ) %>%
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
  mutate(value_vaxview = as.numeric(value_vaxview))

nis <- vroom::vroom('../nis/standard/data.csv.gz'
        ) %>%
  filter(
    vaccine == '≥1 Dose MMR' & age == "35 Months" & birth_year == 2021
  ) %>%
  rename(value_nis = vax_uptake_overall,
         value_nis_lcl=vax_uptake_overall_lcl,
         value_nis_ucl=vax_uptake_overall_ucl) %>%
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

#compare nis and vaxview by year
vaxview2 <- vroom::vroom('../schoolvaxview/standard/data.csv.gz') %>%
  filter(value!='NReq') %>%
  mutate(age = '5 years',
         vaccine = if_else(vax== 'polio', 'Polio',
                           if_else(vax== 'dtap', 'DTaP',
                                   if_else(vax== 'varicella', 'Varicella',
                                           if_else(vax== 'hep_b', 'Hep B', 
                                                   if_else(vax== 'mmr', 'MMR', 
                                                    if_else(vax=='full_exempt', 'Full Exemption',
                                                            if_else(vax=='personal_exempt', 'Personal Exemption',
                                                                    if_else(vax=='medical_exempt', 'Medical Exemption',
                                                           NA_character_)))))))),
         #  time = paste(substr(year,1,4),'09','01', sep='-') #set date to start of academic year (Sept 1,YYYY)
         year = substr(time,1,4),
         value = as.numeric(value)
  ) %>%
  rename(sample_size = N) %>%
  dplyr::select(year, geography, age, vaccine, value, sample_size, percent_surveyed, survey_type) %>%
  mutate(source = 'CDC SchoolVaxView') %>%
  filter(year>=2016 & !is.na(vaccine) & !grepl('Exempt',vaccine) )%>%
  distinct() 


nis2 <- vroom::vroom('../nis/standard/data.csv.gz') %>%
   mutate( age_months = if_else(grepl('Month', age), as.numeric(gsub("\\D", "", age)),
                               if_else(grepl('Day',age),0, 
                                       NA_real_)),
          age_days = age_months * (365/12) +2, 
          time= as.Date(paste(birth_year,'01','01', sep='-')) + age_days,
          year= as.character(year(time))
  ) %>%
  rename(value =vax_uptake_overall,
         value_lcl = vax_uptake_overall_lcl,
         value_ucl = vax_uptake_overall_ucl,
         sample_size = sample_size_overall) %>%
  dplyr::select(year, geography,vaccine, age,value, value_lcl, value_ucl, sample_size )%>%
  mutate(source = 'CDC NIS')


combo_school_NIS <- bind_rows(nis2, vaxview2) %>%
  left_join(all_fips, by='geography') %>%
  rename(fips=geography,
         geography = geography_name) %>%
  dplyr::select(-fips, -state ) %>%
  relocate(year, geography, vaccine)


write_parquet(combo_school_NIS, "./dist/overall_rates_by_source.parquet")

############################################################
# Washington Post school vaccination data - county level
wapo_counties <- vroom::vroom('../schoolvaxview/standard/data_wapo_counties.csv.gz')
arrow::write_parquet(wapo_counties, "dist/wapo_vax_counties.parquet")

# Washington Post school vaccination data - school level
wapo_schools <- vroom::vroom('../schoolvaxview/standard/data_wapo_schools.csv.gz')
arrow::write_parquet(wapo_schools, "dist/wapo_vax_schools.parquet")
