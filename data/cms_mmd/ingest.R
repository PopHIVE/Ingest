#Note raw data downloaded from the CMS MMD dashboard uinsg the scraper_chronic_behavioral.R and scraper_screening.R files
library(tidyverse)
library(arrow)


process <- dcf::dcf_process_record()

#check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "parquet",
  recursive = TRUE,
  full.names = TRUE
)))


if (!identical(process$raw_state, raw_state)) {
    data1 <- arrow::open_dataset('./raw/combined.parquet') %>%
      dplyr::select(fips, year, condition_name, prevalence_rate, age_label, geography, race_label, sex_label) %>%
      collect() %>%
      rename(geography_level = geography,
      race_ethnicity = race_label,
      sex = sex_label,
      age = age_label) %>%
      pivot_wider(., id_cols=c(fips, year, age, geography_level, race_ethnicity, sex), 
                  names_from=condition_name, values_from = prevalence_rate) %>%
      mutate(time = paste0('20',year,'-01-01'),
             fips = if_else(geography_level=='n', '00',fips)
      )%>%
      rename(geography = fips) %>%
      dplyr::select(-year) %>%
      relocate(geography, geography_level, time, age, race_ethnicity, sex) %>%
      rename_with(
        ~ paste0("cms_", .x),
        .cols = -c(geography, geography_level, time, age, race_ethnicity, sex)
      )
  
  data2 <- arrow::open_dataset('./raw/screening_combined.parquet') %>%
    dplyr::select(fips, year, condition_name, care_rate, age_label, geography, race_label, sex_label) %>%
    collect() %>%
    rename(geography_level = geography,
     race_ethnicity = race_label,
    sex = sex_label,
    age = age_label) %>%
    pivot_wider(., id_cols=c(fips, year, age, geography_level, race_ethnicity, sex), 
                names_from=condition_name, values_from = care_rate) %>%
    mutate(time = paste0('20',year,'-01-01'),
           fips = if_else(geography_level=='n', '00',fips)
    )%>%
    rename(geography = fips) %>%
    dplyr::select(-year) %>%
    relocate(geography, geography_level, time, age, race_ethnicity, sex) %>%
    rename_with(
      ~ paste0("cms_scrn_prvnt_", .x),
      .cols = -c(geography, geography_level, time, age, race_ethnicity, sex)
    )
  data <- data1 %>%
    full_join(data2, by=c('geography', 'geography_level', 'time','age', 'race_ethnicity', 'sex')) %>%
    mutate(age = gsub('_plus','+', age),
           age = gsub('All_Ages','Total', age),
           age = gsub('_to_','-', age),
           age = gsub('Under_','<', age),
           age = paste0(age, ' Years'),
           age = gsub('Total Years', 'Total', age),
           race_ethnicity = if_else(race_ethnicity == 'All_Races', 'Total', race_ethnicity),
           sex = if_else(sex == 'All_Sexes', 'Total', sex)
    )
    
    
  vroom::vroom_write(data, "standard/data_state_county_age.csv.gz", ",")

  if (!dir.exists("dist")) dir.create("dist")
  
  #creating dist files
  #aggregated total
  data_total <- data %>%
    filter(race_ethnicity == 'Total', sex == 'Total')
  
  vroom::vroom_write(data_total, "dist/data_state_county_age.csv.gz", ",")
 
  #stratified by race/ethnicity 
  data_by_race <- data %>%
    filter(sex == 'Total')
  
  vroom::vroom_write(data_by_race, "dist/data_state_county_age_by_race.csv.gz", ",")
  
  #stratified by sex
  data_by_sex <- data %>%
    filter(race_ethnicity == 'Total')
  
  vroom::vroom_write(data_by_sex, "dist/data_state_county_age_by_sex.csv.gz", ",")
  
  #record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
  
}
