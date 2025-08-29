library(tidyverse)
library(arrow)


process <- dcf::dcf_process_record()

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "parquet",
  recursive = TRUE,
  full.names = TRUE
)))


if (!identical(process$raw_state, raw_state)) {
  
  data1 <- arrow::open_dataset('./raw/combined.parquet') %>%
    filter( racecat=='.' & sexcat=='.') %>%
    dplyr::select(fips,year, condition_name, prevalence_rate, age_label,geography) %>%
    collect() %>%
    rename(geography_level = geography) %>%
    pivot_wider(., id_cols=c(fips, year, age_label,geography_level), names_from=condition_name, values_from = prevalence_rate) %>%
    mutate(time = paste0('20',year,'-01-01'),
           fips = if_else(geography_level=='n', '00',fips)
           )%>%
    rename(geography = fips,
           age=age_label) %>%
    dplyr::select(-year) %>%
    relocate(geography, geography_level, time,age) %>%
    rename_with(
      ~ paste0("cms_", .x),
      .cols = -c(geography, geography_level, time, age)
    )
  
  data2 <- arrow::open_dataset('./raw/screening_combined.parquet') %>%
    filter( racecat=='.' & sexcat=='.') %>%
    dplyr::select(fips,year, condition_name, care_rate, age_label,geography) %>%
    collect() %>%
    rename(geography_level = geography) %>%
    pivot_wider(., id_cols=c(fips, year, age_label,geography_level), names_from=condition_name, values_from = care_rate) %>%
    mutate(time = paste0('20',year,'-01-01'),
           fips = if_else(geography_level=='n', '00',fips)
    )%>%
    rename(geography = fips,
           age=age_label) %>%
    dplyr::select(-year) %>%
    relocate(geography, geography_level, time,age) %>%
    rename_with(
      ~ paste0("cms_scrn_prvnt_", .x),
      .cols = -c(geography, geography_level, time, age)
    )
  
  data <- data1 %>%
    full_join(data2, by=c('geography', 'geography_level', 'time','age'))
    
    
  vroom::vroom_write(data, "standard/data_state_county_age.csv.gz", ",")
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
  
}
