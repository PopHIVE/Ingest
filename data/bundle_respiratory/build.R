#TO DO: 
##do any processing of variables (e.g., calculate percent, google standardization in the ingest.R scripts)
#move reformatting of the FIPS codes to 5 digit character to the ingest.R

library(dplyr)
library(arrow)
library(cdlTools)
library(lubridate)
library(reshape2)
library(tidyverse)
library(tidycensus)


process <- dcf::dcf_process_record()
standard_files <- paste0("../", names(process$source_files))

#overall_trends_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/overall_trends.parquet')

#############################
##Read in all of the datasets with state-level info
#############################
state_fips <- c(0, as.numeric(unique(tidycensus::fips_codes$state_code)))
state_fips <- stringr::str_pad(gsub("\\D", "", state_fips), width = 2, pad = "0")

state_names <- c('United States', state.name)

bundle_files  <- list( '../epic/standard/weekly.csv.gz',
                       '../gtrends/standard/data.csv.gz',
                       '../nssp/standard/data.csv.gz',
                       '../respnet/standard/data.csv.gz',
                       '../wastewater/standard/data.csv.gz',
                       '../delphi_doctors_claims/standard/data.csv.gz',
                       '../delphi_hospital_claims/standard/data.csv.gz',
                       '../delphi_nhsn/standard/data.csv.gz'
)
                 
start_time <- "2020"



#test <-  vroom::vroom('../gtrends/standard/data.csv.gz') 
     

data <- lapply(bundle_files, function(file) {
  d <- vroom::vroom(file, show_col_types = FALSE)
  if ("age" %in% colnames(d)) {
    d <- d[d$age == "Total", ] #all ages only
    d$age <- NULL
    
  }
  d[!is.na(d$time) & as.character(d$time) > start_time, ]
})

combined <- Reduce(
  function(a, b) merge(a, b, by = c("geography", "time"), all = TRUE),
  data
)


#colnames(combined) <- sub("n_", "epic_", colnames(combined), fixed = TRUE)



############################
############################
#Experimental: try to just create one big output table
# output_table <- combined %>%
#   pivot_longer(
#     cols = where(is.numeric),
#     names_to = "metric",
#     values_to = "value"
#   ) %>%
#   arrange(geography, metric, time) %>%
#   group_by(geography, metric) %>%
#   mutate(
#     value_smooth = zoo::rollapplyr(value, 3, mean, partial = TRUE, na.rm = TRUE),
#     value_smooth_scale = value_smooth / max(value_smooth, na.rm = TRUE) * 100
#   ) %>%
#   ungroup() %>%
#   pivot_wider(
#     names_from = metric,
#     values_from = c(value, value_smooth, value_smooth_scale),
#     names_sep = "_"
#   )
# vroom::vroom_write(
#   output_table,
#   "dist/TEST_mega.csv.gz",
#   ","
# )
# arrow::write_parquet(output_table,
#                      "dist/TEST_mega.parquet")
# 
# jsonlite::write_json(output_table, gzfile("dist/TEST_mega.json.gz"), dataframe = "columns")  #way too big

####################################
####################################
####################################


overall_trends <-   combined %>%
  filter( (time >= max(time) - 365*2) & geography %in% state_fips) %>%
  rename(fips= geography) %>%
  mutate( geography = cdlTools::fips(fips, to = "Name"),
          geography = if_else(fips == '00', 'United States', geography)) %>%
  reshape2::melt(., id.vars = c('geography', 'time','fips')) %>%
  arrange(geography,  time) %>%
  group_by(geography,  variable) %>%
  mutate(
    value = if_else(geography=='Alaska' & grepl('epic',variable),NA_real_,value),
    value_smooth = zoo::rollapplyr(
    value,
    3,
    mean,
    partial = T,
    na.rm = T
  ),
  value_smooth = if_else(is.nan(value_smooth), NA, value_smooth),
  
  value_smooth = if_else(grepl('delphi_hospital',variable)|grepl('delphi_doctor',variable), value, value_smooth), #For Delphi, do not apply additional smoothing since data are pre-smoothed
  
  value_smooth = value_smooth - min(value_smooth, na.rm = T),
  value_smooth_scale = value_smooth / max(value_smooth, na.rm = T) * 100
  ) %>%
  ungroup() %>%
  rename(date = time) %>%
  arrange(variable,geography, date) %>%
  filter( geography %in% c(state.name,'District of Columbia','United States'))

suppressed_rsv <- combined %>%
  dplyr::select(geography, time,  epic_suppressed_flag_rsv) %>%
  rename(suppressed_flag = epic_suppressed_flag_rsv,
         fips=geography) %>%
  mutate(source = 'Epic Cosmos, ED') %>%
  rename(date = time) 

suppressed_flu <- combined %>%
  dplyr::select(geography, time,  epic_suppressed_flag_flu) %>%
  rename(suppressed_flag = epic_suppressed_flag_flu,
         fips=geography) %>%
  mutate(source = 'Epic Cosmos, ED') %>%
  rename(date = time) 
  
suppressed_covid <- combined %>%
  dplyr::select(geography, time,  epic_suppressed_flag_covid) %>%
  rename(suppressed_flag = epic_suppressed_flag_covid,
         fips=geography) %>%
  mutate(source = 'Epic Cosmos, ED') %>%
  rename(date = time) 

overall_trends %>% 
  filter(grepl('rsv',variable) & !is.na(value)) %>%
  filter(variable %in% c('epic_pct_rsv', 'gtrends_rsv_adjusted','percent_visits_rsv', 'rate_rsv','wastewater_rsv','delphi_nhsn_rsv' )) %>%
  mutate( source = if_else(variable=='epic_pct_rsv', 'Epic Cosmos, ED',
                    if_else(variable=='gtrends_rsv_adjusted', 'Google Health Trends',
                            if_else(variable=='percent_visits_rsv', 'CDC NSSP',
                                    if_else(variable=='rate_rsv', 'CDC RespNET',
                                            if_else(variable=='wastewater_rsv', 'CDC NWSS', 
                                                    if_else(variable=='delphi_nhsn_rsv', 'CDC NHSN', 
                                                                                                                                                                  NA_character_
                    ))))))
          ) %>%
  left_join(suppressed_rsv, by=c('fips','date','source')) %>%
  mutate(suppressed_flag = if_else(is.na(suppressed_flag), 0, suppressed_flag)) %>%
  group_by(geography,  fips, source) %>%
  mutate(N_obs = n()) %>%
  filter(N_obs >=52) %>%
  ungroup() %>%
    dplyr::select(-variable, -fips,-N_obs) %>%
    arrow::write_parquet(., "dist/rsv_overall_trends.parquet")

overall_trends %>% 
  filter(grepl('flu',variable) & !is.na(value)) %>%
  filter(variable %in% c('epic_pct_flu', 'percent_visits_flu', 'rate_flu','wastewater_flua','delphi_nhsn_flu' ,'delphi_hospital_flu_smooth')) %>%
  mutate( source = if_else(variable=='epic_pct_flu', 'Epic Cosmos, ED',
                                   if_else(variable=='percent_visits_flu', 'CDC NSSP',
                                           if_else(variable=='rate_flu', 'CDC RespNET',
                                                   if_else(variable=='delphi_hospital_flu_smooth', 'Delphi Hospital Claims', 
                                                       if_else(variable=='wastewater_flua', 'CDC NWSS', 
                                                             if_else(variable=='delphi_nhsn_flu', 'CDC NHSN', 
                                                                   
                                                           NA_character_
                                                           
                                                   ))))))
  ) %>%
  left_join(suppressed_flu, by=c('fips','date','source')) %>%
  mutate(suppressed_flag = if_else(is.na(suppressed_flag), 0, suppressed_flag)) %>%
  group_by(geography,  fips, source) %>%
  mutate(N_obs = n()) %>%
  filter(N_obs >=52) %>%
  ungroup() %>%
  dplyr::select(-variable, -fips,-N_obs) %>%
  arrow::write_parquet(., "dist/flu_overall_trends.parquet")

overall_trends %>% 
  filter(grepl('covid',variable) & !is.na(value)) %>%
  filter(variable %in% c('epic_pct_covid', 'percent_visits_covid', 'rate_covid','wastewater_covid','delphi_nhsn_covid','delphi_hospital_covid_smooth','delphi_doc_covid_smooth' )) %>%
  mutate( source = if_else(variable=='epic_pct_covid', 'Epic Cosmos, ED',
                                   if_else(variable=='percent_visits_covid', 'CDC NSSP',
                                           if_else(variable=='rate_covid', 'CDC RespNET',
                                                   if_else(variable=='wastewater_covid', 'CDC NWSS',
                                                           if_else(variable=='delphi_nhsn_covid', 'CDC NHSN', 
                                                                   if_else(variable=='delphi_hospital_covid_smooth', 'Delphi Hospital Claims', 
                                                                           if_else(variable=='delphi_doc_covid_smooth', 'Delphi Doctor Claims' ,
                                                                             
                                                                   
                                                           NA_character_
                                                           
                                                   )))))))
  ) %>%
  left_join(suppressed_covid, by=c('fips','date','source')) %>%
  mutate(suppressed_flag = if_else(is.na(suppressed_flag), 0, suppressed_flag)) %>%
  group_by(geography,  fips, source) %>%
  mutate(N_obs = n()) %>%
  filter(N_obs >=52) %>%
  ungroup() %>%
  dplyr::select(-variable, -fips,-N_obs) %>%
  arrow::write_parquet(., "dist/covid_overall_trends.parquet")


###################
#NREVSS data
###################
#nrevss_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/positive_tests.parquet')

d <- vroom::vroom('../NREVSS/standard/data.csv.gz') %>%
  rename(value = pcr_detections,
         date = time) 

arrow::write_parquet(d, "dist/rsv_positive_tests.parquet")

#################
#RSV testing data
#################

#epic_testing_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/rsv_testing_pct.parquet')
d2 <- vroom::vroom('../epic/standard/monthly_tests.csv.gz') %>%
 rename(fips = geography) %>%
  mutate(source = 'Epic Cosmos, ED',
         suppressed_flag = if_else(epic_n_ed_j12_j18 == '10 or fewer',1,0),
         geography = cdlTools::fips(fips, to='Name'),
         geography = if_else(fips=='00','United States', geography)
         )%>%
  rename(date=time) %>%
  dplyr::select(source, geography,age, date,epic_pct_rsv_pos_tests , epic_pct_j12_j18_tested_rsv, epic_n_ed_j12_j18,suppressed_flag ) %>%
  filter(!is.na(age) & !is.na(epic_pct_j12_j18_tested_rsv)) 
  
arrow::write_parquet(d2, "dist/rsv_testing_pct.parquet")

#########################
##ED visits by county
##########################
#ed_county_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/ed_visits_by_county.parquet')

d3 <- vroom::vroom('../nssp/standard/data.csv.gz') %>%
  filter(!(geography %in% state_fips)) %>%
  rename( week_end = time) %>%
  mutate(fips = as.numeric(geography),
         source = 'CDC NSSP') 

d3 %>%
  dplyr::select(source, fips,week_end, percent_visits_rsv) %>%
  arrow::write_parquet(., "dist/rsv_ed_visits_by_county.parquet")

d3 %>%
  dplyr::select(source, fips, week_end, percent_visits_flu) %>%
  arrow::write_parquet(., "dist/flu_ed_visits_by_county.parquet")

d3 %>%
  dplyr::select(source,fips, week_end, percent_visits_covid) %>%
  arrow::write_parquet(., "dist/covid_ed_visits_by_county.parquet")

#############
## Age, state
#############

#age_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/trends_by_age.parquet')

bundle_files_age  <- list( '../epic/standard/weekly.csv.gz',
                           '../respnet/standard/data.csv.gz'
)

start_time <- "2020"

#test <-  vroom::vroom('../gtrends/standard/data.csv.gz') 


data_age <- lapply(bundle_files_age, function(file) {
  d <- vroom::vroom(file, show_col_types = FALSE)
  if ("age" %in% colnames(d)) {
  }
  d[!is.na(d$time) & as.character(d$time) > start_time, ]
})

combined_age <- Reduce(
  function(a, b) merge(a, b, by = c("geography", "time", "age"), all = TRUE),
  data_age
)

#colnames(combined_age) <- sub("n_", "epic_", colnames(combined_age), fixed = TRUE)

trends_age <- combined_age %>%
  filter(geography %in% state_fips ) %>%
  filter(time >= max(time) -365*2 ) %>%
  rename(fips= geography) %>%
  mutate( geography = fips(fips, to = "Name"),
          geography = if_else(fips == '00', 'United States', geography)
          ) %>%
  dplyr::select(geography, time, age, fips, starts_with('epic_pct'),
                starts_with('rate')) %>%
  reshape2::melt(., id.vars = c('geography', 'time','fips', 'age'))  %>%
  rename(date = time) %>%
  mutate( source = if_else(grepl('epic', variable), 'Epic Cosmos (ED)', 'CDC RSV-NET (Hospitalization)'
                      )
          ) %>%
  filter(!is.na(value) & !is.na(age)) %>%
  mutate( value = as.numeric(value),
       # suppressed_flag = if_else(source=='Epic Cosmos (ED)' & raw==5,1,0),
          
         ) %>%
  ungroup() %>%
  dplyr::select(date, geography,fips, age, source,  value,variable) %>%
  arrange(geography, age, source,variable, date) %>%
  group_by(geography, age, source,variable) %>%
  mutate(
    value_smooth = zoo::rollapplyr(
      value,
      3,
      mean,
      partial = T,
      na.rm = T
    ),
    value_smooth = if_else(is.nan(value_smooth), NA, value_smooth),
    value_smooth = value_smooth - min(value_smooth, na.rm = T),
    value_smooth_scale = value_smooth / max(value_smooth, na.rm = T) * 100
  ) 


#need to add in suppressed flag!!
suppressed_rsv_age <- combined_age %>%
  dplyr::select(geography, time,age,  epic_suppressed_flag_rsv) %>%
  rename(fips = geography) %>%
  rename(suppressed_flag = epic_suppressed_flag_rsv) %>%
  mutate(variable = 'epic_pct_rsv') %>%
  rename(date = time) 

suppressed_flu_age <- combined_age %>%
  dplyr::select(geography, time,age,  epic_suppressed_flag_flu) %>%
  rename(fips = geography) %>%
  rename(suppressed_flag = epic_suppressed_flag_flu) %>%
  mutate(variable = 'epic_pct_flu') %>%
  rename(date = time) 

suppressed_covid_age <- combined_age %>%
  dplyr::select(geography, time,age,  epic_suppressed_flag_covid) %>%
  rename(fips = geography) %>%
  rename(suppressed_flag = epic_suppressed_flag_covid) %>%
  mutate(variable = 'epic_pct_covid') %>%
  rename(date = time) 


trends_age %>% 
  ungroup() %>%
  filter(variable %in% c('epic_pct_rsv','rate_rsv') & !is.na(value)) %>%
  left_join(suppressed_rsv_age, by=c('fips','date','age','variable')) %>%
  mutate(suppressed_flag = if_else(is.na(suppressed_flag),0,suppressed_flag)) %>%
  dplyr::select(-variable, -fips) %>%
  arrow::write_parquet(., "dist/rsv_trends_by_age.parquet")

trends_age %>% 
  ungroup() %>%
  filter(variable %in% c('epic_pct_flu', 'rate_flu') & !is.na(value)) %>%
  left_join(suppressed_flu_age, by=c('fips','date','age','variable')) %>%
  mutate(suppressed_flag = if_else(is.na(suppressed_flag),0,suppressed_flag)) %>%
  dplyr::select(-variable, -fips) %>%
  arrow::write_parquet(., "dist/flu_trends_by_age.parquet")

trends_age %>% 
  ungroup() %>%
  filter(variable %in% c('epic_pct_covid','rate_covid') & !is.na(value)) %>%
  full_join(suppressed_covid_age, by=c('fips','date','age','variable')) %>%
  mutate(suppressed_flag = if_else(is.na(suppressed_flag),0,suppressed_flag)) %>%
  dplyr::select(-variable, -fips) %>%
  arrow::write_parquet(., "dist/covid_trends_by_age.parquet")


##############################
### Google DMA
#############################
d3 <- vroom::vroom('../gtrends/standard/data_dma.csv.gz') %>%
  dplyr::select(geography, time, gtrends_rsv) %>%
  rename(value = gtrends_rsv) %>%
  rename(date = time) %>%
  filter(date > (max(date, na.rm=T)-365*2) ) %>%
  rename(fips = geography) %>%
  mutate(fips = as.numeric(fips))

  arrow::write_parquet(d3, "dist/rsv_google_dma.parquet")
  
###############################################
# Pneumococcus
################################################
  #abc_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/pneumococcus/serotype_trends.parquet')
  
d4 <- vroom::vroom('../abcs/standard/data.csv.gz') %>%
    filter(geography=='00') %>%
    rename(value = N_IPD) %>%
    mutate(year = lubridate::year(time)
           ) %>%
    dplyr::select(serotype, year, age, value)
  
  arrow::write_parquet(d4, "dist/pneumococcus_serotype_trends.parquet")

  #abc_view_geo <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/pneumococcus/by_geography.parquet')
  d5 <- vroom::vroom('../abcs/standard/data.csv.gz') %>%
    filter(geography != '00' & time == max(time) & age =='Total') %>%
    rename(value = pct_IPD,
           value_N = N_IPD,
           fips=geography) %>%
    mutate(year = lubridate::year(time),
           geography = fips(fips, to = "Abbreviation")
    ) %>%
    dplyr::select(serotype, geography, year,  value, value_N)
    
  arrow::write_parquet(d5, "dist/pneumococcus_by_geography.parquet")
  
  d5a <- vroom::vroom('../abcs/standard/data.csv.gz') %>%
    filter(geography != '00'  & age =='Total') %>%
    rename(value = pct_IPD,
           value_N = N_IPD,
           fips=geography) %>%
    mutate(year = lubridate::year(time),
           geography = fips(fips, to = "Abbreviation")
    ) %>%
    arrange(geography, serotype, year) %>%
    group_by(geography, serotype) %>%
    mutate(value_lag1 = lag(value,1),
           value_lag2 = lag(value,2),
           value_smooth = rowMeans(across(c(value, value_lag1, value_lag2)), na.rm = TRUE)
           ) %>%
    dplyr::select(serotype, geography, year,  value, value_N, value_smooth) %>%
    ungroup()
  
  arrow::write_parquet(d5a, "dist/pneumococcus_by_geography_year.parquet")
  
  d4_2019_2020 <- d4 %>% filter(year %in% c(2019,2020) & age == "50+ years") %>%
    group_by(serotype) %>%
    summarize(value=sum(value)) %>%
    ungroup()
  
  uad <- read_csv(
    '../abcs/standard/uad.csv.gz'
  ) %>%
    full_join(d4_2019_2020, by = 'serotype'
              )%>%
    filter(!is.na(N_SSUAD) & !is.na(value)) %>%
    mutate(year = '2019-2020') %>%
    dplyr::select(geography, year, serotype, N_SSUAD, value) %>%
    rename( ipd = value, pneumonia = N_SSUAD)
  
  arrow::write_parquet(uad, "dist/pneumococcus_comparison.parquet")
  