#TO DO: 
##do any processing of variables (e.g., calculate percent, google standardization in the ingest.R scripts)
#move reformatting of the FIPS codes to 5 digit character to the ingest.R

library(dplyr)
library(arrow)
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

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
state_name_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)
state_abbr_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, state)

bundle_files  <- list( '../epic_resp_infections/standard/weekly.csv.gz',
                       '../gtrends/standard/data.csv.gz',
                       '../nssp/standard/data.csv.gz',
                       '../respnet/standard/data.csv.gz',
                       '../wastewater/standard/data.csv.gz',
                       '../delphi_doctors_claims/standard/data.csv.gz',
                       '../delphi_hospital_claims/standard/data.csv.gz',
                       '../delphi_nhsn/standard/data.csv.gz',
                       '../delphi_ili_fluview/standard/data.csv.gz'
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

# Load Kinsa daily data and aggregate to weekly (Saturday end-of-week)
kinsa_daily <- vroom::vroom('../kinsa_ili/standard/data.csv.gz', show_col_types = FALSE) %>%
  #mutate(time = lubridate::ceiling_date(as.Date(time), "week", week_start = 7) - 1) %>%
  #group_by(geography, time) %>%
  #summarise(kinsa_cough_cold_flu = mean(kinsa_cough_cold_flu, na.rm = TRUE), .groups = "drop") %>%
  filter(as.character(time) > start_time)

data <- c(data, list(kinsa_daily))

combined <- Reduce(
  function(a, b) merge(a, b, by = c("geography", "time"), all = TRUE),
  data
)

#colnames(combined) <- sub("n_", "epic_", colnames(combined), fixed = TRUE)


overall_trends <-   combined %>%
  filter( (time >= max(time) - 365*2) & geography %in% state_fips) %>%
  rename(fips= geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  dplyr::select(-geography_name) %>%
  reshape2::melt(., id.vars = c('geography', 'time','fips')) %>%
  mutate(value = suppressWarnings(as.numeric(value))) %>%
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
  
  value_smooth = value_smooth - suppressWarnings(min(value_smooth, na.rm = T)),

  value_scale = value - suppressWarnings(min(value, na.rm=T)),

  value_scale = value_scale / suppressWarnings(max(value_scale, na.rm = T)) * 100,

  value_smooth_scale = value_smooth / suppressWarnings(max(value_smooth, na.rm = T)) * 100
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
  filter(variable %in% c('epic_pct_flu', 'percent_visits_flu', 'rate_flu','wastewater_flua','delphi_nhsn_flu' ,'delphi_hospital_flu_smooth','delphi_fluview_wili','kinsa_cough_cold_flu')) %>%
  mutate( source = if_else(variable=='epic_pct_flu', 'Epic Cosmos, ED',
                                   if_else(variable=='percent_visits_flu', 'CDC NSSP',
                                           if_else(variable=='rate_flu', 'CDC RespNET',
                                                   if_else(variable=='delphi_hospital_flu_smooth', 'Delphi Hospital Claims',
                                                       if_else(variable=='wastewater_flua', 'CDC NWSS',
                                                             if_else(variable=='delphi_nhsn_flu', 'CDC NHSN',
                                                                   if_else(variable=='delphi_fluview_wili', 'CDC ILINet',
                                                                         if_else(variable=='kinsa_cough_cold_flu', 'Kinsa',
                                                           NA_character_

                                                   ))))))))
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
d2 <- vroom::vroom('../epic_resp_infections/standard/monthly_tests.csv.gz') %>%
 rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(source = 'Epic Cosmos, ED',
         suppressed_flag = if_else(epic_n_ed_j12_j18 == '10 or fewer',1,0),
         geography = if_else(fips=='00','United States', geography_name)
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
  dplyr::select(source, fips,week_end, percent_visits_rsv,is_state_estimate) %>%
  arrow::write_parquet(., "dist/rsv_ed_visits_by_county.parquet")

d3 %>%
  dplyr::select(source, fips, week_end, percent_visits_flu,is_state_estimate) %>%
  arrow::write_parquet(., "dist/flu_ed_visits_by_county.parquet")

d3 %>%
  dplyr::select(source,fips, week_end, percent_visits_covid,is_state_estimate) %>%
  arrow::write_parquet(., "dist/covid_ed_visits_by_county.parquet")

#############
## Age, state
#############

#age_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/trends_by_age.parquet')

bundle_files_age  <- list( '../epic_resp_infections/standard/weekly.csv.gz',
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
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  dplyr::select(-geography_name) %>%
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

    value_scale = value - min(value, na.rm = T),
    value_scale = value_scale / max(value_scale, na.rm = T) * 100,
    
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
  left_join(suppressed_covid_age, by=c('fips','date','age','variable')) %>%
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
    rename(value = N_IPD, value_incidence = rate_IPD) %>%
    mutate(year = lubridate::year(time)
           ) %>%
    dplyr::select(serotype, year, age, value, value_incidence)
  
  arrow::write_parquet(d4, "dist/pneumococcus_serotype_trends.parquet")

  #abc_view_geo <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/pneumococcus/by_geography.parquet')
  d5 <- vroom::vroom('../abcs/standard/data.csv.gz') %>%
    filter(geography != '00' & time == max(time) & age =='Total') %>%
    rename(value = pct_IPD,
           value_N = N_IPD,
           fips=geography) %>%
    left_join(state_abbr_lookup, by = c("fips" = "geography")) %>%
    mutate(year = lubridate::year(time),
           geography = state
    ) %>%
    dplyr::select(serotype, geography, year,  value, value_N)
    
  arrow::write_parquet(d5, "dist/pneumococcus_by_geography.parquet")
  
  d5a <- vroom::vroom('../abcs/standard/data.csv.gz') %>%
    filter(geography != '00'  & age =='Total') %>%
    rename(value = pct_IPD,
           value_N = N_IPD,
           fips=geography) %>%
    left_join(state_abbr_lookup, by = c("fips" = "geography")) %>%
    mutate(year = lubridate::year(time),
           geography = state
    ) %>%
    arrange(geography, serotype, year) %>%
    group_by(geography, serotype) %>%
    mutate(
      value_smooth = slider::slide_dbl(
        value,
        .f = ~ mean(.x, na.rm = TRUE),
        .before = 2,      # previous 2 rows + current = 3-year window
        .complete = FALSE # allow partial windows
      )
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

###############################################
# CDC CFA Rt, NCHS cause-specific mortality rates, and NNDSS notifiable diseases
################################################

cfa_rt_raw <- vroom::vroom('../cdc_cfa_rt/standard/data.csv.gz', show_col_types = FALSE) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  rename(date = time)

cfa_rt <- bind_rows(
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_covid', value = as.numeric(cdc_rt_covid)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_covid_lower', value = as.numeric(cdc_rt_covid_lower)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_covid_upper', value = as.numeric(cdc_rt_covid_upper)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_flu', value = as.numeric(cdc_rt_flu)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_flu_lower', value = as.numeric(cdc_rt_flu_lower)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_flu_upper', value = as.numeric(cdc_rt_flu_upper)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_rsv', value = as.numeric(cdc_rt_rsv)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_rsv_lower', value = as.numeric(cdc_rt_rsv_lower)),
  cfa_rt_raw %>% transmute(geography, date, source = 'CDC CFA Rt', variable = 'cdc_rt_rsv_upper', value = as.numeric(cdc_rt_rsv_upper))
) %>%
  filter(!is.na(value))

nchs_rates <- vroom::vroom('../nchs_mortality/standard/data_state_21_causes.csv.gz', show_col_types = FALSE) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  dplyr::select(geography, date = time,
                rate_covid_19, rate_chronic_lower_respiratory_diseases, rate_influenza_and_pneumonia) %>%
  reshape2::melt(id.vars = c('geography', 'date'), variable.name = 'variable', value.name = 'value') %>%
  mutate(variable = as.character(variable),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC NCHS Mortality') %>%
  filter(!is.na(value))

nnds_vars <- c(
  'influenza_associated_pediatric_mortality',
  'pertussis',
  'novel_influenza_a_virus_infections_confirmed',
  'novel_influenza_a_virus_infections_total',
  'haemophilus_influenzae_invasive_disease_age_5_years_non_b_serotype',
  'haemophilus_influenzae_invasive_disease_age_5_years_nontypeable',
  'haemophilus_influenzae_invasive_disease_age_5_years_serotype_b',
  'haemophilus_influenzae_invasive_disease_age_5_years_unknown_serotype',
  'haemophilus_influenzae_invasive_disease_all_ages_all_serotypes'
)

nnds_long <- vroom::vroom('../nnds/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  dplyr::select(geography, date = time, all_of(nnds_vars)) %>%
  reshape2::melt(id.vars = c('geography', 'date'), variable.name = 'variable', value.name = 'value') %>%
  mutate(variable = as.character(variable),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC NNDSS') %>%
  filter(!is.na(value))

other_measures_trends <- bind_rows(cfa_rt, nchs_rates, nnds_long) %>%
  dplyr::select(geography, date, source, variable, value) %>%
  arrange(source, variable, geography, date)

arrow::write_parquet(other_measures_trends, "dist/other_measures_trends.parquet")


###############################################
# Group A and Group B Streptococcus
###############################################
# Combines:
#   abcs                  CDC ABCs Group A and Group B Streptococcus, annual
#                         national (the strep_* / gas_* / gbs_* files; the
#                         pneumococcal data in that source is handled above)
#   epic_resp_infections  quarterly_gas.csv.gz - Epic Cosmos strep throat
#                         patients, by state and age
#   nnds                  streptococcal toxic shock syndrome, weekly by state
#                         (Epic and NNDSS are combined into gas_state.parquet)
#
# Two long parquets. All eight ABCs topics are stacked into one file with a
# named column per stratification and "Total" wherever a row is not stratified
# on that dimension. Epic and NNDSS share the second: both are state + national
# Group A series keyed on geography, date and one measure. ABCs stays apart
# because it is national-only and annual and carries fourteen dimension and
# companion columns neither of the others has - merging it would leave 62% of
# the cells as padding.
#
# Shared columns: geography (state name or "United States"), geography_fips,
# date (bundles use `date`; the standard files use `time`), year, measure,
# value.
#
# Companion columns rather than extra measure rows, so a tooltip can be built
# from one line: `n_isolates` is the denominator behind every percent, and
# `n_type` the numerator for emm types - "emm1: 22.5% (99 of 440 isolates)".
# Cases, deaths and survivals stay separate `measure` levels: they are three
# plottable series, not metadata for a single value.
#
# Each companion is blank on most rows, so each carries a `<name>_status` column
# saying whether it is blank because the measure has no such companion or
# because CDC never published it. See the status block below.
###############################################

# -----------------------------------------------------------------------------
# 0. FIPS <-> state name lookup. These dist files carry the name for readability
#    and the FIPS code for joining.
# -----------------------------------------------------------------------------
state_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  dplyr::select(geography_fips = geography, geography_name)

keep_geographies <- c(state.name, "District of Columbia", "United States")

add_geography_names <- function(df) {
  df %>%
    rename(geography_fips = geography) %>%
    left_join(state_lookup, by = "geography_fips") %>%
    mutate(geography = if_else(geography_fips == "00", "United States",
                               geography_name)) %>%
    filter(geography %in% keep_geographies) %>%
    dplyr::select(-geography_name) %>%
    relocate(geography, geography_fips)
}

# vroom attaches `spec` and `problems` attributes to what it reads; those ride
# through dplyr and get serialized into the parquet as R metadata, so two builds
# of identical data can differ byte for byte. These files are committed, so strip
# them and keep the output reproducible.
write_dist <- function(df, file) {
  df <- as.data.frame(df)
  attr(df, "spec") <- NULL
  attr(df, "problems") <- NULL
  arrow::write_parquet(df, file.path("dist", file))
}

# Read everything as text and convert the measure columns explicitly, rather
# than letting vroom guess. The measure columns are sparse - a cell CDC never
# published is NA - and vroom infers a column's type from a sample, so an
# all-NA measure would otherwise come back `logical` and silently blank real
# values elsewhere. Naming each column instead would warn on every file that
# lacks one, since the eight files carry different dimensions.
read_standard <- function(path) {
  vroom::vroom(
    file.path("..", path),
    show_col_types = FALSE,
    col_types = vroom::cols(.default = vroom::col_character())
  ) %>%
    mutate(
      time = as.Date(time),
      across(starts_with("abcs_"), as.numeric)
    ) %>%
    add_geography_names()
}

# A measure the source did not publish is NA there, and stays NA here. Nothing
# is filled in, so a 0 in `value` is always a measured zero.
#
# The companion columns (`n_isolates`, `n_type`) are blank on most rows for two
# unrelated reasons, and a bare NA cannot tell them apart: either the measure has
# no such companion at all (a case rate has no isolate denominator), or CDC
# simply never published it for that row. Each companion therefore carries a
# status column saying which:
#
#   "reported"        the companion holds CDC's published figure
#   "not_reported"    the companion applies to this measure but CDC published
#                     nothing - blank rather than 0, since "22.5% of 0 isolates"
#                     would read as broken
#   "not_applicable"  the measure has no such companion
#
# So a status of "reported" always accompanies a value, and the other two always
# accompany NA. The assertion after the stacks below enforces exactly that.
REPORTED       <- "reported"
NOT_REPORTED   <- "not_reported"
NOT_APPLICABLE <- "not_applicable"

COMPANIONS <- c("n_isolates", "n_type")
status_of <- function(x) paste0(x, "_status")

# -----------------------------------------------------------------------------
# 1. ABCs: stack all eight strep topics into one file
# -----------------------------------------------------------------------------
DIMS <- c("pathogen", "age", "sex", "race_ethnicity", "onset", "rate_denominator")
ENTITIES <- c("syndrome", "antibiotic", "emm_type", "serotype", "alph_type")

# Melt one standard file into the shared long schema. The standard files already
# carry their dimensions as columns - `antibiotic`, `emm_type`, `serotype` and so
# on - so this only stacks the measure columns and normalises their names.
#
#   measures    output measure level -> source column
#   companions  output name -> source column, for columns carried alongside
#               `value` rather than melted. The files name the same isolate
#               count three ways (`abcs_n_isolates`, `abcs_gbs_n_isolates`,
#               `abcs_gas_emm_n_isolates_total`), so normalising here keeps one
#               `n_isolates` column across the stacks.
stack_abcs <- function(path, measures, companions = character()) {
  d <- read_standard(path)
  companions <- companions[companions %in% names(d)]
  ids <- c("geography", "geography_fips", "time",
           intersect(c(DIMS, ENTITIES), names(d)))

  # Record why a companion is blank, which its own NA cannot say on its own.
  comp_cols <- character()
  for (nm in names(companions)) {
    d[[nm]] <- as.numeric(d[[companions[[nm]]]])
    d[[status_of(nm)]] <- if_else(is.na(d[[nm]]), NOT_REPORTED, REPORTED)
    comp_cols <- c(comp_cols, nm, status_of(nm))
  }

  missing <- setdiff(unname(measures), names(d))
  if (length(missing)) {
    stop("strep: ", path, " has no column ", paste(missing, collapse = ", "))
  }

  bind_rows(lapply(names(measures), function(m) {
    d %>%
      dplyr::select(all_of(c(ids, comp_cols)), value = all_of(measures[[m]])) %>%
      mutate(measure = m)
  }))
}

abcs_strep <- bind_rows(
  stack_abcs(
    "abcs/standard/strep_rates.csv.gz",
    c(rate_cases = "abcs_rate_cases", rate_deaths = "abcs_rate_deaths")
  ),
  stack_abcs(
    "abcs/standard/strep_counts.csv.gz",
    c(n_cases = "abcs_N_cases", n_deaths = "abcs_N_deaths",
      n_survivals = "abcs_N_survivals")
  ),
  stack_abcs(
    "abcs/standard/strep_resistance.csv.gz",
    c(pct_resistant = "abcs_pct_resistant"),
    companions = c(n_isolates = "abcs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gas_syndromes.csv.gz",
    c(rate_syndrome = "abcs_gas_rate_syndrome")
  ),
  stack_abcs(
    "abcs/standard/gbs_syndromes.csv.gz",
    c(pct_syndrome = "abcs_gbs_pct_syndrome")
  ),
  stack_abcs(
    "abcs/standard/gbs_serotypes.csv.gz",
    c(pct_serotype = "abcs_gbs_pct_serotype"),
    companions = c(n_isolates = "abcs_gbs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gbs_alph.csv.gz",
    c(pct_alph_type = "abcs_gbs_pct_alph"),
    companions = c(n_isolates = "abcs_gbs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gas_emm.csv.gz",
    c(pct_emm_type = "abcs_gas_emm_pct"),
    companions = c(n_type = "abcs_gas_emm_n",
                   n_isolates = "abcs_gas_emm_n_isolates_total")
  )
) %>%
  rename(date = time) %>%
  mutate(
    year = as.integer(format(date, "%Y")),
    # "Total" fills any dimension a row is not stratified on, so every column is
    # populated and a consumer can filter on it without handling NA
    across(all_of(c(DIMS, ENTITIES)), ~ tidyr::replace_na(as.character(.x), "Total")),
    # A stack that never had a companion contributes neither the value nor its
    # status, so both arrive NA from bind_rows. That is the third case: the
    # measure has no such companion at all.
    across(all_of(status_of(COMPANIONS)),
           ~ tidyr::replace_na(as.character(.x), NOT_APPLICABLE))
  ) %>%
  dplyr::select(all_of(c("geography", "geography_fips", "date", "year", DIMS, ENTITIES,
                         "measure", "value",
                         "n_type", "n_type_status",
                         "n_isolates", "n_isolates_status"))) %>%
  arrange(across(all_of(c("geography", "date", DIMS, ENTITIES, "measure"))))

if (anyDuplicated(abcs_strep[c("geography", "date", DIMS, ENTITIES, "measure")])) {
  stop("strep: duplicate index rows in abcs_strep.")
}

# `value` may be NA - that is how the file says CDC published nothing - but no
# column a consumer filters on may be, and each companion must be populated
# exactly when its status says "reported".
index_cols <- c("geography", "geography_fips", "date", "year", DIMS, ENTITIES,
                "measure")
if (anyNA(abcs_strep[index_cols])) {
  stop("strep: NA in an index, dimension or entity column.")
}
for (cc in COMPANIONS) {
  if (!identical(is.na(abcs_strep[[cc]]),
                 abcs_strep[[status_of(cc)]] != REPORTED)) {
    stop("strep: ", cc, " disagrees with ", status_of(cc), ".")
  }
}

write_dist(abcs_strep, "abcs_strep.parquet")

# -----------------------------------------------------------------------------
# 2. Epic Cosmos strep throat (from epic_resp_infections)
#    Two upstream suppression flags: the numerator flag covers both the count
#    and the percent (the percent derives from that same cell), the denominator
#    flag covers the patient total. Each measure gets its own.
# -----------------------------------------------------------------------------
epic_gas <- vroom::vroom(
  "../epic_resp_infections/standard/quarterly_gas.csv.gz",
  show_col_types = FALSE,
  col_types = vroom::cols(geography = "c", time = "D", age = "c",
                          .default = vroom::col_double())
) %>%
  add_geography_names() %>%
  dplyr::select(
    geography, geography_fips, time, age,
    n_strep_throat    = epic_n_strep_throat,
    pct_strep_throat  = epic_pct_strep_throat,
    n_patients        = epic_n_patients,
    .numerator_flag   = epic_strep_throat_suppressed_flag,
    .denominator_flag = epic_n_patients_suppressed_flag
  ) %>%
  tidyr::pivot_longer(
    c(n_strep_throat, pct_strep_throat, n_patients),
    names_to = "measure", values_to = "value"
  ) %>%
  mutate(
    suppressed = if_else(measure == "n_patients",
                         .denominator_flag, .numerator_flag),
    date = time,
    year = as.integer(format(time, "%Y"))
  ) %>%
  mutate(source = "Epic Cosmos") %>%
  dplyr::select(geography, geography_fips, date, year, age,
                source, measure, value, suppressed)

# -----------------------------------------------------------------------------
# 3. NNDSS streptococcal toxic shock syndrome
#    NNDSS publishes a cumulative year-to-date count that resets each MMWR year
#    (national 2024 runs 5 -> 647 across weeks 1-52), so it is de-accumulated
#    into a weekly-incident series. Both forms are emitted; they are not
#    additive.
#
#    Note this is the only Group A measure NNDSS carries - streptococcal toxic
#    shock syndrome is nationally notifiable, but invasive Group A disease
#    generally and Group B disease are not, so there is no broader NNDSS series
#    to draw on.
# -----------------------------------------------------------------------------
nnds_stss <- vroom::vroom(
  "../nnds/standard/data.csv.gz",
  show_col_types = FALSE,
  col_select = c(time, mmwr_year, mmwr_week, geography,
                 streptococcal_toxic_shock_syndrome),
  col_types = vroom::cols(geography = "c", time = "D")
) %>%
  rename(stss_cases_cumulative = streptococcal_toxic_shock_syndrome) %>%
  filter(!is.na(geography)) %>%
  add_geography_names() %>%
  arrange(geography, mmwr_year, mmwr_week) %>%
  group_by(geography, mmwr_year) %>%
  # The cumulative count resets each MMWR year, so the year's first week is
  # itself the increment (default = 0)
  mutate(
    stss_cases_weekly = stss_cases_cumulative -
      lag(stss_cases_cumulative, default = 0)
  ) %>%
  ungroup()

# NNDSS revises earlier weeks downward on occasion, which surfaces as a negative
# increment. Following the bundle_measles convention, these are kept as reported
# rather than clamped - the transparency is deliberate, and plots should cut the
# y axis at 0 instead.
n_negative <- sum(nnds_stss$stss_cases_weekly < 0, na.rm = TRUE)
if (n_negative > 0) {
  message(
    "NNDSS: ", n_negative, " of ", nrow(nnds_stss),
    " weekly increments are negative (downward revisions to the cumulative ",
    "count); kept as reported, per the bundle_measles convention."
  )
}

nnds_stss <- nnds_stss %>%
  # `date` is the MMWR week ending Saturday, so the week number adds nothing
  rename(date = time, year = mmwr_year) %>%
  dplyr::select(geography, geography_fips, date, year,
                stss_cases_weekly, stss_cases_cumulative) %>%
  tidyr::pivot_longer(c(stss_cases_weekly, stss_cases_cumulative),
                      names_to = "measure", values_to = "value") %>%
  filter(!is.na(value)) %>%
  # NNDSS publishes no age breakdown, so "Total" per the aggregate convention;
  # `suppressed` is an Epic mechanism and does not apply
  mutate(source = "NNDSS", age = "Total", suppressed = NA_real_) %>%
  dplyr::select(geography, geography_fips, date, year, age,
                source, measure, value, suppressed)

# -----------------------------------------------------------------------------
# 4. State-level Group A surveillance: Epic and NNDSS in one file
#    Both are state + national Group A series keyed on geography, date and a
#    single measure, so they share a schema. `source` separates them and `age`
#    is "Total" for NNDSS.
#    ABCs stays separate: it is national-only and annual, and carries fourteen
#    dimension and companion columns neither of these has.
# -----------------------------------------------------------------------------
gas_state <- bind_rows(epic_gas, nnds_stss) %>%
  arrange(geography, date, source, age, measure)

if (anyNA(gas_state[c("geography", "geography_fips", "date", "year",
                      "age", "source", "measure")])) {
  stop("strep: NA in an index column of gas_state.")
}

write_dist(gas_state, "gas_state.parquet")


