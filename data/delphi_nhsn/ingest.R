library(epidatr)
library(tidyverse)
library(lubridate)

process <- dcf::dcf_process_record()

nhsn_endpoints <- c('confirmed_admissions_covid_ew', 'confirmed_admissions_rsv_ew','confirmed_admissions_flu_ew')

delphi_maxdate <- epidatr::pub_covidcast_meta() %>%
  filter(signal %in% nhsn_endpoints & data_source == 'nhsn') %>%
  pull(last_update) %>%
  max() %>%
  as.character()

if (!identical(process$delphi_maxdate, delphi_maxdate)) {

state <- epidatr::pub_covidcast(
  source = "nhsn", signal = nhsn_endpoints,
  geo_type = c("state"),
  time_type = "week" # important! This field defaults to "day", which won't work with data reported by week
) 
nation <- epidatr::pub_covidcast(
  source = "nhsn", signal = nhsn_endpoints,
  geo_type = c("nation"),
  time_type = "week" # important! This field defaults to "day", which won't work with data reported by week
) 

all <- bind_rows(state, nation)%>%
  vroom::vroom_write(., "raw/data.csv.xz", ",")

  
# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv.xz",
  recursive = TRUE,
  full.names = TRUE
)))

#process raw if state has changed
#if (!identical(process$raw_state, raw_state)) {
  data <- vroom::vroom('./raw/data.csv.xz') %>%
    mutate(geography = sprintf("%02d",cdlTools::fips(geo_value, to='fips') ),
           geography = if_else(geo_value=='us','00', geography),
           remove = if_else(
             (grepl('rsv', signal)|grepl('flu', signal)) & 
               time_value < as.Date('2024-10-31'),
             1,
             0)
    ) %>%
    filter(remove==0) %>%
    rename(time = time_value) %>%
    dplyr::select(geography, time,signal, value) %>%
    pivot_wider(
      names_from = signal, 
      values_from = value,
      id_cols = c(geography, time)
      ) %>%
    rename(
      delphi_nhsn_covid = confirmed_admissions_covid_ew, #n_covid
      delphi_nhsn_flu = confirmed_admissions_flu_ew, #n_flu
      delphi_nhsn_rsv = confirmed_admissions_rsv_ew, #n_rsv
    ) %>%
    mutate(time =  if_else( weekdays(time)=='Sunday', time+6, time)
           ) 
    
       
 
  vroom::vroom_write(data, "standard/data.csv.gz", ",")
  
  # record processed raw state
  process$raw_state <- raw_state
  process$delphi_maxdate <- delphi_maxdate

  dcf::dcf_process_record(updated = process)


}

#to edit API key:
#library("usethis")
#edit_r_environ()
##add
#DELPHI_EPIDATA_KEY="XXXXXXXXXX"