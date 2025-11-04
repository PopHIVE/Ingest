library(epidatr)
library(tidyverse)

process <- dcf::dcf_process_record()

select_endpoints <- c('smoothed_covid19_from_claims','smoothed_flu_from_claims')

end.date <- lubridate::floor_date(Sys.Date(), 'week') - 1 #most recent saturday

timepoints <- seq.Date(from=as.Date('2020-01-04'), to=end.date, by='week')

#the smoothed data are available daily from the API, but we just take the most recent saturday value
state <- epidatr::pub_covidcast(
  source = "hospital-admissions", signal = select_endpoints,
  time_values=timepoints,
  geo_type = c("state"),
  time_type = "day" # important! This field defaults to "day", which won't work with data reported by week
) 
nation <- epidatr::pub_covidcast(
  source = "hospital-admissions", signal = select_endpoints,
  geo_type = c("nation"),
  time_values=timepoints,
  time_type = "day" # important! This field defaults to "day", which won't work with data reported by week
) 
county <- epidatr::pub_covidcast(
  source = "hospital-admissions", signal = select_endpoints,
  geo_type = c("county"),
  time_values=timepoints,
  time_type = "day" # important! This field defaults to "day", which won't work with data reported by week
) 

all <- bind_rows(state, nation,county) %>%
  vroom::vroom_write(., "raw/data.csv.xz", ",")


# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv.xz",
  recursive = TRUE,
  full.names = TRUE
)))

states.avail <- tolower(c(state.abb, 'us'))

#process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  data <- vroom::vroom('./raw/data.csv.xz') %>%
    mutate(geography = if_else(geo_value %in% states.avail,
             sprintf("%02d",cdlTools::fips(geo_value, to='fips') ),
             geo_value
            ),
           geography = if_else(geo_value=='us','00', geography)
    ) %>%
    rename(time = time_value) %>%
    dplyr::select(geography, time,signal, value) %>%
    pivot_wider(
      names_from = signal, 
      values_from = value,
      id_cols = c(geography, time)
    ) %>%
    rename(
      delphi_hospital_covid_smooth = smoothed_covid19_from_claims,
      delphi_hospital_flu_smooth = smoothed_flu_from_claims,
              )
  
  
  vroom::vroom_write(data, "standard/data.csv.gz", ",")
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
  
  
}

#to edit API key:
#library("usethis")
#edit_r_environ()
##add
#DELPHI_EPIDATA_KEY="XXXXXXXXXX"