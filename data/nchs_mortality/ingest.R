library(dcf)
library(tidyverse)
library(cdlTools)
library(arrow)
library(reshape2)

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "xkb8-kh2a",
  "raw",
  process$raw_state,
  parquet=T
)

if (!identical(process$raw_state, raw_state)) {
  
  #type of overdose counts by state (12 month backward total)
  data_type <- open_dataset('./raw/xkb8-kh2a.parquet') %>%
    collect() %>%
    mutate( time = as.Date(paste(Year, Month, '01', sep='-'), '%Y-%B-%d'),
            State = if_else(State=='YC','NY', State), #combines NYC and NY state
    ) %>%
    group_by(time,State, Indicator) %>%
    summarize( N_deaths = sum(`Data Value`)) %>%
    reshape2::dcast( time+State ~ Indicator , value.var='N_deaths') %>%
    mutate( 
            geography = if_else(State=='US', 0,
                               fips(State, to='FIPS')
          )
    ) %>%
    rename( n_deaths_cocaine = "Cocaine (T40.5)" ,
            n_deaths_heroin = "Heroin (T40.1)" ,
            n_deaths_methadone = "Methadone (T40.3)",
            n_deaths_any_opiod = "Natural, semi-synthetic, & synthetic opioids, incl. methadone (T40.2-T40.4)",
            n_deaths_all_cause = "Number of Deaths" ,
            n_deaths_overdose = "Number of Drug Overdose Deaths" #,
           # pct_drug_specified = "Percent with drugs specified",
                ) %>%
    dplyr::select(geography, time, starts_with('n_deaths_'))
  
  
  
  data_pct_specified <- open_dataset('./raw/xkb8-kh2a.parquet') %>%
    collect() %>%
    mutate( time = as.Date(paste(Year, Month, '01', sep='-'), '%Y-%B-%d'),
            State = if_else(State=='YC','NY', State), #combines NYC and NY state
    ) %>%
    filter(Indicator== "Percent with drugs specified") %>%
    group_by(time,State, Indicator) %>%
    #population-weighted average
    mutate(wgt = if_else(`State Name`=='New York', (19.87-8.258)/19.87,
                         if_else(`State Name`=='New York City', 8.258/19.87,
                                 1
                         )),
           wgt_part = wgt*`Data Value`
    ) %>%
    summarize( pct_drug_specified = sum(wgt_part)) %>%
    reshape2::dcast( time+State ~ Indicator , value.var='pct_drug_specified') %>%
    mutate( 
      geography = if_else(State=='US', 0,
                          fips(State, to='FIPS')
      )
    ) %>%
    rename( # pct_drug_specified = "Percent with drugs specified",
    ) %>%
    dplyr::select(geography, time, starts_with('pct_'))
  
  
  ##Completeness of data
  
  data_completeness <- open_dataset('./raw/xkb8-kh2a.parquet') %>%
    collect() %>%
    #population-weighted average for New York
    mutate( State = if_else(State=='YC','NY', State), #combines NYC and NY state
            geography = if_else(State=='US', 0,
                                fips(State, to='FIPS') ),
              wgt = if_else(`State Name`=='New York', (19.87-8.258)/19.87,
                         if_else(`State Name`=='New York City', 8.258/19.87,
                                 1
                         )),
           wgt_part_pct_complete = wgt*`Percent Complete`,
           wgt_part_pct_pending_invest = wgt*`Percent Pending Investigation`,
           time = as.Date(paste(Year, Month, '01', sep='-'), '%Y-%B-%d'),
           
    ) %>%
  group_by( geography, time, Indicator ) %>%
        summarize( pct_complete = sum(wgt_part_pct_complete),
                   pct_pending_invest = sum(wgt_part_pct_complete)) %>%
    dplyr::select(-Indicator) %>%
    distinct()
  
 data <- data_type %>%
   full_join(data_completeness, by=c('geography', 'time')) %>%
   full_join(data_pct_specified, by=c('geography', 'time')) 
   
  vroom::vroom_write(
    data,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
    
}