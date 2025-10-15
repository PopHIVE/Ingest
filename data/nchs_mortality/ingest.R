library(dcf)
library(tidyverse)
library(cdlTools)
library(arrow)
library(reshape2)

#State level, including type of drug
process1 <- dcf::dcf_process_record()
raw_state1 <- dcf::dcf_download_cdc(
  "xkb8-kh2a",
  "raw",
  process1$raw_state,
  parquet=T
)

#County-level
process2 <- dcf::dcf_process_record()
raw_state2 <- dcf::dcf_download_cdc(
  "gb4e-yj24",
  "raw",
  process2$raw_state,
  parquet=T
)

process3 <- dcf::dcf_process_record()
raw_state3 <- dcf::dcf_download_cdc(
  "489q-934x",
  "raw",
  process3$raw_state,
  parquet=T
)


if (!identical(process1$raw_state, raw_state1)) {
  
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
  process1$raw_state <- raw_state1
  dcf::dcf_process_record(updated = process1)
    
}

##############################
#2nd county-level dataset
##############################

if (!identical(process2$raw_state, raw_state2)) {
  
  #type of overdose counts by state (12 month backward total)
  data2 <- open_dataset('./raw/gb4e-yj24.parquet') %>%
    collect() %>%
    mutate( time = as.Date(MonthEndingDate, '%m/%d/%Y'),
            STATEFIPS = sprintf("%02d", STATEFIPS),
            COUNTYFIPS = sprintf("%03d", COUNTYFIPS),
            geography=paste0(STATEFIPS, COUNTYFIPS)
    ) %>%
    rename(n_deaths_overdose='Provisional Drug Overdose Deaths',
           pct_pending_invest = 'Percentage Of Records Pending Investigation',
           ) %>%
    dplyr::select(geography, time, n_deaths_overdose,pct_pending_invest)
 
  
  vroom::vroom_write(
    data2,
    "standard/data_county.csv.gz",
    ","
  )
  
  # record processed raw state
  process2$raw_state <- raw_state2
  dcf::dcf_process_record(updated = process2)
  
}

if (!identical(process3$raw_state, raw_state3)) {
  
  data_type <- open_dataset('./raw/489q-934x.parquet') %>%
    rename(time_period = 'Time Period',
           type_rate = 'Rate Type',
           cause_of_death = 'Cause of Death',
           year_q = "Year and Quarter",
           Rate_overall = 'Overall Rate'
           ) %>%
    filter(time_period == "3-month period" & type_rate == 'Age-adjusted'  ) %>%
    collect() %>%
    pivot_longer( cols= starts_with('Rate')) %>%
    mutate(state = map_lgl(name, ~ any(str_detect(.x, c('Rate_overall',state.name))))
           )%>%
    filter(state ==T ) %>%
    mutate(geography_name =  gsub('Rate ', '', name)) %>%
    mutate( geography_name = if_else(geography_name=='Rate_overall', 'United States', geography_name),
            qtr = str_extract(year_q, "(?<=\\s).*"),
            month = if_else(qtr=='Q1', '01',
                            if_else(qtr=='Q2', '04',
                                    if_else(qtr=='Q3', '07',
                                            if_else(qtr=='Q4', '10', '99'
                                            )))),
            time = as.Date(
              paste(substr(year_q,1,4), month, '01', sep='-')
            )
            ) %>%
    dplyr::select(geography_name,time, cause_of_death, value ) %>%
    pivot_wider(id_cols = c(geography_name,time),names_from = cause_of_death, values_from = value, names_prefix='rate_') %>%
    rename_with(
      ~ str_to_lower(.) %>%
        str_replace_all("[\\s,-]+", "_") %>%
        str_replace_all("[^a-z0-9_]", ""),
      .cols = starts_with("rate_")
    )
    
  
  vroom::vroom_write(
    data_type,
    "standard/data_state_21_causes.csv.gz",
    ","
  )
    
  # record processed raw state
  process3$raw_state <- raw_state3
  dcf::dcf_process_record(updated = process3)
  
}