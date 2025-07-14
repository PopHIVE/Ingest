library(dcf)
library(tidyverse)
library(reshape2)
library(cdlTools)
#
# Download
#

#Flu, covid, RSV combined files
process1 <- dcf::dcf_process_record()
raw_state1 <- dcf::dcf_download_cdc(
  "kvib-3txy",
  "raw",
  process1$raw_state
)

#RSV only
process2 <- dcf::dcf_process_record()
raw_state2 <- dcf::dcf_download_cdc(
  "29hc-w46k",
  "raw",
  process2$raw_state
)


#covid only
process3 <- dcf::dcf_process_record()
raw_state3 <- dcf::dcf_download_cdc(
  "6jg4-xsqq",
  "raw",
  process3$raw_state
)

#
# Reformat
#
if (!identical(process1$raw_state, raw_state1)) {
  
  #Data 1 has national*age or state*(overall age) for all viruses. 
  ##
  data1 <- vroom::vroom('raw/kvib-3txy.csv.xz') %>%
    filter(Type=='Unadjusted Rate' & Sex=='Overall' & `Race/Ethnicity`=='Overall') %>%
    rename(virus= 'Surveillance Network',
           age = 'Age group',
           state = Site,
           time= 'Week Ending Date' ) %>%
    mutate( virus = if_else(grepl('COVID', toupper(virus)),'rate_covid',
                        if_else(grepl('RSV', toupper(virus)),'rate_rsv',   
                            if_else(grepl('FLU', toupper(virus)),'rate_flu',           
                                    'rate_any'                          
                                  )))
    ) %>%
    dcast( .,  time + age + state ~ virus, value.var = 'Weekly Rate') %>%
    mutate( rate_flu = if_else(is.na(rate_flu),0, rate_flu), #do not fill in below
            geography = if_else(state=='Overall', 0,
                                fips(state, to='FIPS'))
          
            ) %>%
    filter(age =='Overall') %>%
    dplyr::select(-state)
  
  #data 2 has state*age for rsv
  data2 <- vroom::vroom('raw/29hc-w46k.csv.xz') %>%
    filter(`Age Category` %in% c('1-4 years', '0-<1 year','5-17 years', '18-49 years' ,
                                 "≥65 years" ,"50-64 years" )  &
             Sex=='All' & Race=='All' & Type=='Crude Rate') %>%
    rename(rate_rsv= Rate,
           time= "Week ending date",
           age = "Age Category" ) %>%
    mutate(geography = if_else(State=='RSV-NET', 0,
                               fips(State, to='FIPS'))
           ) %>%
    dplyr::select(geography, age, time, rate_rsv )
  
  #data 3 has state*age for influenza
  data3 <- vroom::vroom('raw/6jg4-xsqq.csv.xz') %>%
    filter(AgeCategory_Legend %in% c('1-4 years', '0-<1 year','5-17 years', '18-49 years' ,
                                     "≥65 years" ,"50-64 years" )) %>%
    rename(
           age = 'AgeCategory_Legend',
           state = State,
           time= '_WeekendDate' ) %>%
    dcast( .,  time + age + state ~ ., value.var = 'WeeklyRate') %>%
    mutate(geography = if_else(state=='COVID-NET', 0,
                                fips(state, to='FIPS'))
            
    ) %>%
    rename(rate_covid = '.') %>%
    dplyr::select(-state)
  
  data2_3_combo <- data2 %>%
    full_join(data3, by= c('age', 'time', 'geography')) 

  data_combined = bind_rows(data1, data2_3_combo) %>%
    mutate(rate_covid = if_else(time < '2020-03-01',0, rate_covid),
           rate_rsv = if_else(is.na(rate_rsv),0, rate_rsv), #do NOT fill in flu here
           
          age = if_else(age=='0-<1 year',"<1 Years",
                     if_else( age=='1-4 years', "1-4 Years",
                              if_else(age=="5-17 years" ,"5-17 Years",
                                      if_else(age=="18-49 years" ,"18-49 Years",
                                              if_else(age=="50-64 years" ,"50-64 Years",
                                                      if_else(age=="≥65 years",'65+ Years', 
                                                              if_else(age=="Overall",'Total',                      
                                                              
                                                              'other'               
                                                      )))))))
           ) 
  

  #Write standard data
  vroom::vroom_write(
    data_combined,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process1$raw_state <- raw_state1
  dcf::dcf_process_record(updated = process1)
  
  process2$raw_state <- raw_state2
  dcf::dcf_process_record(updated = process2)
  
  process3$raw_state <- raw_state3
  dcf::dcf_process_record(updated = process3)
  
}


