library(tidyverse)
library(cdlTools)
library(dcf)
#
# Download and add files to the raw directory
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "rdmq-nq56",
  "raw",
  process$raw_state
)


#####################
# Reformat STATE Level
######################


if (!identical(process$raw_state, raw_state)) {
  data_state <- vroom::vroom("./raw/rdmq-nq56.csv.xz", show_col_types = FALSE) %>%
    filter(county=='All'  ) %>%
    rename(state=geography, date='week_end') %>%
    dplyr::select(fips, date, percent_visits_rsv, percent_visits_influenza, percent_visits_covid) %>%
    collect() %>%
    rename(time = date, 
           geography = fips ,
           percent_visits_flu = percent_visits_influenza
           )%>%
    mutate(geography = geography,
           geography = sprintf("%05d", geography),
           geography = substr(geography,1,2)) %>%
    dplyr::select(geography,time, percent_visits_rsv, percent_visits_flu, percent_visits_covid) %>%
    mutate(
      percent_visits_rsv  = if_else(geography=='56' & time >= '2024-07-01', NA_real_,percent_visits_rsv ), 
      percent_visits_flu  = if_else(geography=='56' & time >= '2024-07-01', NA_real_,percent_visits_flu ) ,
      percent_visits_covid  = if_else(geography=='56' & time >= '2024-07-01', NA_real_,percent_visits_covid ) 
                 ) #Wyoming stops reporting in July 2024

  #####################
  # Reformat COUNTY Level
  ######################
    #for states without county info fill in with state-level from data_state_merge
    data_state_merge <- data_state %>%
      rename(
        percent_visits_rsv_state = percent_visits_rsv,
        percent_visits_covid_state = percent_visits_covid,
        percent_visits_flu_state = percent_visits_flu  
      ) %>%
      dplyr::select(
        geography,
        time,
        percent_visits_rsv_state,
        percent_visits_covid_state,
        percent_visits_flu_state
      ) %>%
      rename(state_fips = geography,
             week_end = time)
    
    data_county <- vroom::vroom("./raw/rdmq-nq56.csv.xz", show_col_types = FALSE) %>%
      filter(county != 'All') %>%
      rename(state = geography) %>%
      mutate(fips = sprintf("%05d", fips),
             state_fips = substr(fips,1,2)
             ) %>%
      dplyr::select(
        state,
        county,
        fips,
        state_fips,
        week_end,
        percent_visits_rsv,
        percent_visits_covid,
        percent_visits_influenza
      ) %>%
     left_join(data_state_merge, by = c('week_end', 'state_fips')) %>%
      rename(fips_code = fips) %>%
      mutate(
        is_state_estimate = if_else(
          is.na(percent_visits_covid) | is.na(percent_visits_influenza) | is.na(percent_visits_rsv),
          1L, 0L
        ),
        percent_visits_covid = if_else(
          is.na(percent_visits_covid),
          percent_visits_covid_state,
          percent_visits_covid
        ),
        percent_visits_flu = if_else(
          is.na(percent_visits_influenza),
          percent_visits_flu_state,
          percent_visits_influenza
        ),
        percent_visits_rsv = if_else(
          is.na(percent_visits_rsv),
          percent_visits_rsv_state,
          percent_visits_rsv
        ),
        
        #fix CT county coding
        fips_code = if_else(
          state == 'Connecticut' & county == 'Fairfield',
          '09001',
          if_else(
            state == 'Connecticut' & county == 'Hartford',
            '09003',
            if_else(
              state == 'Connecticut' & county == 'Litchfield',
              '09005',
              if_else(
                state == 'Connecticut' & county == 'Middlesex',
                '09007',
                if_else(
                  state == 'Connecticut' & county == 'New Haven',
                  '09009',
                  if_else(
                    state == 'Connecticut' & county == 'New London',
                    '09011',
                    if_else(
                      state == 'Connecticut' & county == 'Tolland',
                      '09013',
                      if_else(
                        state == 'Connecticut' & county == 'Windham',
                        '09015',
                        fips_code
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ) %>%
      mutate(
        percent_visits_rsv  = if_else(state_fips=='56' & week_end >= '2024-07-01', NA_real_,percent_visits_rsv ), 
        percent_visits_flu  = if_else(state_fips=='56' & week_end >= '2024-07-01', NA_real_,percent_visits_flu) ,
        percent_visits_covid  = if_else(state_fips=='56' & week_end >= '2024-07-01', NA_real_,percent_visits_covid ) 
      ) %>% #Wyoming stops reporting in July 2024
      as.data.frame() %>%
      rename(geography = fips_code) %>%
      rename(time=week_end) %>%
      dplyr::select(
        geography,
        time,
        percent_visits_covid,
        percent_visits_flu,
        percent_visits_rsv,
        is_state_estimate
      )


  data <- bind_rows(data_state, data_county) %>%
    mutate(geography = if_else(geography == "46113", "46102", geography))

  vroom::vroom_write(
    data,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}


