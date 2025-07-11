library(tidyverse)
library(RSocrata)
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
    dplyr::select(geography,time, percent_visits_rsv, percent_visits_flu, percent_visits_covid)

  #####################
  # Reformat COUNTY Level
  ######################
  
    data_state_merge <- vroom::vroom("./raw/rdmq-nq56.csv.xz", show_col_types = FALSE) %>%
      filter(county == 'All') %>%
      rename(
        percent_visits_rsv_state = percent_visits_rsv,
        percent_visits_covid_state = percent_visits_covid,
        percent_visits_flu_state = percent_visits_influenza
      ) %>%
      mutate( state=geography,
        geography = fips(geography, to='fips')
             )%>%
      dplyr::select(
        fips,
        week_end,
        percent_visits_rsv_state,
        percent_visits_covid_state,
        percent_visits_flu_state
      )
    
    data_county <- vroom::vroom("./raw/rdmq-nq56.csv.xz", show_col_types = FALSE) %>%
      filter(county != 'All') %>%
      rename(state = geography) %>%
      dplyr::select(
        state,
        county,
        fips,
        week_end,
        percent_visits_rsv,
        percent_visits_covid,
        percent_visits_influenza
      ) %>%
      left_join(data_state_merge, by = c('week_end', 'fips')) %>%
      mutate(
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
        fips = if_else(
          state == 'Connecticut' & county == 'Fairfield',
          9001,
          if_else(
            state == 'Connecticut' & county == 'Hartford',
            9003,
            if_else(
              state == 'Connecticut' & county == 'Litchfield',
              9005,
              if_else(
                state == 'Connecticut' & county == 'Middlesex',
                9007,
                if_else(
                  state == 'Connecticut' & county == 'New Haven',
                  9009,
                  if_else(
                    state == 'Connecticut' & county == 'New London',
                    9011,
                    if_else(
                      state == 'Connecticut' & county == 'Tolland',
                      9013,
                      if_else(
                        state == 'Connecticut' & county == 'Windham',
                        9015,
                        fips
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ) %>%
      dplyr::select(
        fips,
        week_end,
        percent_visits_covid,
        percent_visits_flu,
        percent_visits_rsv
      ) %>%
      as.data.frame() %>%
      rename(geography=fips,
             time=week_end)
  
  
  data <- bind_rows(data_state, data_county)
  
  vroom::vroom_write(
    data,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}


