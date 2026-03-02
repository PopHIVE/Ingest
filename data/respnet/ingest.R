library(dcf)
library(tidyverse)
library(reshape2)
#
# Download
#

process <- dcf::dcf_process_record()

#Flu, covid, RSV combined files
raw_state_combined <- dcf::dcf_download_cdc(
  "kvib-3txy",
  "raw",
  process$raw_state_combined
)

#RSV only
raw_state_rsv <- dcf::dcf_download_cdc(
  "29hc-w46k",
  "raw",
  process$raw_state_rsv
)


#covid only
raw_state_covid <- dcf::dcf_download_cdc(
  "6jg4-xsqq",
  "raw",
  process$raw_state_covid
)

#
# Reformat
#

raw_state <- paste0(raw_state_combined, raw_state_rsv, raw_state_covid)
if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  #Data 1 has national*age or state*(overall age) for all viruses.
  ##
  data1 <- vroom::vroom('raw/kvib-3txy.csv.xz') %>%
    filter(
      rate_type == "Observed" &
        Sex == 'Overall' &
        `Race/Ethnicity` == 'Overall'
    ) %>%
    rename(
      virus = 'Surveillance Network',
      age = 'Age group',
      state = Site,
      time = 'Week Ending Date'
    ) %>%
    mutate(
      virus = if_else(
        grepl('COVID', toupper(virus)),
        'rate_covid',
        if_else(
          grepl('RSV', toupper(virus)),
          'rate_rsv',
          if_else(grepl('FLU', toupper(virus)), 'rate_flu', 'rate_any')
        )
      )
    ) %>%
    reshape2::dcast(., time + age + state ~ virus, value.var = 'Weekly Rate') %>%
    left_join(state_fips_lookup, by = c("state" = "geography_name")) %>%
    mutate(
      rate_flu = if_else(is.na(rate_flu), 0, rate_flu), #do not fill in below
      geography = if_else(state == "Overall", "00", geography)
    ) %>%
    filter(age == 'Overall') %>%
    dplyr::select(-state)
  
  #data 2 has state*age for rsv
  data2 <- vroom::vroom('raw/29hc-w46k.csv.xz') %>%
    filter(
      `Age Category` %in%
        c(
          '1-4 years',
          '0-<1 year',
          '5-17 years',
          '18-49 years',
          "≥65 years",
          "50-64 years"
        ) &
        Sex == 'All' &
        Race == 'All' &
        Type == 'Crude Rate'
    ) %>%
    rename(rate_rsv = Rate, time = "Week ending date", age = "Age Category") %>%
    left_join(state_fips_lookup, by = c("State" = "geography_name")) %>%
    mutate(
      geography = if_else(State == "RSV-NET", "00", geography)
    ) %>%
    dplyr::select(geography, age, time, rate_rsv)
  
  #data 3 has state*age for covid
  data3 <- vroom::vroom('raw/6jg4-xsqq.csv.xz') %>%
    filter(
      `Age Category` %in%
        c(
          '1-4 years',
          '0-<1 year',
          '5-17 years',
          '18-49 years',
          "≥65 years",
          "50-64 years"
        ) &
        Sex == 'All' &
        Race == 'All' &
        `Rate Type` == 'Observed'
    ) %>%
    rename(
      age = 'Age Category',
      state = State,
      time = 'Week ending date',
      rate_covid = 'Weekly Rate'
    ) %>%
    left_join(state_fips_lookup, by = c("state" = "geography_name")) %>%
    mutate(
      geography = if_else(state == "COVID-NET", "00", geography)
    ) %>%
    dplyr::select(-state)
  
  data2_3_combo <- data2 %>%
    full_join(data3, by = c('age', 'time', 'geography'))
  
  data_combined = bind_rows(data1, data2_3_combo) %>%
    as.data.frame() %>%  # Convert from Arrow to regular R data frame
    rename(fips = geography) %>%
    mutate(
      fips = as.numeric(fips),  # Ensure fips is numeric before sprintf
      rate_covid = if_else(time < '2020-03-01', 0, rate_covid),
      rate_rsv = if_else(is.na(rate_rsv), 0, rate_rsv), #do NOT fill in flu here

      age = if_else(
        age == '0-<1 year',
        "<1 Years",
        if_else(
          age == '1-4 years',
          "1-4 Years",
          if_else(
            age == "5-17 years",
            "5-17 Years",
            if_else(
              age == "18-49 years",
              "18-49 Years",
              if_else(
                age == "50-64 years",
                "50-64 Years",
                if_else(
                  age == "≥65 years",
                  '65+ Years',
                  if_else(
                    age == "Overall",
                    'Total',
                    
                    'other'
                  )
                )
              )
            )
          )
        )
      ),
      geography = sprintf("%02d", fips),
      time = lubridate::floor_date(time)
    ) %>%
    dplyr::select(time, geography, age, starts_with('rate'))
  
  
 
  #hash 284e55f3220473dfef18353f23577bfa90fd8fc8 Oct 6, 2025
  #versions <- dcf::dcf_get_file("./standard/data.csv.gz", versions=T )
  
  # oct6_2025 <- vroom::vroom(dcf::dcf_get_file("./standard/data.csv.gz", "2025-10-06" )) %>%
  #   mutate(vintage = as.Date('2025-10-06'))
  # 
  # data_combined2 <- data_combined %>%
  #   mutate(vintage = Sys.Date()) %>%
  #   bind_rows(oct6_2025) %>%
  #   arrange(time, geography, age, desc(vintage)) %>%
  #   group_by(time, geography, age) %>%
  #   mutate(order =row_number()) 
  # 
    #Write standard data
  vroom::vroom_write(
    data_combined,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state_combined <- raw_state_combined
  process$raw_state_rsv <- raw_state_rsv
  process$raw_state_covid <- raw_state_covid
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}