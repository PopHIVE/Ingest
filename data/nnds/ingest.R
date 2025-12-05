library(tidyverse)
library(cdlTools)
library(dcf)
library(MMWRweek)
library(janitor)

all_fips = vroom::vroom('../../resources/all_fips.csv.gz') %>%
  filter(geography_name  %in% c(state.name, 'United States','District of Columbia') & geography != '11001'
                              ) %>%
  mutate(geography_name = toupper(geography_name))
#
# Download and add files to the raw directory
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "x9gk-5huc",
  "raw",
  process$raw_state

)

if (!identical(process$raw_state, raw_state)) {
  data <- vroom::vroom("./raw/x9gk-5huc.csv.xz", show_col_types = FALSE) %>%
    mutate(time = MMWRweek2Date(`Current MMWR Year`, `MMWR WEEK`, MMWRday = NULL)
      )
  
  total_grp <- data %>%
    filter(!is.na(LOCATION1)) %>%
    group_by(Label ) %>%
    summarize(total = sum(`Current week`, na.rm=T)
              )

  data_wide <- data %>%
    left_join(total_grp, by='Label') %>%
    filter(total>0)%>%
     mutate(`Current week` = if_else(is.na(`Current week`),0,`Current week`)) %>%
    filter(!is.na(LOCATION1)|`Reporting Area`=="US RESIDENTS") %>%
    pivot_wider(id_cols = c(time, `Reporting Area` ), values_from= `Current week`, names_from=Label) %>%
    clean_names() %>%
    mutate(reporting_area = toupper(reporting_area),
           reporting_area = if_else(reporting_area=='US RESIDENTS', 'UNITED STATES',reporting_area )) %>%
    left_join(all_fips, by=c('reporting_area'='geography_name')) %>%
    dplyr::relocate(time, geography) %>%
    dplyr::select( -reporting_area, -state)
  
  vroom::vroom_write(
    data_wide,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
    
 
  
}