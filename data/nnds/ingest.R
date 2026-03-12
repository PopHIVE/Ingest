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
    summarize(total = sum(`Cumulative YTD Current MMWR Year`, na.rm=T)
              )

  data_wide <- data %>%
    left_join(total_grp, by='Label') %>%
    filter(total>0)%>%
     mutate(`Cumulative YTD Current MMWR Year` = if_else(is.na(`Cumulative YTD Current MMWR Year`),0,`Cumulative YTD Current MMWR Year`),
    `Reporting Area` = toupper(`Reporting Area`)
     ) %>%
    filter(!is.na(LOCATION1)|`Reporting Area`%in% c('TOTAL') )%>%
    pivot_wider(id_cols = c(time, `Reporting Area` ), values_from= `Cumulative YTD Current MMWR Year`, names_from=Label) %>%
    clean_names() %>%
    mutate(
          reporting_area = if_else(reporting_area == 'TOTAL', 'UNITED STATES',reporting_area )) %>%
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


test <- data %>%
filter(Label=="Measles, Indigenous" & `Reporting Area` == "Total") %>%
arrange(`Reporting Area`, time) %>%
  group_by(`Reporting Area`) %>%
  rename(current=`Current week`) %>%
  summarize(csum2= cumsum(current)
            ) %>%
  arrange(desc(total))
