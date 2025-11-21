library(dcf)
library(tidyverse)
library(cdlTools)
library(tidyr)
#
# Download
#
all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') %>%
  filter(geography_name %in% c(state.name, 'District of Columbia', 'United States')
         & geography !='11001')

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "qvzb-qs6p",
  "raw",
  process$raw_state
)

# add files to the `raw` directory

#
# Reformat
#
if (!identical(process$raw_state, raw_state)) {
  
   data_age <-  vroom::vroom("./raw/qvzb-qs6p.csv.xz", show_col_types = FALSE) %>%
    rename(
      agec = "Age Group (years)",
      year = Year,
      st = 'IPD Serotype',
      N_IPD = 'Frequency Count',
      site = Site,
    ) %>%
    mutate(
      st = if_else(st == '16', '16F', st),
      agec1 = if_else(agec %in% c("Age <2", "Age 2-4"), 1, 2),
      agec = gsub('Age ', '', agec),
      agec2 = if_else(
        agec %in% c('<2', '2-4'),
        '<5',
        if_else(
          agec %in% c('5-17', '18-49'),
          '5-49',
          if_else(agec %in% c('50-64', '65+'), '50+', NA)
        )
      ),
      agec2 = factor(
        agec2,
        levels = c('<5', '5-49', '50+'),
        labels = c('<5 years', '5-49 years', '50+ years')
      )
    ) %>%
    group_by(site, st, agec2, year) %>%
    summarize(N_IPD = sum(N_IPD)) %>%
    ungroup() %>%
    mutate(time = as.Date(paste(year,'01','01',sep='-'))) %>%
    rename(age= agec2,
           serotype=st) %>%
    dplyr::select( site, age, serotype,  time, N_IPD)%>%
    tidyr::complete(site,serotype,age,time, fill=list(N_IPD=0)) %>%
    left_join(all_fips, by=c('site'='state')) %>%
    mutate(geography = if_else(site=='All_Sites', '00', geography)) %>%
    group_by(geography, age, time) %>%
    mutate(pct_IPD = 100* N_IPD / sum(N_IPD)) %>%
    dplyr::select( geography, age, serotype,  time, N_IPD,pct_IPD) %>%
    ungroup()
    
   data_total <- data_age %>%
     group_by(geography, time,serotype) %>%
     summarize(N_IPD = sum(N_IPD)) %>%
     ungroup() %>%
     group_by(geography, time) %>%
     mutate(pct_IPD = 100* N_IPD / sum(N_IPD),
            age = 'Total') %>%
     ungroup()
   
  
   data2 <- bind_rows(data_age, data_total) 
   
  vroom::vroom_write(
    data2,
    "standard/data.csv.gz",
    ","
  )
  
  uad <- read_csv(
    '../abcs/raw/ramirez_ofid_2025_ofae727.csv'
  )  %>%
    mutate(N_SSUAD = over65 + a50_64_with_indication + a50_64_no_indication,
           time=as.Date('2020-01-01'),
           geography= 'KY-TN-CT-IL') %>%
    rename(serotype=st) %>%
    dplyr::select(geography, time,serotype, N_SSUAD)

  vroom::vroom_write(
    uad,
    "standard/uad.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

# read from the `raw` directory, and write to the `standard` directory

