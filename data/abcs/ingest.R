library(dcf)
library(tidyverse)
library(cdlTools)

#
# Download
#

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
  
  #2019 serotype counts https://pubmed.ncbi.nlm.nih.gov/35172327/
  data_state <- vroom::vroom("./raw/jiac058_suppl_supplementary_table_s2.csv", show_col_types = FALSE) %>%
    group_by(State, sero) %>%
    summarize(N_cases = n()) %>%
    mutate(sero = as.factor(sero)) %>%
    ungroup() %>%
    group_by(State, sero) %>%
    mutate(mean_cases = max(N_cases, na.rm = T)) %>%
    group_by(State) %>%
    mutate(pct = N_cases / sum(N_cases, na.rm = T) * 100) %>%
    ungroup() %>%
    tidyr::complete(sero, State, fill = list(pct = 0)) %>%
    mutate(geography= fips(State, to='FIPS'),
           geography = sprintf("%02d", geography),
           time = as.Date('2019-01-01'),
           age='Total',
           N_cases = if_else(is.na(N_cases),0, N_cases)
           ) %>%
    rename(serotype=sero,
           N_IPD=N_cases,
           pct_IPD = pct
           ) %>%
    dplyr::select( age, serotype, geography, time, N_IPD,pct_IPD) %>%
    ungroup()
  
  
  data_nat <- vroom::vroom("./raw/qvzb-qs6p.csv.xz", show_col_types = FALSE) %>%
    rename(
      agec = "Age Group (years)",
      year = Year,
      st = 'IPD Serotype',
      N_IPD = 'Frequency Count'
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
    group_by(st, agec2, year) %>%
    summarize(N_IPD = sum(N_IPD)) %>%
    ungroup() %>%
    mutate(time = as.Date(paste(year,'01','01',sep='-')),
           geography='00') %>%
    rename(age= agec2,
           serotype=st) %>%
    dplyr::select( age, serotype, geography, time, N_IPD)
  
  
  data <- bind_rows(data_nat, data_state)
  
  vroom::vroom_write(
    data,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

# read from the `raw` directory, and write to the `standard` directory

