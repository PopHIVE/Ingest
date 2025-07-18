#TO DO: ensure all weekdates start with Sunday
##do any processing of variables (e.g., calculate percent, google standardization in the ingest.R scripts)
#move reformatting of the FIPS codes to 5 digit character to the ingest.R

library(dplyr)
library(arrow)
library(cdlTools)
library(lubridate)
library(reshape2)
library(tidycensus)

#overall_trends_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/overall_trends.parquet')

#############################
##Read in all of the datasets
#############################
state_fips <- c(0, as.numeric(unique(tidycensus::fips_codes$state_code)))
state_fips <- stringr::str_pad(gsub("\\D", "", state_fips), width = 2, pad = "0")

state_names <- c('United States', state.name)

bundle_files  <- list( '../epic/standard/weekly.csv.gz',
                       '../gtrends/standard/data.csv.gz',
                       '../NREVSS/standard/data.csv.gz',
                       '../nssp/standard/data.csv.gz',
                       '../respnet/standard/data.csv.gz',
                       '../wastewater/standard/data.csv.gz'
)
                 
start_time <- "2020"

#test <-  vroom::vroom('../gtrends/standard/data.csv.gz') 
     

data <- lapply(bundle_files, function(file) {
  d <- vroom::vroom(file, show_col_types = FALSE)
  if ("age" %in% colnames(d)) {
    d <- d[d$age == "Total", ] #all ages only
    d$age <- NULL
    
  }
  d[!is.na(d$time) & as.character(d$time) > start_time, ]
})

combined <- Reduce(
  function(a, b) merge(a, b, by = c("geography", "time"), all = TRUE),
  data
)
colnames(combined) <- sub("n_", "epic_", colnames(combined), fixed = TRUE)

overall_trends <- reshape2::melt(combined, id.vars = c('geography', 'time')) %>%
  filter(geography %in% state_fips ) %>%
  rename(fips= geography) %>%
  mutate( geography = fips(fips, to = "Name"),
         geography = if_else(fips == '00', 'United States', geography))

overall_trends %>% 
  filter(grepl('rsv',variable) & !is.na(value)) %>%
    arrow::write_parquet(., "dist/rsv/overall_trends.parquet")

overall_trends %>% 
  filter(grepl('flu',variable) & !is.na(value)) %>%
  arrow::write_parquet(., "dist/flu/overall_trends.parquet")

overall_trends %>% 
  filter(grepl('covid',variable) & !is.na(value)) %>%
  arrow::write_parquet(., "dist/covid/overall_trends.parquet")


#NREVSS data
#nrevss_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/positive_tests.parquet')
key <- readRDS('../../resources/hhs_regions.rds') %>%
  mutate(geography = gsub('Region ', 'hhs_',Group.1)
  ) %>%
  dplyr::select(x, geography)

d <- vroom::vroom('../NREVSS/standard/data.csv.gz') %>%
  rename(value = nrevss,
         date = time) %>%
  mutate(epiyr = lubridate::year(date), 
         year = lubridate::year(date),
         epiyr = if_else(nrevss_week<=26,year - 1 ,nrevss_week),
         epiwk  = if_else( nrevss_week<=26, nrevss_week+52, nrevss_week  ),
         week = nrevss_week,
         epiwk=epiwk-26,
         source = 'CDC NREVSS'
         ) %>%
  left_join(key, by='geography') %>%
  dplyr::select(-geography) %>%
  rename(geography=x) %>%
  mutate(scaled_cases = value/max(value)*100) %>%
  dplyr::select(source, geography, date, scaled_cases, value, epiyr, epiwk, week, year)
  
arrow::write_parquet(d, "dist/covid/positive_tests.parquet")

#RSV testing data
#epic_testing_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/rsv_testing_pct.parquet')
d2 <- vroom::vroom('../epic/standard/no_geo.csv.gz') %>%
  rename(n_pneumonia= n_rsv_tests,
         value = rsv_tests,
         date= time
         ) %>%
  mutate(source = 'Epic Cosmos, ED',
         suppressed_flag = if_else(n_pneumonia == '10 or fewer',1,0),
         geography = 'United States'
         )%>%
  dplyr::select(source, geography,age, date, value, n_pneumonia, suppressed_flag ) %>%
  filter(!is.na(age)) 
  
arrow::write_parquet(d2, "dist/covid/rsv_testing_pct.parquet")

##ED visits by county
#ed_county_view <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/ed_visits_by_county.parquet')

d3 <- vroom::vroom('../nssp/standard/data.csv.gz') %>%
  filter(!(geography %in% state_fips)) %>%
  rename( week_end = time) %>%
  mutate(fips = as.numeric(geography),
         source = 'CDC NSSP') 

d3 %>%
  dplyr::select(source, fips,week_end, percent_visits_rsv) %>%
  arrow::write_parquet(., "dist/rsv/ed_visits_by_county.parquet")

d3 %>%
  dplyr::select(source, fips, week_end, percent_visits_flu) %>%
  arrow::write_parquet(., "dist/flu/ed_visits_by_county.parquet")

d3 %>%
  dplyr::select(source,fips, week_end, percent_visits_covid) %>%
  arrow::write_parquet(., "dist/covid/ed_visits_by_county.parquet")


