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


##this works but is inefficient
# View(combined %>% filter(geography=='06'))
# 
# epic <- vroom::vroom('../epic/standard/weekly.csv.gz', show_col_types = FALSE, guess_max = Inf) %>%
#   rename(date = time,
#          fips = geography) %>%
#   mutate(pct_rsv = n_rsv /n_all_encounters,
#          pct_flu = n_flu /n_all_encounters,
#          pct_covid = n_covid /n_all_encounters,
#          source = 'Epic Cosmos, ED',
#          geography = fips(fips, to = "Name")
#         ) 
# 
# gtrends <- vroom::vroom('../gtrends/standard/data.csv.gz', show_col_types = FALSE, guess_max = Inf) %>%
#   rename(date = time,
#          fips = geography) %>%
#     dplyr::select(fips, date, gtrends_rsv_vaccine, gtrends_rsv) %>%
#   mutate(source = 'Google Health Trends',
#          geography = fips(fips, to = "Name"))
# 
# nrevss <- vroom::vroom('../NREVSS/standard/data.csv.gz', show_col_types = FALSE, guess_max = Inf) %>%
#   rename(date = time,
#          fips = geography) %>%
#   mutate(source = 'NREVSS'
#          )
# 
# nssp <- vroom::vroom('../nssp/standard/data.csv.gz', show_col_types = FALSE, guess_max = Inf) %>%
#   rename(date = time,
#          fips = geography) %>%
#   mutate(source = 'CDC NSSP',
#          geography = fips(fips, to = "Name"),
#          geography = if_else(fips == 0, 'United States', geography)) 
# 
# respnet <- vroom::vroom('../respnet/standard/data.csv.gz', show_col_types = FALSE, guess_max = Inf) %>%
#   rename(date = time,
#          fips = geography) %>%
#   mutate(source = 'CDC RespNET',
#          geography = fips(fips, to = "Name"),
#          geography = if_else(fips == 0, 'United States', geography)
#          )
# 
# wastewater <- vroom::vroom('../wastewater/standard/data.csv.gz', show_col_types = FALSE, guess_max = Inf) %>%
#   rename(date = time,
#          fips = geography) %>%
#   mutate(source = 'CDC NWWS',
#          geography = fips(fips, to = "Name")
#          ) 
# 
# #############################
# ###RSV overall trends
# #############################
# 
# epic_rsv <- epic %>%
#   mutate(suppressed_flag = if_else(n_rsv == 5, 1, 0)) %>%
#   dplyr::select(geography, date, source, pct_rsv, suppressed_flag) %>%
#   rename(value = pct_rsv)
# 
# gtrends_rsv = gtrends %>%
#   mutate(suppressed_flag = 0 ,
#          month=lubridate::month(date),
#          season = if_else(month>=7 & month <=10,1,0),
#          rsv_novax2 = gtrends_rsv - season*(4.41-1.69)*gtrends_rsv_vaccine - (1-season)*3.41*gtrends_rsv_vaccine,  #2.655 based on the regression below
#          rsv_novax2 = if_else(rsv_novax2<0,0,rsv_novax2),
#          value = rsv_novax2) %>%
#   dplyr::select(geography, date, source, value, suppressed_flag) 
#   
# nssp_rsv_state <- nssp %>%
#   filter(geography %in% state_names) %>%
#   rename(value=percent_visits_rsv) %>%
#   mutate(suppressed_flag = 0) %>%
#   dplyr::select(geography, date, source, value, suppressed_flag) 
# 
# respnet_rsv <- respnet %>%
#   rename(value=rate_rsv) %>%
#   filter(age == 'Total') %>%
#   mutate(suppressed_flag = 0) %>%
#   dplyr::select(geography, date, source, value, suppressed_flag) 
# 
#   wastewater_rsv <- wastewater %>%
#     rename(value=wastewater_rsv) %>%
#   mutate(suppressed_flag = 0) %>%
#     dplyr::select(geography, date, source, value, suppressed_flag) 
# 
#   overall_trends_rsv <- bind_rows(epic_rsv, 
#                               gtrends_rsv, 
#                               nssp_rsv_state, 
#                               respnet_rsv, 
#                               wastewater_rsv
#                               ) %>%
#     arrange(source, geography, date) %>%
#     group_by(source, geography) %>%
#    mutate(value_smooth = zoo::rollapplyr(
#       value,
#       3,
#       mean,
#       partial = T,
#       na.rm = T
#     ),
#     value_smooth = if_else(is.nan(value_smooth), NA, value_smooth),
#     value_smooth = value_smooth - min(value_smooth, na.rm = T),
#     value_smooth_scale = value_smooth / max(value_smooth, na.rm = T) * 100
#    ) %>%
#     ungroup() %>%
#     dplyr::select(geography, date, source, value,value_smooth,value_smooth_scale, suppressed_flag) 
#   
#   write_parquet(overall_trends_rsv, './dist/rsv/overall_trends.parquet')
#   