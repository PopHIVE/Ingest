#https://www.cdc.gov/brfss/annual_data/2022/pdf/Complex-Sampling-Weights-and-Preparing-Module-Data-for-Analysis-2022-508.pdf
#Researchers conducting analysis of variables from the core-only section should use the variable _LLCPWT for weighting.

library(tidyverse)
library(survey)
library(arrow)


# yrs <- 2018:2024
# 
# 
# read_year <- function(yr.select) {
#   var_pos_file <- paste0('./resources/variables', yr.select, '.csv')
#   
#   positions <- read_csv(var_pos_file) %>%
#     rename(start = 'Starting Column',
#            var = 'Variable Name',
#            length = 'Field Length') %>%
#     mutate(end = start + length - 1) %>%
#     filter(!is.na(start))
#   widths <- positions %>% pull(length)
#   varnames <- positions %>% pull(var)
#   start <- positions %>% pull(start)
#   end <- positions %>% pull(end)
#   
#   file.name <-
#     paste0("./raw/staging/LLCP",
#            yr.select,
#            "ASC/LLCP",
#            yr.select,
#            ".ASC")
#   
#   a <- read_fwf(
#     file = file.name,
#     fwf_positions  (start, end, varnames),
#     col_types = paste(rep('c', length(start)), collapse = '')
#   )
#   return(a)
# }
# 
# 
# all_data_ls <- lapply(yrs, read_year)
# all_data <- bind_rows(all_data_ls)
# arrow::write_dataset(all_data,path='./raw/survey_responses.parquet', format='parquet' ,max_rows_per_file=50000)


b <-
  arrow::open_dataset('./raw/survey_responses.parquet', format = 'parquet') %>%
  rename(state = '_STATE',
         wgt = '_LLCPWT',
         agec = '_AGE_G') %>%
  mutate(
    agec = as.numeric(agec),
    DIABETE4 = as.numeric(DIABETE4),
    wgt = as.numeric(wgt),
    age = if_else(agec == 1, '18-24 Years',
                  if_else(
                    agec == 2, '25-34 Years',
                    if_else(agec == 3, '35-44 Years',
                            if_else(
                              agec == 4, '45-54 Years',
                              if_else(agec == 5, '55-64 Years',
                                      if_else(agec ==
                                                6, '65+ Years',
                                              NA_character_))
                            ))
                  )),
    diab_yes = if_else(DIABETE4 == 1, 1,
                       if_else(DIABETE4 %in% c(2, 3, 4, 7, 9), 0,
                               NA_real_)),
    time = as.Date(paste0(IYEAR, '-01-01'))
  ) %>%
  dplyr::select(diab_yes , state, wgt, age, agec, DIABETE4, time) %>%
  collect()

prevalence_age_state <- b %>%
  group_by(state,time, age) %>%
  mutate(wgt = 100*wgt / sum(wgt, na.rm = T),
         prev_diabetes = wgt * diab_yes) %>%
  summarize(prev_diabetes = sum(prev_diabetes, na.rm = T))


prevalence_age_total <- b %>%
  group_by(age, time) %>%
  mutate(wgt = 100*wgt / sum(wgt, na.rm = T),
         prev_diabetes = wgt * diab_yes) %>%
  summarize(prev_diabetes = sum(prev_diabetes, na.rm = T)) %>%
  mutate(state = '00')

prevalence_total_state <- b %>%
  group_by(state,time) %>%
  mutate(wgt = 100*wgt / sum(wgt, na.rm = T),
         prev_diabetes = wgt * diab_yes) %>%
  summarize(prev_diabetes = sum(prev_diabetes, na.rm = T)) %>%
  ungroup() %>%
  mutate(age = 'Overall')


prevalence_combined <-
  bind_rows(prevalence_total_state,
            prevalence_age_total,
            prevalence_age_state) %>%
  filter(!is.na(age)) %>%
  rename(geography = state) %>%
  dplyr::select(geography, time, age, prev_diabetes) 

#check against data from web
v1 <- vroom::vroom('./standard/data.csv.gz') %>%
  dplyr::select(time, age, geography, pct_diabetes_value) %>%
  rename(pct_diabetes_precalc = pct_diabetes_value) %>%
  left_join(prevalence_combined, by=c('geography', 'time', 'age'))

ggplot(v1) +
  geom_point(aes(x=prev_diabetes, y=pct_diabetes_precalc, color=geography))+
  geom_abline(aes(intercept=0, slope=1))

