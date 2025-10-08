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
         LLCPWT = '_LLCPWT',
         agec = '_AGE_G',
         bmi_cat = '_BMI5CAT') %>%
  mutate(
    agec = as.numeric(agec),
    DIABETE4 = as.numeric(DIABETE4),
    DIABETE3 = as.numeric(DIABETE3),
    LLCPWT = as.numeric(LLCPWT),
    bmi_cat = as.numeric(bmi_cat),
    IYEAR = as.numeric(IYEAR),
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
    DIABETE4 = if_else(IYEAR <= 2018, DIABETE3, DIABETE4),
    diab_yes = if_else(DIABETE4 == 1 , 1,
                       if_else(DIABETE4 %in% c(2, 3, 4, 7, 9), 0,
                               NA_real_)),
    obese_yes = if_else(bmi_cat == 4, 1, 0),
    time = as.Date(paste0(IYEAR, '-01-01'))
  ) %>%
  dplyr::select(diab_yes ,obese_yes, state, LLCPWT, age, agec,  time) %>%
  collect()

b_sum <- b %>%
  group_by(age,state,time, diab_yes) %>%
  summarize(LLCPWT = sum(LLCPWT))

# Define the design
design_age_state <- svydesign(
  id = ~1,
  weights = ~LLCPWT,
  data = b
)

# Calculate percent (mean) of diabetes by age, state, and time
prevalence_age_state <- svyby(
  ~diab_yes ,                  # variable of interest
  ~age + state + time,        # grouping variables
  design = design_age_state,
  svymean,
  vartype = 'ci',
  na.rm = TRUE
)

prevalence_age <- svyby(
  ~diab_yes,                  # variable of interest
  ~age +  time,        # grouping variables
  design = design_age_state,
  svymean,
  vartype = 'ci',
  na.rm = TRUE
)%>%
  mutate(state = '00')

prevalence_state <- svyby(
  ~diab_yes,                  # variable of interest
  ~ state + time,        # grouping variables
  design = design_age_state,
  svymean,
  vartype = 'ci',
  na.rm = TRUE
) %>%
  mutate(age = 'Total')

prevalence_year <- svyby(
  ~diab_yes,                  # variable of interest
  ~  time,        # grouping variables
  design = design_age_state,
  svymean,
  vartype = 'ci',
  na.rm = TRUE
) %>%
  mutate(age = 'Total', 
         state= '00'
         )


prevalence_combined <-
  bind_rows(prevalence_state,
            prevalence_age,
            
            prevalence_year) %>%
  mutate(prev_diabetes = diab_yes * 100,
         pct_diabetes_value_lcl = ci_l * 100,
         pct_diabetes_value_ucl = ci_u * 100,
         ) %>%
  filter(!is.na(age)) %>%
  rename(geography = state) %>%
  dplyr::select(geography, time, age, prev_diabetes) 

#check against data from web
v1 <- vroom::vroom('./standard/data.csv.gz') %>%
  dplyr::select(time, age, geography, pct_diabetes_value, pct_diabetes_sample_size) %>%
  rename(pct_diabetes_precalc = pct_diabetes_value) %>%
  left_join(prevalence_combined, by=c('geography', 'time', 'age')) 

ggplot(v1) +
  geom_point(aes(x=prev_diabetes, y=pct_diabetes_precalc, color=as.factor(time)))+
  geom_abline(aes(intercept=0, slope=1))

