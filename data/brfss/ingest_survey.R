#https://www.cdc.gov/brfss/annual_data/2022/pdf/Complex-Sampling-Weights-and-Preparing-Module-Data-for-Analysis-2022-508.pdf
#Researchers conducting analysis of variables from the core-only section should use the variable _LLCPWT for weighting. 

library(tidyverse)
library(survey) 

positions <- read_csv('./resources/variables2024.csv') %>%
  rename(start = 'Starting Column',
         var = 'Variable Name',
         length ='Field Length') %>%
         mutate( 
           end = start + length -1
           )
    widths <- positions %>% pull(length)
    varnames <- positions %>% pull(var)
    start <- positions %>% pull(start)
    end <- positions %>% pull(end)


a <- read_fwf(file = "./raw/staging/LLCP2024ASC/LLCP2024.ASC",
              fwf_positions  ( start, end, varnames
              )
)


ny <- a %>%
  rename(state = '_STATE',
         wgt = '_LLCPWT',
         agec = '_AGE_G') %>%
  filter(state == '36') %>%
  dplyr::select(DIABETE4 ,state, wgt,agec)
