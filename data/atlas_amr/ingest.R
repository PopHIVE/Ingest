library(tidyverse)
library(arrow)

# a1 <- readxl::read_excel('./raw/staging/2025_03_11 atlas_antibiotics.xlsx', guess_max = 5000)
# 
#  arrow::write_parquet(a1,'./raw/staging/2025_03_11 atlas_antibiotics.parquet' )

a1 <- arrow::open_dataset('./raw/staging/2025_03_11 atlas_antibiotics.parquet') %>%
  filter(Country=='United States') %>%
  dplyr::select(Species,  State, `Age Group`, Source, Year) %>%
  collect()


a2 <- a1 %>% 
  group_by(Species) %>%
  mutate(N_total_species = n()) %>%
  filter(N_total_species >=1000) %>%
  ungroup() %>%
  group_by(Species,  Year) %>%
  summarize(N=n()) %>%
  mutate(N = if_else(N<10,NA_real_,N)) %>%
  ungroup() %>%
  tidyr::complete(Species, Year, fill=list(N=NA))
