library(tidyverse)
library(tidycensus)

fips_codes_county <- tidycensus::fips_codes %>%
  mutate(fips = paste0(state_code, county_code)) %>%
  dplyr::select(fips, county, state, state_code) %>%
  rename(county_name=county)

fips_codes_state = fips_codes_county %>%
  dplyr::select( state, state_code) %>%
  unique() %>%
  rename(fips= state_code) %>%
  mutate(county_name = state.name[match(state, state.abb)],
         county_name = if_else(state=='DC', 'District of Columbia', county_name)
  )

fips_national = data.frame('fips'='00', 'state_code'='US','state'='US', 'county_name'='United States')

fips_code_combined <-   bind_rows(fips_codes_county,fips_codes_state,fips_national) %>%
  dplyr::select(-state_code) %>%
 # mutate(fips=as.numeric(fips)) %>%
  rename(geography=fips,
         geography_name = county_name)

vroom::vroom_write(fips_code_combined, './resources/all_fips.csv.gz')
