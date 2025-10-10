library(tidyverse)
library(tidycensus)
# read data from data source projects
library(tidyverse)

all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') %>%
  mutate(geography = as.numeric(geography))

#WISQARS data

wisqars <- vroom::vroom('../../data/wisqars/standard/data.csv.gz')

#CMS data is annual, not by month
cms <- vroom::vroom('../../data/cms_mmd/standard/data_state_county_age.csv.gz') %>%
  dplyr::select(geography, time, age,cms_alcohol_use_disorder, cms_drug_use_disorder,
                cms_opioid_use_disorder_dx_px_based, cms_opioid_use_disorder_overarching,
                cms_tobacco_use_disorder )%>%
  mutate(geography=as.numeric(geography)) 

cms_all_age_year <- cms %>%
  filter(age =='All_Ages')


wisqars_od <- wisqars %>%
  dplyr::select(geography, age, time, rate_unintentional_drug_poisoning )



nchs_od_state <- vroom::vroom('../nchs_mortality/standard/data.csv.gz') %>%
 # mutate(geography = sprintf("%02d", geography)) %>%
  left_join(all_fips, by='geography') %>%
  rename(nchs_pct_complete = pct_complete,
         nchs_pct_pending_invest = pct_pending_invest) %>%
  relocate(geography_name, state, geography)

write_parquet(nchs_od_state,'./dist/overdose_deaths_state.parquet' )

nchs_od_county <- vroom::vroom('../nchs_mortality/standard/data_county.csv.gz') %>%
 # mutate(geography = sprintf("%05d", geography)) %>%
  right_join(all_fips, by='geography') %>%
  rename(
         nchs_pct_pending_invest = pct_pending_invest)%>%
  relocate(geography_name, state, geography)

write_parquet(nchs_od_county,'./dist/overdose_deaths_county.parquet' )

nchs <- bind_rows(nchs_od_state, nchs_od_county)

#epic

# epic <- vroom::vroom('../../data/epic/standard/weekly.csv.gz') %>%
#   dplyr::select( )

#Google--weekly (averaged to monthly) searches
google <- vroom::vroom('../../data/gtrends/standard/data.csv.gz') %>%
  rename(gtrends_drug_overdose ="gtrends_drug+overdose") %>%
  dplyr::select(geography, time, gtrends_naloxone,gtrends_narcan, gtrends_overdose,gtrends_drug_overdose)%>%
  mutate(geography=as.numeric(geography),
         time = lubridate::floor_date(time, unit='month'))%>%
  group_by(geography, time) %>%
  summarise(across(
    c(gtrends_naloxone, gtrends_narcan, gtrends_overdose, gtrends_drug_overdose),
    ~ mean(.x, na.rm = TRUE)
  ))  



## Month trends in overdoses

### google trends
### NCHS deaths in previous 12 month
drugs_month <- nchs %>%
  dplyr::select(-geography_name) %>%
  full_join(google, by=c('geography','time')) %>%
  rename(date = time,
  ) %>%
  left_join(all_fips, by='geography') %>%
  arrange(geography, date) %>%
  group_by(geography) %>%
  mutate(gtrends_narcan_cum12 = zoo::rollsum(gtrends_narcan, k=12, na.pad=T))


###Time series of drug overdose deaths and naloxone searches by state

#The nchs data are 12 month cumulative sum.
drugs_month %>%
  filter(geography_name=='New York' ) %>%
  ggplot()+
  # geom_line(aes(x=date, y=gtrends_naloxone/max(gtrends_naloxone)))+
  geom_line(aes(x=date, y=gtrends_narcan_cum12/max(gtrends_narcan_cum12, na.rm=T)), color='red')+
  geom_line(aes(x=date, y=n_deaths_overdose/max(n_deaths_overdose, na.rm=T)), color='blue')+
  #geom_line(aes(x=date, y=gtrends_drug_overdose/max(gtrends_drug_overdose)), color='blue')+
  theme_classic()+
  ylim(0,NA)

drugs_month %>%
  filter(geography_name=='New York' ) %>%
  ggplot(aes(x=date, y=n_deaths_overdose))+
  geom_line()+
  theme_classic()+
  ylim(0,NA)

## Map of OD by month; county,--just take every 12th observation,
##NCHS deathsm CMS opioid use disorder
#nchs_od_county
