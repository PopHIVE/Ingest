#Medicare FFS uses the CCW algorithms https://www2.ccwdata.org/documents/10280/19139421/chr-chronic-condition-algorithms.pdfcms
library(tidyverse)
library(arrow)
### FIPS codes
all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') 

pop <- vroom::vroom('../../resources/census_population_2021.csv.xz') %>%
  dplyr::select(GEOID, Total) %>%
  rename(geography=GEOID,
         pop=Total)

#### WISQARS data

wisqars <- vroom::vroom('../../data/wisqars/standard/data.csv.gz') %>%
  mutate(time = time %m+% years(1)  - 1 ) #define based on end of period

#CMS data is annual, not by month
cms <- vroom::vroom('../../data/cms_mmd/standard/data_state_county_age.csv.gz') %>%
  dplyr::select(geography, time, age,cms_alcohol_use_disorder, cms_drug_use_disorder,
                cms_opioid_use_disorder_dx_px_based, cms_opioid_use_disorder_overarching,
                cms_tobacco_use_disorder ) %>%
  mutate(time = time %m+% years(1)  - 1 ) #define based on end of period



cms_65plus_year <- cms %>%
  filter(age =='65+ Years') %>%
  mutate(time_end = time + 1) 

wisqars_od <- wisqars %>%
  mutate(time_end = time + 1) %>%
  dplyr::select(geography, age, time_end, rate_drug_poisoning ) 

nchs_od_state <- vroom::vroom('../nchs_mortality/standard/data.csv.gz') %>%
 # mutate(geography = sprintf("%02d", geography)) %>%
  left_join(all_fips, by='geography') %>%
  rename(nchs_pct_complete = pct_complete,
         nchs_pct_pending_invest = pct_pending_invest) %>%
  relocate(geography_name, state, geography)

write_parquet(nchs_od_state,'./dist/overdose_deaths_state.parquet' )

nchs_od_county <- vroom::vroom('../nchs_mortality/standard/data_county.csv.gz') %>%
 # mutate(geography = sprintf("%05d", geography)) %>%
  full_join(all_fips, by='geography') %>%
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
  mutate(time = lubridate::floor_date(time, unit='month'))%>%
  group_by(geography, time) %>%
  summarise(across(
    c(gtrends_naloxone, gtrends_narcan, gtrends_overdose, gtrends_drug_overdose),
    ~ mean(.x, na.rm = TRUE)
  ))  



## Month trends in overdoses

#need to standardize the age naming; age groups do not really align; main interest is <65 and 65+
# ggplot(cms) +geom_line(aes(x=age, y=cms_opioid_use_disorder_overarching, group=geography))

drugs_month_age <- wisqars_od %>%
  left_join(cms, by=c('age', 'geography','time'))

### google trends
### NCHS deaths in previous 12 month
drugs_month <- nchs %>%
  dplyr::select(-geography_name) %>%
  full_join(google, by=c('geography','time')) %>%
  full_join(cms_65plus_year, by=c('geography'='geography','time'='time_end')) %>%
  full_join(wisqars_od %>% filter(age=='Total'), by=c('geography'='geography','time'='time_end')) %>%
  rename(date = time,
  ) %>%
  dplyr::select(geography, date, n_deaths_overdose,n_deaths_all_cause, rate_drug_poisoning,gtrends_narcan,cms_drug_use_disorder,cms_opioid_use_disorder_overarching  ) %>%
  left_join(all_fips, by='geography') %>%
  arrange(geography, date) %>%
  group_by(geography) %>%
  left_join(pop, by='geography') %>%
  mutate(gtrends_narcan_cum12 = zoo::rollsum(gtrends_narcan, k=12, na.pad=T),
         od_death_rate = n_deaths_overdose / pop * 100000
         )

drugs_month_source <- drugs_month %>%
  dplyr::select(date, geography,geography_name,gtrends_narcan_cum12, od_death_rate,rate_drug_poisoning,cms_opioid_use_disorder_overarching ) %>%
  pivot_longer( cols=c(gtrends_narcan_cum12, od_death_rate,rate_drug_poisoning,cms_opioid_use_disorder_overarching)) %>%
  mutate( source = if_else( name== 'gtrends_narcan_cum12', "Google Health Trends",
                            if_else( name== 'od_death_rate', "CDC/NCHS",
                                     if_else( name== 'rate_drug_poisoning', "CDC/WISQARS",
                                              if_else( name== 'cms_opioid_use_disorder_overarching', "Medicare",NA_character_
                            ))))
  )

drugs_month_source %>% 
  ungroup() %>%
    filter(geography_name %in% c('United States', 'District of Columbia', state.name)) %>%
  dplyr::select(geography_name,date, source, value ) %>%
  rename(geography = geography_name) %>%
  filter(!is.na(value)) %>%
  vroom::vroom_write(., './dist/overdose_by_geography_and_source.parquet')

drugs_month_source %>% 
  ungroup() %>%
  filter(!(geography_name %in% c('United States', 'District of Columbia', state.name))) %>%
  dplyr::select(geography_name,date, source, value ) %>%
  rename(geography = geography_name) %>%
  filter(!is.na(value)) %>%
  vroom::vroom_write(., './dist/overdose_by_geography_and_source_county.parquet')





###Time series of drug overdose deaths and naloxone searches by state

#The nchs + WISQARS, google data are 12 month cumulative sum .(e.g., 2023-01-01 is the total for 2022 calendar year)
drugs_month %>%
  filter(geography_name=='New York' ) %>%
  ggplot()+
  # geom_line(aes(x=date, y=gtrends_naloxone/max(gtrends_naloxone)))+
  geom_line(aes(x=date, y=gtrends_narcan_cum12/max(gtrends_narcan_cum12, na.rm=T)), color='red')+
  geom_line(aes(x=date, y=od_death_rate/max(od_death_rate, na.rm=T)), color='blue')+
  geom_point(aes(x=date, y=rate_drug_poisoning/max(rate_drug_poisoning, na.rm=T)), color='black')+
  geom_point(aes(x=date, y=cms_opioid_use_disorder_overarching/max(cms_opioid_use_disorder_overarching, na.rm=T)), color='orange')+
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
library(usmap)
pop = vroom::vroom('../../resources/census_population_2021.csv.xz') %>%
  dplyr::select(Total, GEOID) %>%
  rename(geography = GEOID) %>%
  mutate(geography = as.numeric(geography))

nchs_od_month_pull <- nchs %>%
  left_join(pop, by='geography') %>%
  mutate(month=month(time),
         max_date = max(time, na.rm=T),
         max_month = month(max_date),
         year= year(time),
         overdose_rate = n_deaths_overdose / Total*100000) 

nchs_od_month_pull %>%
  filter(year==2025) %>%
  filter(month==max_month) %>%
  rename(states= geography_name) %>%
plot_usmap(data=., regions='state', values = "overdose_rate", color = NA) + 
  scale_fill_continuous(name = "Deaths/100,000 that are overdose", label = scales::comma) + 
  theme(legend.position = "right")



cms %>%
  filter(time==max(time, na.rm=T) & age=='<65 Years') %>%
  rename(fips=geography) %>%
plot_usmap(data=., regions='county', values = "cms_opioid_use_disorder_overarching", color = NA) + 
  scale_fill_continuous(name = "Opioid use disorder prevalence, <65 years CMS", label = scales::comma) + 
  theme(legend.position = "right")

#County maps
cms %>%
  filter(time==max(time) & age=='65+ Years') %>%
  rename(fips=geography) %>%
  plot_usmap(data=., regions='county', values = "cms_opioid_use_disorder_overarching", color = NA) + 
  scale_fill_continuous(name = "Prevalence", label = scales::comma) + 
  theme(legend.position = "right")+
  ggtitle('Opioid use disorder prevalence, 65+ years Medicare FFS')
        

nchs_od_month_pull %>%
  filter(time=='2024-12-31' & !(geography_name %in% c(state.name, 'District of Columbia'))) %>%
  rename(fips = geography) %>%
  plot_usmap(data=., regions='counties', values = "overdose_rate", color = NA) + 
  scale_fill_continuous(name = "Deaths/100,000 that are overdose", label = scales::comma) + 
  theme(legend.position = "right")+
  ggtitle('Overdose deaths/100000 (NCHS)')


