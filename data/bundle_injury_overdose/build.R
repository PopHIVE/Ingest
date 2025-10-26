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
  mutate( year= year(time),
        time = time %m+% years(1)  - 1 ) #define based on end of period

wisqars_long_rate <- wisqars%>%
  dplyr::select(geography, age,year, starts_with('wisqars_rate')) %>%
  pivot_longer(starts_with('wisqars_rate')) %>%
  mutate( name = gsub('wisqars_rate_', '',name))

wisqars_long <- wisqars%>%
  dplyr::select(geography, year,age, starts_with('wisqars_death')) %>%
  pivot_longer(starts_with('wisqars_death'), values_to='N') %>%
  mutate( name = gsub('wisqars_deaths_', '',name)) %>%
  full_join(wisqars_long_rate, by=c('geography', 'year','age', 'name')) %>%
  left_join(all_fips, by='geography') %>%
  dplyr::select(-geography,state) %>%
  rename(geography = geography_name,
         cause_of_death = name) %>%
  dplyr::select(year, age, geography, cause_of_death, value, N)

write_parquet(wisqars_long,'./dist/deaths_cause_age.parquet')


#CMS data is annual, not by month
cms <- vroom::vroom('../../data/cms_mmd/standard/data_state_county_age.csv.gz') %>%
  dplyr::select(geography, time, age,cms_alcohol_use_disorder, cms_drug_use_disorder,
                cms_opioid_use_disorder_dx_px_based, cms_opioid_use_disorder_overarching,
                cms_tobacco_use_disorder ) %>%
  mutate(time = time %m+% years(1)  - 1 ) #define based on end of period


#export to parquet; add in Epic county here later
  cms %>%
  filter(age =='65+ Years') %>%
  mutate(year=year(time)) %>%
  rename(opioid_rate = cms_opioid_use_disorder_overarching) %>%
  dplyr::select(year, geography,opioid_rate) %>%
   mutate(source='Medicare') %>%
  write_parquet('./dist/county_opioid_by_source.parquet')

cms_65plus_year <- cms %>%
  filter(age =='65+ Years')  

wisqars_od <- wisqars %>%
  mutate(time_end = time + 1) %>%
  dplyr::select(geography, age, time_end, rate_drug_poisoning ) 

nchs_od_state <- vroom::vroom('../nchs_mortality/standard/data.csv.gz') %>%
 # mutate(geography = sprintf("%02d", geography)) %>%
  left_join(all_fips, by='geography') %>%
  left_join(pop, by='geography') %>%
  rename(nchs_pct_complete = pct_complete,
         nchs_pct_pending_invest = pct_pending_invest) %>%
  relocate(geography_name, state, geography) %>%
  mutate(rate_deaths_overdose = n_deaths_overdose / pop *100000,
         suppressed = if_else(is.na(n_deaths_overdose),1,0)) %>%
  dplyr::select(geography, geography_name,time,n_deaths_overdose,rate_deaths_overdose )

nchs_od_state %>%
  dplyr::select (-geography) %>%
  rename(geography = geography_name) %>%
  mutate(max_date = max(time),
         max_month = month(max_date),
         month = month(time)) %>%
  filter(month==max_month) %>%
  dplyr::select(geography, time,n_deaths_overdose,rate_deaths_overdose ) %>%
  write_parquet(.,'./dist/overdose_deaths_state.parquet' )

nchs_od_county <- vroom::vroom('../nchs_mortality/standard/data_county.csv.gz') %>%
 # mutate(geography = sprintf("%05d", geography)) %>%
  full_join(all_fips, by='geography') %>%
  rename(
         nchs_pct_pending_invest = pct_pending_invest)%>%
  relocate(geography_name, state, geography) %>%
  left_join(pop, by='geography') %>%
  mutate(month=month(time),
         year= year(time),
         suppressed = if_else(is.na(n_deaths_overdose),1,0),
         # N_deaths >0 & <10 are suppressed. fill with 5
         n_deaths_overdose = if_else(is.na(n_deaths_overdose),5,n_deaths_overdose),
         rate_deaths_overdose = n_deaths_overdose / pop*100000
  ) %>%
  dplyr::select(geography,time,n_deaths_overdose,rate_deaths_overdose, suppressed )

write_parquet(nchs_od_county,'./dist/overdose_deaths_county.parquet' )

nchs <- bind_rows(nchs_od_state, nchs_od_county)

#epic

epic <- vroom::vroom('../../data/epic/standard/monthly.csv.gz') 

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



## trends in overdoses

combine_long <- function() {
  drugs_month_age <- wisqars_od %>%
    mutate(time = time_end + 1) %>%
    left_join(cms, by = c('age', 'geography', 'time'))
  
  ### google trends
  ### NCHS deaths in previous 12 month
  nchs_od <- nchs %>%
    rename(value = rate_deaths_overdose, nchs_n_deaths_overdose = n_deaths_overdose) %>%
    dplyr::select(geography, time, value, nchs_n_deaths_overdose , suppressed) %>%
    mutate(source = 'CDC/NCHS', age = 'Total') %>%
    filter(!is.na(time))
  
  google_od <- google %>%
    arrange(time) %>%
    # mutate(gtrends_narcan_cum12 = zoo::rollsum(gtrends_narcan, k = 12, na.pad =
    #                                              T)) %>%
    rename(value = gtrends_narcan) %>%
    dplyr::select(geography, time, value) %>%
    mutate(source = "Google Health Trends", age = 'Total')
  
  cms_65plus_year_od <- cms_65plus_year %>%
    rename(value = cms_opioid_use_disorder_overarching) %>%
    dplyr::select(geography, time, value, time_end) %>%
    mutate(source = 'Medicare FFS', age = '65+ Years')
  
  wisqars_od2 <- wisqars_od %>%
    rename(value = rate_drug_poisoning)  %>%
    mutate(source = 'CDC/WISQARS')
  
  epic_od <- epic %>%
    rename(value = epic_pct_ed_opioid) %>%
    dplyr::select(time, geography, age, value, suppressed) %>%
    mutate(source = 'Epic Cosmos')
  
  drugs_month_source <- bind_rows(nchs_od, google_od, wisqars_od2, epic_od) %>%
    left_join(all_fips, by = 'geography') %>%
    rename(fips = geography) %>%
    dplyr::select(-state) %>%
    group_by(source, age, fips) %>%
    mutate(value_scale = value/max(value, na.rm=T),
           time = if_else(!is.na(time_end), time_end, time)) #for 12m cum ave data, use last date
  
  
  drugs_month_source %>%
    ungroup() %>%
    rename(date = time) %>%
    filter(geography_name %in% c('United States', 'District of Columbia', state.name)) %>%
    rename(geography = geography_name) %>%
    filter(!is.na(value)) %>%
    write_parquet(., './dist/overdose_by_geography_and_source.parquet')
  
  drugs_month_source %>%
    ungroup() %>%
    filter(!(
      geography_name %in% c('United States', 'District of Columbia', state.name)
    )) %>%
    rename(date = time) %>%
    dplyr::select(geography_name, date, source, value) %>%
    rename(geography = geography_name) %>%
    filter(!is.na(value)) %>%
    write_parquet(.,
                       './dist/overdose_by_geography_and_source_county.parquet')
}

combine_long()


od_county <- read_parquet('./dist/overdose_by_geography_and_source_county.parquet')

od_state <- read_parquet('./dist/overdose_by_geography_and_source.parquet')

od_state %>%
  filter(geography=='Kentucky' & age=='Total') %>%
  ggplot()+
  geom_line(aes(x=date, y=value_scale, group=source, color=source)) +
  theme_classic()


###Time series of drug overdose deaths and naloxone searches by state

#The nchs + WISQARS, google data are 12 month cumulative sum .(e.g., 2023-01-01 is the total for 2022 calendar year)
# drugs_month %>%
#   filter(geography_name=='New York' ) %>%
#   ggplot()+
#   # geom_line(aes(x=date, y=gtrends_naloxone/max(gtrends_naloxone)))+
#   geom_line(aes(x=date, y=gtrends_narcan_cum12/max(gtrends_narcan_cum12, na.rm=T)), color='red')+
#   geom_line(aes(x=date, y=od_death_rate/max(od_death_rate, na.rm=T)), color='blue')+
#   geom_point(aes(x=date, y=rate_drug_poisoning/max(rate_drug_poisoning, na.rm=T)), color='black')+
#   geom_point(aes(x=date, y=cms_opioid_use_disorder_overarching/max(cms_opioid_use_disorder_overarching, na.rm=T)), color='orange')+
#     theme_classic()+
#   ylim(0,NA)+
#   ylab('Scaled value')



## Map of OD by month; county,--just take every 12th observation,
##NCHS deathsm CMS opioid use disorder
library(usmap)

nchs_od_state %>%
  filter(time=="2020-04-01") %>%
  rename(state= geography_name) %>%
plot_usmap(data=., regions='state', values = "rate_deaths_overdose", color = NA) + 
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
        

nchs_od_county %>%
  filter(time=='2020-12-01' ) %>%
  rename(fips = geography) %>%
  plot_usmap(data=., regions='counties', values = "rate_deaths_overdose", color = NA) + 
  scale_fill_continuous(name = "Deaths/100,000 that are overdose", label = scales::comma) + 
  theme(legend.position = "right")+
  ggtitle('Overdose deaths/100000 (NCHS)')


wisqars %>%
  filter(geography=='36' ) %>%
ggplot() +
  geom_line(aes(x=time, y=rate_firearm_intentional, group=age, color=age))



wisqars_long %>%
  filter(geography=='36' ) %>%
  ggplot() +
  geom_line(aes(x=time, y=rate_firearm_intentional, group=age, color=age))+
  ylim(0,NA) +
  theme_classic()+
  ylab('Deaths/100000')+
  ggtitle('Rate of Deaths from Firearm injuries in New York')

