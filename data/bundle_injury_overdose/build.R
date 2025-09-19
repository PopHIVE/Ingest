library(tidyverse)
library(tidycensus)
# read data from data source projects
# and write to this project's `dist` directory

all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') %>%
  mutate(geography = as.numeric(geography))

#brfss

#a1 <- vroom::vroom('../../data/brfss/standard/data.csv.gz')

##cms_mmd

#CMS data is annual, not by month
cms <- vroom::vroom('../../data/cms_mmd/standard/data_state_county_age.csv.gz') %>%
  dplyr::select(geography, time, age,cms_alcohol_use_disorder, cms_drug_use_disorder,
                cms_opioid_use_disorder_dx_px_based, cms_opioid_use_disorder_overarching,
                cms_tobacco_use_disorder )%>%
  mutate(geography=as.numeric(geography)) 

cms_all_age_year <- cms %>%
  filter(age =='All_Ages')


#nchs_mortality

nchs_county <- vroom::vroom('../../data/nchs_mortality/standard/data_county.csv.gz') 
nchs_state <- vroom::vroom('../../data/nchs_mortality/standard/data.csv.gz') 

nchs <- bind_rows(nchs_state, nchs_county) %>%
  rename(nchs_pct_complete = pct_complete,
         nchs_pct_pending_invest = pct_pending_invest)



#epic

# epic <- vroom::vroom('../../data/epic/standard/weekly.csv.gz') %>%
#   dplyr::select( )

#Google
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

#drug
drugs_month <- nchs %>%
  full_join(google, by=c('geography','time')) %>%
  rename(date = time,
         ) %>%
  left_join(all_fips, by='geography') 


#The nchs data are 12 month cumulative sum.
drugs_month %>%
  filter(geography_name=='New York' ) %>%
  ggplot()+
  geom_line(aes(x=date, y=gtrends_naloxone/max(gtrends_naloxone)))+
  geom_line(aes(x=date, y=gtrends_narcan/max(gtrends_narcan)), color='red')+
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


  
