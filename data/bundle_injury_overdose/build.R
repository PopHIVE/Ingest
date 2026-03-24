#Medicare FFS uses the CCW algorithms https://www2.ccwdata.org/documents/10280/19139421/chr-chronic-condition-algorithms.pdfcms
library(tidyverse)
library(lubridate)
library(arrow)

### FIPS codes
all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') 

state_fips <- all_fips %>%
  filter(geography_name %in% c('District of Columbia',state.name) & geography != '11001') %>%
  pull(geography)

state_cw <- all_fips %>%
  filter(geography %in% c(state_fips, '00' ))

pop_region <- vroom::vroom('../../resources/census_population_2021.csv.xz') %>%
  dplyr::select(GEOID, Total) %>%
  rename(geography=GEOID,
         pop=Total)

pop_us <- pop_region %>%
  filter(geography %in% state_fips) %>%
  summarize(pop = sum(pop)) %>%
  mutate(geography = '00')

pop <- bind_rows(pop_us, pop_region)

#Google--yearly search volume
google <- vroom::vroom('../../data/gtrends/standard/data_year.csv.gz') %>%
  rename(gtrends_drug_overdose ="gtrends_drug+overdose",
         gtrends_heat_exhaustion="gtrends_heat+exhaustion",
         gtrends_heat_stroke="gtrends_heat+stroke",
  ) %>%
  dplyr::select(geography, time, gtrends_9mm, gtrends_naloxone,gtrends_drug_overdose,
                gtrends_heat_exhaustion,gtrends_heat_stroke,gtrends_narcan,gtrends_overdose,gtrends_shotgun)

#### WISQARS data
wisqars <- vroom::vroom('../../data/wisqars/standard/data.csv.gz') %>%
  mutate( year= lubridate::year(time),
          time = as.Date(paste(year, '07','01', sep='-')),
          age = if_else(age == "0-14 Years" , "<15 Years", age)
  ) #define based on end of period


wisqars_aggregated <- wisqars %>%
  filter(sex == "All", race == "All", ethnicity == "All")

wisqars_long_rate <- wisqars_aggregated %>%
  dplyr::select(geography, age, year, starts_with('wisqars_rate')) %>%
  pivot_longer(starts_with('wisqars_rate')) %>%
  mutate( name = gsub('wisqars_rate_', '',name))

wisqars_long <- wisqars_aggregated %>%
  dplyr::select(geography, year, age, starts_with('wisqars_death')) %>%
  pivot_longer(starts_with('wisqars_death'), values_to='N') %>%
  mutate( name = gsub('wisqars_deaths_', '',name)) %>%
  full_join(wisqars_long_rate, by=c('geography', 'year','age', 'name')) %>%
  left_join(all_fips, by='geography') %>%
  dplyr::select(-geography, state) %>%
  rename(geography = geography_name,
         cause_of_death = name) %>%
  filter(cause_of_death %in% c('natural_environmental',
                              'drowning_includes_water_transport_',
                               'fall',
                               'fire_flame',
                              'suffocation',
                               'motor_vehicle_traffic',
                               'firearm_accident',
                               'firearm_intentional',
                               'firearm_homicide',
                               'firearm_suicide',
                               'firearm_legal_intervention',
                               'drug_poisoning',
                              'pedal_cyclist_mv_traffic',
                              "pedestrian_mv_traffic",
                               'non_drug_poisoning'
                               )) %>%
  mutate(cause_of_death = gsub('natural_environmental', 'Natural/environmental', cause_of_death),
    cause_of_death = gsub('drowning_includes_water_transport_', 'Drowning, including water transport', cause_of_death),
          cause_of_death = gsub( 'fall', 'Fall', cause_of_death),
          cause_of_death = gsub('fire_flame' , 'Exposure to smoke, fire, flame' , cause_of_death),
          cause_of_death = gsub('motor_vehicle_traffic' , 'Motor vehicle, traffic', cause_of_death),
          cause_of_death = gsub( 'non_drug_poisoning', 'Non-drug poisoning' , cause_of_death),
          cause_of_death = gsub( 'drug_poisoning', 'Drug poisoning' , cause_of_death),
    cause_of_death = gsub( 'suffocation', 'Suffocation' , cause_of_death),
    cause_of_death = gsub( 'pedal_cyclist_mv_traffic', 'Pedal cyclist (motor vehicle)' , cause_of_death),
    cause_of_death = gsub( 'pedestrian_mv_traffic', 'Pedestrian (motor vehicle traffic)' , cause_of_death),

          cause_of_death = gsub('firearm_accident' ,'Firearm (unintentional)' , cause_of_death),
          cause_of_death = gsub('firearm_legal_intervention' ,'Firearm (legal intervention)' , cause_of_death),
          cause_of_death = gsub('firearm_intentional' ,'Firearm (intentional)' , cause_of_death),
          cause_of_death = gsub('firearm_homicide' ,'Firearm (homicide)' , cause_of_death),
          cause_of_death = gsub('firearm_suicide' ,'Firearm (suicide)' , cause_of_death)
          ) %>%
  dplyr::select(year, age, geography, cause_of_death, value, N)

write_parquet(wisqars_long,'./dist/deaths_cause_age.parquet')

# Demographic-stratified version
wisqars_long_rate_demo <- wisqars %>%
  dplyr::select(geography, age, sex, race, ethnicity, year, starts_with('wisqars_rate')) %>%
  pivot_longer(starts_with('wisqars_rate')) %>%
  mutate( name = gsub('wisqars_rate_', '',name))

wisqars_long_demo <- wisqars %>%
  dplyr::select(geography, year, age, sex, race, ethnicity, starts_with('wisqars_death')) %>%
  pivot_longer(starts_with('wisqars_death'), values_to='N') %>%
  mutate( name = gsub('wisqars_deaths_', '',name)) %>%
  full_join(wisqars_long_rate_demo, by=c('geography', 'year', 'age', 'sex', 'race', 'ethnicity', 'name')) %>%
  left_join(all_fips, by='geography') %>%
  dplyr::select(-geography, state) %>%
  rename(geography = geography_name,
         cause_of_death = name) %>%
  dplyr::select(year, age, sex, race, ethnicity, geography, cause_of_death, value, N)

write_parquet(wisqars_long_demo,'./dist/deaths_cause_age_demographics.parquet')

#CMS data is annual, not by month
cms <- vroom::vroom('../../data/cms_mmd/standard/data_state_county_age.csv.gz') %>%
  dplyr::select(geography, time, age,cms_alcohol_use_disorder, cms_drug_use_disorder,
                cms_opioid_use_disorder_dx_px_based, cms_opioid_use_disorder_overarching,
                cms_tobacco_use_disorder ) %>%
  mutate(year=year(time),
         time = as.Date(paste(year, '07','01', sep='-'))
  )

#export to parquet; add in Epic county here later
cms %>%
  filter(age =='65+ Years') %>%
  mutate(year=year(time)) %>%
  rename(opioid_rate = cms_opioid_use_disorder_overarching) %>%
  dplyr::select(year, geography,opioid_rate) %>%
  mutate(source='Medicare FFS') %>%
  write_parquet('./dist/county_opioid_by_source.parquet')

cms %>%
  filter(age =='65+ Years') %>%
  rename(opioid_rate = cms_opioid_use_disorder_overarching) %>%
  mutate(source='Medicare FFS') %>%
  left_join(all_fips, by='geography') %>%
  filter(state %in% c(state.abb,'US','DC') & geography_name %in% c(state.name, 'District of Columbia','United States')) %>%
  dplyr::select(year, geography_name,opioid_rate) %>%
  rename(geography=geography_name) %>%
  unique() %>%
  write_parquet('./dist/state_opioid_by_source.parquet')

cms_65plus_year <- cms %>%
  filter(age =='65+ Years')

wisqars_od <- wisqars_aggregated %>%
  dplyr::select(geography, age, time, wisqars_rate_drug_poisoning)

# Demographic version
wisqars_od_demo <- wisqars %>%
  left_join(all_fips, by = 'geography') %>%
  dplyr::select(geography_name, age, sex, race, ethnicity, time, wisqars_rate_drug_poisoning) %>%
  rename(geography = geography_name)

write_parquet(wisqars_od_demo, './dist/overdose_by_demographics.parquet')

nchs_od_state <- vroom::vroom('../nchs_mortality/standard/data.csv.gz') %>%
  left_join(all_fips, by='geography') %>%
  left_join(pop, by='geography') %>%
  rename(nchs_pct_complete = pct_complete,
         nchs_pct_pending_invest = pct_pending_invest) %>%
  relocate(geography_name, state, geography) %>%
  mutate(rate_deaths_overdose = n_deaths_overdose / pop *100000,
         suppressed = if_else(is.na(n_deaths_overdose),1,0)) %>%
  dplyr::select(geography, geography_name,time,n_deaths_overdose,rate_deaths_overdose)

nchs_od_state %>%
  dplyr::select(-geography) %>%
  rename(geography = geography_name) %>%
  mutate(max_date = max(time),
         max_month = month(max_date),
         month = month(time)) %>%
  filter(month==12) %>%
  dplyr::select(geography, time,n_deaths_overdose,rate_deaths_overdose) %>%
  write_parquet(.,'./dist/overdose_deaths_state.parquet')

nchs_od_county <- vroom::vroom('../nchs_mortality/standard/data_county.csv.gz') %>%
  full_join(all_fips, by='geography') %>%
  rename(nchs_pct_pending_invest = pct_pending_invest) %>%
  relocate(geography_name, state, geography) %>%
  left_join(pop, by='geography') %>%
  mutate(month=month(time),
         year= year(time),
         suppressed = if_else(is.na(n_deaths_overdose),1,0),
         n_deaths_overdose = if_else(is.na(n_deaths_overdose),5,n_deaths_overdose),
         rate_deaths_overdose = n_deaths_overdose / pop*100000
  ) %>%
  dplyr::select(geography,time,n_deaths_overdose,rate_deaths_overdose, suppressed) %>%
  unique() %>%
  filter(!is.na(time))

nchs_od_county %>%
mutate(max_date = max(time),
       max_month = month(max_date),
       month = month(time)) %>%
  filter(month==12) %>%
  dplyr::select(geography, time,n_deaths_overdose,rate_deaths_overdose) %>%
write_parquet(. ,'./dist/overdose_deaths_county.parquet')

nchs <- bind_rows(nchs_od_state, nchs_od_county)

#epic
epic <- vroom::vroom('../../data/epic_injury/standard/monthly_injury.csv.gz') %>%
  mutate( age = if_else(age == "15-25 Years", '15-24 Years', 
                        if_else(age ==  "25-45 Years", '25-44 Years', age)),
          epic_rate_ed_firearm = if_else(geography=='02',NA_real_,epic_rate_ed_firearm),
          epic_rate_ed_opioid = if_else(geography=='02',NA_real_,epic_rate_ed_opioid),
          epic_rate_ed_heat = if_else(geography=='02',NA_real_,epic_rate_ed_heat)
  )

epic_year <- vroom::vroom('../../data/epic_injury/standard/yearly_injury.csv.gz') %>%
  mutate( age = if_else(age == "15-25 Years", '15-24 Years', 
                        if_else(age ==  "25-45 Years", '25-44 Years', age)),
          epic_rate_ed_firearm = if_else(geography=='02',NA_real_,epic_rate_ed_firearm),
          epic_rate_ed_opioid = if_else(geography=='02',NA_real_,epic_rate_ed_opioid),
          epic_rate_ed_heat = if_else(geography=='02',NA_real_,epic_rate_ed_heat)
  )

## trends in overdoses
combine_long <- function() {
  drugs_month_age <- wisqars_od %>%
    left_join(cms, by = c('age', 'geography', 'time'))
  
  ### NCHS deaths in previous 12 month
  nchs_od <- nchs %>%
    rename(value = rate_deaths_overdose, nchs_n_deaths_overdose = n_deaths_overdose) %>%
    dplyr::select(geography, time, value, nchs_n_deaths_overdose, suppressed) %>%
    mutate(source = 'CDC/NCHS', age = 'Total',
           month=lubridate::month(time)) %>%
    filter(!is.na(time) & month==12) %>%
    mutate(year = lubridate::year(time),
           time = as.Date(paste(year,'07','01', sep='-'))
    )
  
  #yearly search 
  google_od <- google %>%
    arrange(time) %>%
    rename(value = gtrends_narcan) %>%
    dplyr::select(geography, time, value) %>%
    mutate(source = "Google Health Trends", age = 'Total')
  
  #yearly cms
  cms_od <- cms %>%
    rename(value = cms_opioid_use_disorder_overarching) %>%
    dplyr::select(geography, time, age, value) %>%
    mutate(source = 'Medicare FFS')
  
  #yearly death
  wisqars_od2 <- wisqars_od %>%
    rename(value = wisqars_rate_drug_poisoning) %>%
    mutate(source = 'CDC/WISQARS')
  
  #yearly epic
  epic_od <- epic_year %>%
    rename(value = epic_rate_ed_opioid) %>%
    dplyr::select(time, geography, age, value, suppressed_opioid) %>%
    mutate(source = 'Epic Cosmos')
  
  drugs_month_source <- bind_rows(nchs_od, google_od, wisqars_od2, epic_od, cms_od) %>%
    left_join(all_fips, by = 'geography') %>%
    ungroup() %>%
    rename(fips = geography) %>%
    dplyr::select(-state) %>%
    group_by(source, age, fips) %>%
    mutate(value_scale = value/max(value, na.rm=T))
  
  drugs_month_source %>%
    ungroup() %>%
    rename(date = time) %>%
    filter(geography_name %in% c('United States', 'District of Columbia', state.name)) %>%
    rename(geography = geography_name) %>%
    filter(!is.na(value)) %>%
    mutate(suppressed = if_else(is.na(suppressed),suppressed_opioid, suppressed)) %>%
    dplyr::select(geography, date,age,source, value, value_scale,suppressed ) %>%
    write_parquet(., './dist/overdose_by_geography_and_source.parquet')
  
  drugs_month_source %>%
    ungroup() %>%
    filter(!(
      geography_name %in% c('United States', 'District of Columbia', state.name)
    )) %>%
    rename(date = time) %>%
    dplyr::select(geography_name, date, source, value) %>%
    rename(geography = geography_name) %>%
    #filter(!is.na(value)) %>%
    write_parquet(.,
                  './dist/overdose_by_geography_and_source_county.parquet')
}

combine_long()

od_county <- read_parquet('./dist/overdose_by_geography_and_source_county.parquet')

od_state <- read_parquet('./dist/overdose_by_geography_and_source.parquet')

p1 <- od_state %>%
  filter(geography=='Ohio' & age=='Total') %>%
  ggplot()+
  geom_line(aes(x=date, y=value_scale, group=source, color=source)) +
  theme_classic()
p1

plotly::ggplotly(p1)

od_state_year <- od_state %>%
  mutate(year=year(date)) %>%
  group_by(geography, age, year, source) %>%
  summarize(value_year = mean(value, na.rm=T)) %>%
  ungroup() %>%
  group_by(geography, age, source) %>%
  mutate(value_year_scale = value_year / max(value_year, na.rm=T))

od_state_year %>%
  rename(value=value_year) %>%
  dplyr::select(-value_year_scale) %>%
  write_parquet(.,
                './dist/overdose_by_geography_and_source_state_year.parquet')

od_state_year %>%
  filter(age=='Total' & geography=='United States') %>%
  ggplot() +
  geom_line(aes(x=year, y=value_year, group=source, color=source)) +
  theme_classic() +
  ylim(0,NA) +
  facet_wrap(~source, scales='free_y', ncol=1)

## Map of OD by month; county,--just take every 12th observation,
##NCHS deaths, CMS opioid use disorder
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
  filter(time=='2020-12-01') %>%
  rename(fips = geography) %>%
  plot_usmap(data=., regions='counties', values = "rate_deaths_overdose", color = NA) + 
  scale_fill_continuous(name = "Deaths/100,000 that are overdose", label = scales::comma) + 
  theme(legend.position = "right")+
  ggtitle('Overdose deaths/100000 (NCHS)')

wisqars %>%
  filter(geography=='00') %>%
  ggplot() +
  geom_line(aes(x=time, y=wisqars_rate_firearm_intentional, group=age, color=age))+
  theme_classic()

##################
##Firearm by source
########################
wisqars_firearm <- wisqars_aggregated %>%
  dplyr::select(geography, time, age, wisqars_rate_firearm_intentional, wisqars_rate_firearm_accident,
                wisqars_rate_firearm_homicide, wisqars_rate_firearm_suicide,
                wisqars_rate_firearm_legal_intervention) %>%
  pivot_longer(cols = c(wisqars_rate_firearm_intentional, wisqars_rate_firearm_accident,
                        wisqars_rate_firearm_homicide, wisqars_rate_firearm_suicide,
                        wisqars_rate_firearm_legal_intervention)) %>%
  rename(source = name)

# Demographic version
wisqars_firearm_demo <- wisqars %>%
  left_join(all_fips, by = 'geography') %>%
  dplyr::select(geography_name, time, age, sex, race, ethnicity,
                wisqars_rate_firearm_intentional, wisqars_rate_firearm_accident,
                wisqars_rate_firearm_homicide, wisqars_rate_firearm_suicide,
                wisqars_rate_firearm_legal_intervention) %>%
  rename(geography = geography_name) %>%
  pivot_longer(cols = c(wisqars_rate_firearm_intentional, wisqars_rate_firearm_accident,
                        wisqars_rate_firearm_homicide, wisqars_rate_firearm_suicide,
                        wisqars_rate_firearm_legal_intervention)) %>%
  rename(source = name)

write_parquet(wisqars_firearm_demo, './dist/firearms_by_demographics.parquet')

google_firearm <- google %>%
  dplyr::select(geography, time, gtrends_shotgun, gtrends_9mm) %>%
  pivot_longer(cols=c(gtrends_shotgun, gtrends_9mm)) %>%
  rename(source = name) %>%
  mutate(age= 'Total')

epic_firearms <- epic %>%
  dplyr::select(geography, time, age, epic_n_ed_firearm, epic_rate_ed_firearm) %>%
  mutate(source='Epic Cosmos') %>%
  rename(value = epic_rate_ed_firearm) %>%
  filter(!is.na(time))

epic_firearms_year <- epic_year %>%
  dplyr::select(geography, time, age, epic_n_ed_firearm, epic_rate_ed_firearm) %>%
  mutate(source='Epic Cosmos') %>%
  rename(value = epic_rate_ed_firearm) %>%
  filter(!is.na(time)) %>%
  mutate(year= lubridate::year(time)) %>%
  left_join(state_cw, by=c('geography')) %>%
  dplyr::select(-time, -geography, -state)  %>%
  rename(geography = geography_name) 

firearms_by_source <- bind_rows(google_firearm, epic_firearms, wisqars_firearm) %>% 
  ungroup() %>%
  rename(fips = geography) %>%
  left_join(state_cw, by=c('fips'='geography')) %>%
  rename(geography = geography_name) %>%
  dplyr::select(-fips)

write_parquet(firearms_by_source,'./dist/firearms_geography_source.parquet')

firearms_by_source_year <- firearms_by_source %>%
  filter(!grepl('Epic', source)) %>%
  ungroup() %>%
  mutate(year= lubridate::year(time)) %>%
  group_by(age, geography, source, year) %>%
  summarize(value = mean(value)) %>%
  ungroup() %>%
  bind_rows(epic_firearms_year)  %>%
  mutate( source = case_when(
    source == 'wisqars_rate_firearm_intentional' ~ 'CDC/WISQARS: Firearm (intentional)',
    source == 'wisqars_rate_firearm_accident' ~ 'CDC/WISQARS: Firearm (unintentional)',
    source == 'wisqars_rate_firearm_homicide' ~ 'CDC/WISQARS: Firearm (homicide)',
    source == 'wisqars_rate_firearm_suicide' ~ 'CDC/WISQARS: Firearm (suicide)',
    source == 'wisqars_rate_firearm_legal_intervention' ~ 'CDC/WISQARS: Firearm (legal intervention)',
    TRUE ~ source
  ))

firearms_by_source_year %>%
  write_parquet(.,
                './dist/firearms_by_geography_and_source_state_year.parquet')

## Heat related
google_heat <- google %>%
  dplyr::select(geography, time, gtrends_heat_stroke, gtrends_heat_exhaustion) %>%
  mutate(source= 'Google Health Trends') %>%
  rename(fips = geography) %>%
  ungroup() %>%
  left_join(all_fips, by = c('fips' = 'geography')) %>%
  dplyr::select(geography_name, time, starts_with('gtrends')) %>%
  pivot_longer(cols=c(starts_with('gtrends'))) %>%
  mutate( source = if_else(name=='gtrends_heat_stroke', 'Google Health Trends: Heat Stroke',
                           if_else(name=='gtrends_heat_exhaustion', 'Google Health Trends: Heat Exhaustion',
                                   NA_character_))
  ) %>%
  rename(geography = geography_name) %>%
  dplyr::select(geography, time, source, value) %>%
  mutate(age = 'Total')

epic_heat_year <- epic_year %>%
  rename(value = epic_rate_ed_heat) %>%
  mutate(source = 'Epic Cosmos') %>%
  rename(fips = geography) %>%
  left_join(state_cw, by=c('fips'='geography')) %>%
  mutate(year= lubridate::year(time)) %>%
  dplyr::select(source, year, geography_name, age, value, suppressed_heat) %>%
  rename(geography = geography_name) 

google_heat_year <- google_heat %>%
  mutate(year= lubridate::year(time)) %>%
  group_by(year, source, geography, age) %>%
  summarize(value = mean(value, na.rm=T))

heat_by_source_year <- bind_rows(google_heat_year, epic_heat_year)

#write_parquet(heat_by_source,'./dist/heat_related_geography_source.parquet')

heat_by_source_year %>%
  filter(age=='Total' & geography=='United States') %>%
  ggplot() +
  geom_line(aes(x=year, y=value, group=source, color=source)) +
  theme_classic() +
  ylim(0,NA)+
  facet_wrap(~source, scales='free_y', ncol=1)

heat_by_source_year %>%
  write_parquet(.,
                './dist/heat_by_geography_and_source_state_year.parquet')

##Google DMA 
d3 <- vroom::vroom('../gtrends/standard/data_dma_year.csv.gz') %>%
  rename(gtrends_heat_exhaustion = 'gtrends_heat+exhaustion') %>%
  dplyr::select(geography, time, gtrends_narcan, gtrends_9mm, gtrends_shotgun, gtrends_heat_exhaustion) %>%
  rename(date = time) %>%
  rename(fips = geography) %>%
  mutate(fips = as.numeric(fips))

arrow::write_parquet(d3, "dist/google_dma.parquet")