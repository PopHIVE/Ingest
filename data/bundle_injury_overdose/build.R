###NOTE 12/4: the annual files should be updated for gtrends and Epic...rather than using a mean of the weekly/monthly data, instead use a direct pull of yearly data


#Medicare FFS uses the CCW algorithms https://www2.ccwdata.org/documents/10280/19139421/chr-chronic-condition-algorithms.pdfcms
library(tidyverse)
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
                gtrends_heat_exhaustion,gtrends_heat_stroke,gtrends_narcan,gtrends_overdose,gtrends_shotgun )  

#### WISQARS data
wisqars <- vroom::vroom('../../data/wisqars/standard/data.csv.gz') %>%
  mutate( year= year(time),
          time = as.Date(paste(year, '07','01', sep='-')  ),
          age = if_else(age == "0-14 Years" , "<15 Years", age)
        ) #define based on end of period

wisqars_long_rate <- wisqars%>%
  dplyr::select(geography, age, 
                #sex, race, ethnicity, 
                year, starts_with('wisqars_rate')) %>%
  pivot_longer(starts_with('wisqars_rate')) %>%
  mutate( name = gsub('wisqars_rate_', '',name))

wisqars_long <- wisqars%>%
  dplyr::select(geography, year,age, 
                #sex, race, ethnicity, 
                starts_with('wisqars_death')) %>%
  pivot_longer(starts_with('wisqars_death'), values_to='N') %>%
  mutate( name = gsub('wisqars_deaths_', '',name)) %>%
  full_join(wisqars_long_rate, by=c('geography', 'year','age', 
                                    #'sex', 'race', 'ethnicity', 
                                    'name')) %>%
  left_join(all_fips, by='geography') %>%
  dplyr::select(-geography,state) %>%
  rename(geography = geography_name,
         cause_of_death = name) %>%
  dplyr::select(year, age, 
                #sex, race, ethnicity, 
                geography, cause_of_death, value, N)

write_parquet(wisqars_long,'./dist/deaths_cause_age.parquet')


#CMS data is annual, not by month
cms <- vroom::vroom('../../data/cms_mmd/standard/data_state_county_age.csv.gz') %>%
  dplyr::select(geography, time, age,cms_alcohol_use_disorder, cms_drug_use_disorder,
                cms_opioid_use_disorder_dx_px_based, cms_opioid_use_disorder_overarching,
                cms_tobacco_use_disorder ) %>%
  mutate(year=year(time),
         time = as.Date(paste(year, '07','01', sep='-')  )
         
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

wisqars_od <- wisqars %>%
  dplyr::select(geography, age, 
                #sex, race, ethnicity, 
                time, wisqars_rate_drug_poisoning ) 

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
  dplyr::select(geography,time,n_deaths_overdose,rate_deaths_overdose, suppressed ) %>%
  unique() %>%
  filter(!is.na(time))

write_parquet(nchs_od_county,'./dist/overdose_deaths_county.parquet' )

nchs <- bind_rows(nchs_od_state, nchs_od_county)

#epic

epic <- vroom::vroom('../../data/epic/standard/monthly_injury.csv.gz') %>%
  mutate( age = if_else(age == "15-25 Years", '15-24 Years', 
                         if_else(age ==  "25-45 Years", '25-44 Years', age
          )),
          
          #Set alaska to NA
          epic_pct_ed_firearm = if_else(geography=='02',NA_real_,epic_pct_ed_firearm),
          epic_pct_ed_opioid = if_else(geography=='02',NA_real_,epic_pct_ed_opioid),
          epic_pct_ed_heat = if_else(geography=='02',NA_real_,epic_pct_ed_heat),
          
          )



## trends in overdoses

combine_long <- function() {
  drugs_month_age <- wisqars_od %>%
    left_join(cms, by = c('age', 'geography', 'time'))
  
  ### google trends
  ### NCHS deaths in previous 12 month
  nchs_od <- nchs %>%
    rename(value = rate_deaths_overdose, nchs_n_deaths_overdose = n_deaths_overdose) %>%
    dplyr::select(geography, time, value, nchs_n_deaths_overdose , suppressed) %>%
    mutate(source = 'CDC/NCHS', age = 'Total',
           month=lubridate::month(time)) %>%
    filter(!is.na(time) & month==12) %>%
    mutate(year = lubridate::year(time),
           time = as.Date(paste(year,'07','01', sep='-')) # it is a 12 month backward ave, so for calendar year, take december, and set as mid-year estimate
           )
  
  google_od <- google %>%
    arrange(time) %>%
    rename(value = gtrends_narcan) %>%
    dplyr::select(geography, time, value) %>%
    mutate(source = "Google Health Trends", age = 'Total')
  
  cms_od <- cms %>%
    rename(value = cms_opioid_use_disorder_overarching) %>%
    dplyr::select(geography, time,age, value) %>%
    mutate(source = 'Medicare FFS')
  
  wisqars_od2 <- wisqars_od %>%
    rename(value = wisqars_rate_drug_poisoning)  %>%
    mutate(source = 'CDC/WISQARS')
  
  epic_od <- epic %>%
    rename(value = epic_pct_ed_opioid) %>%
    dplyr::select(time, geography, age, value, suppressed_opioid) %>%
    mutate(source = 'Epic Cosmos')
  
  drugs_month_source <- bind_rows(nchs_od, google_od, wisqars_od2, epic_od,cms_od) %>%
    left_join(all_fips, by = 'geography') %>%
    ungroup() %>%
    rename(fips = geography) %>%
    dplyr::select(-state) %>%
    group_by(source, age, fips) %>%
    mutate(value_scale = value/max(value, na.rm=T)) #for 12m cum ave data, use last date
  
  
  drugs_month_source %>%
    ungroup() %>%
    rename(date = time) %>%
    filter(geography_name %in% c('United States', 'District of Columbia', state.name)) %>%
    rename(geography = geography_name) %>%
    filter(!is.na(value)) %>%
    dplyr::select(geography, date,age,source, value, value_scale,suppressed, suppressed_opioid ) %>%
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

p1 <- od_state %>%
  filter(geography=='Ohio' & age=='Total') %>%
  ggplot()+
  geom_line(aes(x=date, y=value_scale, group=source, color=source)) +
  theme_classic()
p1

plotly::ggplotly(p1)

od_state_year <- od_state %>%
  mutate(year=year(date)) %>%
  group_by(geography, age, year,source) %>%
  summarize(value_year =mean(value, na.rm=T)) %>%
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
  filter(geography=='00' ) %>%
ggplot() +
  geom_line(aes(x=time, y=wisqars_rate_firearm_intentional, group=age, color=age))+
  theme_classic()

##################
##Firearm by source
########################
wisqars_firarm <- wisqars %>%
  dplyr::select(geography, time, age, 
                #sex, race, ethnicity, 
                wisqars_rate_firearm_intentional,wisqars_rate_firearm_accident ) %>%
  pivot_longer(cols=c( wisqars_rate_firearm_intentional,wisqars_rate_firearm_accident )) %>%
  rename(source= name)

# wisqars %>%
#     filter(age=='Total') %>%
#     ggplot() +
#   geom_point( aes(x=wisqars_rate_firearm_intentional, y=wisqars_rate_firearm_accident, color=time))

google_firearm <- google %>%
  dplyr::select(geography, time, gtrends_shotgun, gtrends_9mm) %>%
  pivot_longer(cols=c(gtrends_shotgun, gtrends_9mm)) %>%
  rename(source = name) %>%
  mutate(age= 'Total')

epic_firearms <- epic %>%
  dplyr::select(geography, time, age, epic_n_ed_firearm , epic_pct_ed_firearm) %>%
  mutate(source='Epic Cosmos') %>%
  rename(value =epic_pct_ed_firearm)%>%
  filter(!is.na(time))


firearms_by_source <- bind_rows(google_firearm, epic_firearms, wisqars_firarm) %>% 
  ungroup() %>%
  rename(fips = geography) %>%
  left_join(state_cw, by=c('fips'='geography')) %>%
  rename(geography = geography_name) %>%
  dplyr::select(-fips)

write_parquet(firearms_by_source,'./dist/firearms_geography_source.parquet')



firearms_by_source_year <- firearms_by_source %>%
  ungroup() %>%
  mutate(year= lubridate::year(time)) %>%
  group_by(age, geography, source, year) %>%
  summarize(value = mean(value) ) %>%
  #filter(year>=2015) %>%
  mutate(value_scale = value/max(value, na.rm=T)) 

firearms_by_source_year %>%
  filter(age=='Total' & geography=='United States') %>%
  ggplot() +
  geom_line(aes(x=year, y=value, group=source, color=source)) +
  theme_classic() +
  facet_wrap(~source, scales='free_y', ncol=1)+
  ylim(0,NA)

firearms_by_source_year %>%
  dplyr::select(-value_scale) %>%
  write_parquet(.,
                './dist/firearms_by_geography_and_source_state_year.parquet')


## Heat related
google_heat <- google %>%
  dplyr::select(geography, time,gtrends_heat_stroke,gtrends_heat_exhaustion) %>%
  mutate(source= 'Google Health Trends') %>%
  rename(fips = geography) %>%
  ungroup() %>%
    left_join(all_fips, by = c('fips' = 'geography')) %>%
  dplyr::select(geography_name, time, starts_with('gtrends')) %>%
  pivot_longer( cols=c(starts_with('gtrends'))) %>%
  mutate( source = if_else(name=='gtrends_heat_stroke', 'Google Health Trends: Heat Stroke' ,
                           if_else(name=='gtrends_heat_exhaustion', 'Google Health Trends: Heat Exhaustion' ,
                          NA_character_))
          ) %>%
  rename(geography = geography_name) %>%
  dplyr::select(geography, time, source, value ) %>%
  mutate(age = 'Total')

epic_heat <- epic %>%
  rename(value = epic_pct_ed_heat) %>%
  mutate(source = 'Epic Cosmos')%>%
  rename(fips= geography) %>%
  left_join(state_cw, by=c('fips'='geography')) %>%
  dplyr::select(source,time, geography_name, age, value, suppressed_heat) %>%
  rename(geography = geography_name)
  
heat_by_source <- bind_rows(google_heat,epic_heat)

write_parquet(heat_by_source,'./dist/heat_related_geography_source.parquet')

heat_by_source_year <- heat_by_source %>%
  mutate(year= lubridate::year(time)) %>%
  group_by(year, source, geography, age) %>%
  summarize(value = mean(value, na.rm=T))

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
  dplyr::select(geography, time, gtrends_narcan,gtrends_9mm,gtrends_shotgun,gtrends_heat_exhaustion) %>%
    rename(date = time) %>%
  rename(fips = geography) %>%
  mutate(fips = as.numeric(fips))

arrow::write_parquet(d3, "dist/google_dma.parquet")
