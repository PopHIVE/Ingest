# read data from data source projects
# and write to this project's `dist` directory
library(tidyverse)
library(arrow)

pop <- dcf::dcf_load_census(year=2021, out_dir='./resources',overwrite=F) %>%
  mutate(`<18 Years` = `<10 Years` +   `10-18 Years`) %>%
  dplyr::select(-`<10 Years`, -`10-18 Years`)

firstup <- function(x) {
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  return(x)
}

#read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/chronic_diseases/brfss_cosmos_prevalence_compared.parquet')


#read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/chronic_diseases/brfss_prevalence_by_geography.parquet')
brfss <- vroom::vroom('../brfss/standard/data.csv.gz')

brfss_long <- brfss %>%
  rename(fips=geography) %>%
  mutate( geography = cdlTools::fips(fips, to = 'Name' ),
          geography = if_else(fips=='00','United States', geography)) %>%
  pivot_longer(
    cols = starts_with("pct_"),
    names_to = c("outcome_name", "metric"),
    names_pattern = "pct_([^_]+)_(.*)",
    values_to = "val"
  ) %>%
  pivot_wider(
    names_from = metric,
    values_from = val
  ) %>%
  mutate(outcome_name = firstup(outcome_name),
         year= lubridate::year(time),
         source='CDC BRFSS'
         ) %>%
  dplyr::select(geography, year,age, source, outcome_name, value, value_lcl, value_ucl,sample_size )

write_parquet(brfss_long,'./dist/brfss_prevalence_by_geography.parquet' )

#read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/chronic_diseases/prevalence_by_geography.parquet')

#format population size file
pop_long_state <- pop %>%
  reshape2::melt(., id.vars=c('GEOID', 'region_name')) %>%
  rename(age = variable,
         pop_2021=value
         )

pop_tot_national_age <- pop_long_state %>%
  filter(region_name %in% c(state.name,'District of Columbia')) %>%
  group_by(age) %>%
  summarize(pop_2021 = sum(pop_2021) 
  ) %>%
  ungroup() %>%
  mutate(region_name = 'United States',
         GEOID = '00')

pop_combined <- pop_long_state %>%
  filter(region_name %in% c(state.name,'District of Columbia')) %>%
  bind_rows(  pop_tot_national_age) %>%
  rename(geography=region_name)


epic_state <- vroom::vroom('../epic/standard/state_no_time.csv.gz') %>%
  rename(pct_Diabetes = 'percentage_with_base_patient_followed_by_hemoglobin_a1c_6.5%_or_more_within_10_years_(%)')%>%
filter(!is.na(pct_Diabetes) ) %>%
  dplyr::select(geography, age,pct_Diabetes,bmi_30_49.8 ,n_patients) %>%
  rename(
          pct_Obesity = bmi_30_49.8,
         fips=geography
         ) %>%
  mutate( geography = cdlTools::fips(fips, to = 'Name' ),
          geography = if_else(fips=='00','United States', geography),
          age = if_else(age=='≥65 Years','65+ Years', age)) %>%
  pivot_longer(
            cols = c(starts_with("pct_")),
            names_to = c("outcome_name"),
            names_prefix  = "pct_",
            values_to = "value"
          ) %>%
 left_join(pop_combined, by=c('age'='age','geography'='geography')) %>%
  mutate(pct_captured = ifelse(n_patients == "10 or fewer", NA, as.numeric(n_patients)/pop_2021 * 100 ),
         source='Epic Cosmos'
         ) %>%
  dplyr::select(geography, fips,age, outcome_name, source,value
                ,pct_captured,n_patients
                ) %>%
  filter(!is.na(age)) %>% #small number of records missing age; filter those out here
  rename(sample_size=n_patients) %>%
  filter( fips!='52') 

write_parquet(epic_state,'./dist/epic_prevalence_by_geography.parquet' )


## CMS state
cms_state <- vroom::vroom('../cms_mmd/standard/data_state_county_age.csv.gz') %>%
  filter(geography_level %in% c('n','s')) %>%
  dplyr::select(geography, time, age, cms_obesity, cms_diabetes) %>%
  mutate(source='Medicare_CMS') %>%
  rename(
    fips=geography
  ) %>%
  mutate( geography = cdlTools::fips(fips, to = 'Name' ),
          geography = if_else(fips=='00','United States', geography),
          age = if_else(age=='≥65 Years','65+ Years', age)) %>%
  pivot_longer(
    cols = c(starts_with("cms_")),
    names_to = c("outcome_name"),
    names_prefix  = "cms_",
    values_to = "value"
  )  %>%
  mutate(age = if_else( age=='65_plus', "65+ Years",
                  if_else(age=='85_plus', "85+ Years",
                if_else( age=="All_Ages", 'Total', paste(age, 'Years'))))
         ) %>%
  mutate(outcome_name = tools::toTitleCase(outcome_name))


## Combined file

cms_state_most_recent <- cms_state %>%
  filter(time == max(time, na.rm=T)) %>% #only take most recent year
  dplyr::select(-time)

brfss_most_recent <- brfss_long %>%
  filter(year == max(year, na.rm=T)) %>%
  dplyr::select(-year)

epic_brfss_cms_combined <- bind_rows(epic_state,brfss_most_recent,cms_state_most_recent)


write_parquet(epic_brfss_cms_combined,'./dist/prevalence_by_geography_and_source.parquet' )


#Combine CMS and BRFSS but maintain time

brfss_cms_combined_year <- cms_state %>%
  mutate(year = lubridate::year(time)) %>%
  dplyr::select(-time) %>%
  bind_rows(brfss_most_recent) %>%
  filter(age %in% c('Total',"65+ Years" )) #age groups where the datasets overlap

write_parquet(brfss_cms_combined_year,'./dist/prevalence_by_geography_year_and_source.parquet' )





# County
epic_county <- vroom::vroom('../epic/standard/county_no_time.csv.gz') %>%
  rename(pct_Obesity = bmi_30_49.8,
         pct_Diabetes = 'percentage_with_base_patient_followed_by_hemoglobin_a1c_6.5%_or_more_within_10_years_(%)',
         n_patients = n_obesity_county) %>%
  dplyr::select(geography, age,pct_Obesity,pct_Diabetes ,n_patients
                ) %>%
  filter(!is.na(pct_Diabetes) ) %>%
  mutate(  age = if_else(age=='≥65 Years','65+ Years', age)
           ) %>%
  pivot_longer(
    cols = c(starts_with("pct_")),
    names_to = c("outcome_name"),
    names_prefix  = "pct_",
    values_to = "value"
  ) %>%
  left_join(pop_long_state, by=c('age'='age','geography'='GEOID')) %>%
  mutate(pct_captured = ifelse(n_patients == "10 or fewer", NA, as.numeric(n_patients)/pop_2021 * 100 ),
         source='Epic Cosmos'
  ) %>%
  dplyr::select(geography, age, outcome_name, source,value
                ,pct_captured,n_patients
  ) %>%
  filter(!is.na(age)) %>% #small number of records missing age; filter those out here
  rename(sample_size=n_patients)

write_parquet(epic_county,'./dist/epic_prevalence_by_geography_county.parquet' )

## CMS county
cms_county <- vroom::vroom('../cms_mmd/standard/data_state_county_age.csv.gz') %>%
  filter(geography_level %in% c('c')) %>%
  dplyr::select(geography, time, age, cms_obesity, cms_diabetes) %>%
  mutate(source='Medicare_CMS') %>%
  mutate( age = if_else(age=='≥65 Years','65+ Years', age)) %>%
  pivot_longer(
    cols = c(starts_with("cms_")),
    names_to = c("outcome_name"),
    names_prefix  = "cms_",
    values_to = "value"
  )  %>%
  mutate(age = if_else( age=='65_plus', "65+ Years",
                        if_else(age=='85_plus', "85+ Years",
                                if_else( age=="All_Ages", 'Total', paste(age, 'Years'))))
  ) %>%
  filter(time == max(time, na.rm=T)) %>% #only take most recent year
  dplyr::select(-time)%>%
  mutate(outcome_name = tools::toTitleCase(outcome_name))

epic_cms_county_combine <- bind_rows(cms_county,epic_county)
write_parquet(epic_cms_county_combine,'./dist/epic_prevalence_by_geography_county_and_source.parquet' )

# 
# wide_state_compare <- epic_brfss_cms_combined %>%
#   filter(!is.na(geography)) %>%
#   pivot_wider(id_cols = c(geography,  age), names_from=c(source,outcome_name), values_from=value)
# 
# a <- wide_state_compare %>%
#   filter(age=="65+ Years") %>%
# ggplot()+
#   geom_point(aes(x=Medicare_CMS_Obesity, y= `Epic Cosmos_Obesity`) )+
#   xlim(8,50) +
#   ylim(8,50)+
#   theme_minimal()+
#   ggtitle('Obesity in Epic Cosmos vs Medicare')+
#   geom_abline(slope=1, intercept=0, lty=2)
# 
# b <- wide_state_compare %>%
#   filter(age=="65+ Years") %>%
# ggplot()+
#   geom_point(aes(x=Medicare_CMS_Obesity, y= `CDC BRFSS_Obesity`) )+
#   xlim(8,50) +
#   ylim(8,50)+
#   theme_minimal()+
#   ggtitle('Obesity in BRFSS vs Medicare')+
#   geom_abline(slope=1, intercept=0, lty=2)
# 
# c <- wide_state_compare %>%
#   filter(age=="65+ Years") %>%
#   ggplot()+
#   geom_point(aes(y=`Epic Cosmos_Obesity`, x= `CDC BRFSS_Obesity`) )+
#   xlim(8,50) +
#   ylim(8,50)+
#   theme_minimal()+
#   ggtitle('Obesity in Cosmos vs Medicare')+
#   geom_abline(slope=1, intercept=0, lty=2)
# 
# gridExtra::grid.arrange(a,b,c, nrow=1)
