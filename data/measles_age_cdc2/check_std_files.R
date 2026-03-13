library(tidyverse)

combined <- vroom::vroom('./standard/data.csv.gz')

combined %>% 
    filter(vax_group =='Total') %>%
    ggplot() +
geom_line(aes(x = week, y = cdc_cum_cases, color = age_group)) +
facet_wrap(~year)

combined %>% 
    filter(vax_group =='Total') %>%
    ggplot() +geom_line(aes(x = week, y = cdc_new_cases, color = age_group)) +
facet_wrap(~year)



combined %>% 
    filter(vax_group !='Total') %>%
    ggplot() +geom_line(aes(x = week, y = cdc_new_cases, color = vax_group)) +
facet_wrap(~year)

combined %>% 
    filter(vax_group !='Total') %>%
    ggplot() +geom_line(aes(x = week, y = cdc_cum_cases, color = vax_group)) +
facet_wrap(~year)
