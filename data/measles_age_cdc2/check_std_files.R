library(tidyverse)

combined <- vroom::vroom('./standard/data.csv.gz')

p1 <- combined %>%
    filter(vax_group =='Total') %>%
    ggplot() +
    geom_line(aes(x = week, y = cdc_cum_cases, color = age_group)) +
    facet_wrap(~year, nrow = 1)

p2 <- combined %>%
    filter(vax_group =='Total') %>%
    ggplot() +
    geom_line(aes(x = week, y = cdc_new_cases, color = age_group)) +
    facet_wrap(~year, nrow = 1)

p3 <- combined %>%
    filter(vax_group !='Total') %>%
    ggplot() +
    geom_line(aes(x = week, y = cdc_new_cases, color = vax_group)) +
    facet_wrap(~year, nrow = 1)

p4 <- combined %>%
    filter(vax_group !='Total') %>%
    ggplot() +
    geom_line(aes(x = week, y = cdc_cum_cases, color = vax_group)) +
    facet_wrap(~year, nrow = 1)


p5 <- combined %>%
    ggplot() +
    geom_line(aes(x = week, y = cdc_new_hosp, color = age_group)) +
    facet_wrap(~year, nrow = 1)
p5

p6 <- combined %>%
    filter(vax_group =='Total') %>%
    ggplot() +
    geom_line(aes(x = week, y = cdc_cum_hosp, color = age_group)) +
    facet_wrap(~year, nrow = 1)
p6
