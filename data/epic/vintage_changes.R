#This file demonstrates the small variations that occur when pulling epic data over time
library(tidyverse)
library(dcf)
library(plotly)
library( patchwork)
all_commits <- dcf::dcf_get_file("./raw/covid.csv.xz", versions = TRUE)

all_commits$dt <-  as.POSIXct(all_commits$date, format = "%a %b %d %H:%M:%S %Y %z") 

all_dates <- format(all_commits$dt, "%Y-%m-%d")

sep_pattern <- "\\s*[–—−-]\\s*"

v1 <- lapply(all_dates, function(X){ 
 dfname <-   dcf::dcf_get_file("./raw/covid.csv.xz", date = X)
 dfname_all <-   dcf::dcf_get_file("./raw/all_encounters.csv.xz", date = X)
 
 df_all <- vroom::vroom(dfname_all) %>%
   mutate(year = as.character(year))
 
  df <- vroom::vroom(dfname) %>%
    mutate(year = as.character(year)) %>%
    left_join(df_all, by=c('state','age','year','week')) %>%
    mutate(vintage = X)

return(df)
 })



v2 <- bind_rows(v1) %>%
  separate(week, into = c("start_str", "end_str"),
           sep = sep_pattern, remove = FALSE, extra = "merge", fill = "right") %>%
  mutate(
    start_str = trimws(start_str),
    date_str  = paste(start_str, year),
    
    end_str = trimws(end_str),
    date_end_str  = paste(end_str, year),
    
    date = as.Date(date_str, format = "%b %d %Y"),
    date_end = as.Date(date_end_str, format = "%b %d %Y"),
    
    week_length = as.numeric(date_end - date),
    week_length = if_else(week_length<0, week_length+365, week_length),
    
    pct_covid = n_covid/ n_all_encounters
  )

p1 <- v2 %>%
  filter(week_length>=5) %>%
  filter(age=="65+ Years"  & state =="New York" ) %>%
ggplot()+
  geom_line(aes(x=date, y=n_covid, group=vintage, color=vintage))+
  theme_classic() +
  ggtitle('COVID case N by week and vintage of data pull, FL, 65+')

p2 <- v2 %>%
  filter(week_length>=5) %>%
  filter(age=="65+ Years"  & state =="New York" ) %>%
  ggplot()+
  geom_line(aes(x=date, y=pct_covid, group=vintage, color=vintage))+
  theme_classic() +
  ggtitle('COVID case pct by week and vintage of data pull, FL, 65+')

 p1+p2
 
ggplotly(p1)
ggplotly(p2)
