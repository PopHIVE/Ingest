library(tidyverse)
library(dcf)
#
# Download
#
agegrps <- list(c(0,14),
                c(15,24), 
                c(25,44), 
                c(45,64) ,
                c(65,199)
)

#Use custom age groups
#violence, stratified by age and state

wisqars_downloader <- function(max_year=2023) {
  lapply(agegrps, function(X) {
    raw_file <-
      paste0("raw/violence_state_age_", X[1], "_", X[2], ".csv.xz")
    dcf::dcf_download_wisqars(
      raw_file,
      intent = "violence",
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages=F,
      race_reporting = 'none' #this allows going back before 2018
    )
    
  })
  
  #accident, stratified by age and state
  lapply(agegrps, function(X) {
    raw_file <-
      paste0("raw/accident_state_age_", X[1], "_", X[2], ".csv.xz")
    dcf::dcf_download_wisqars(
      raw_file,
      intent = "unintentional",
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages=F,
      race_reporting = 'none' #this allows going back before 2018
    )
    
  })
  
  
  #violence, stratified by age
  lapply(agegrps, function(X) {
    raw_file <- paste0("raw/violence_age_", X[1], "_", X[2], ".csv.xz")
    dcf::dcf_download_wisqars(
      raw_file,
      intent = "violence",
      group_by = c("MECH",  "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages=F,
      race_reporting = 'none' #this allows going back before 2018
    )
    
  })
  
  #accident, stratified by age
  lapply(agegrps, function(X) {
    raw_file <- paste0("raw/accident_age_", X[1], "_", X[2], ".csv.xz")
    dcf::dcf_download_wisqars(
      raw_file,
      intent = "unintentional",
      group_by = c("MECH",  "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages=F,
      race_reporting = 'none' #this allows going back before 2018
    )
    
  })
  
  
  
  #violence, stratified by state
  dcf::dcf_download_wisqars(
    "raw/violence_state.csv.xz",
    intent = "violence",
    group_by = c("MECH", "STATE",  "YEAR"),
    year_start = 2001,
    year_end = max_year,
    race_reporting = 'none' #this allows going back before 2018
  )
  
  #accident, stratified by state
  dcf::dcf_download_wisqars(
    "raw/accident_state.csv.xz",
    intent = "unintentional",
    group_by = c("MECH", "STATE",  "YEAR"),
    year_start = 2001,
    year_end = max_year,
    race_reporting = 'none' #this allows going back before 2018
  )
  
  #Overall
  dcf::dcf_download_wisqars(
    "raw/violence.csv.xz",
    intent = "violence",
    group_by = c("MECH",  "YEAR"),
    year_start = 2001,
    year_end = max_year,
    race_reporting = 'none' #this allows going back before 2018
  )
  
  #accident, stratified by state
  dcf::dcf_download_wisqars(
    "raw/accident.csv.xz",
    intent = "unintentional",
    group_by = c("MECH",  "YEAR"),
    year_start = 2001,
    year_end = max_year,
    race_reporting = 'none' #this allows going back before 2018
  )
}

#RERESH ALL THE DATA
#wisqars_downloader(max_year=2023)
  
#
# Reformat
#

raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv",
  full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  files <- list.files("raw", pattern = "\\.csv\\.xz$", full.names = TRUE)
  
  # read and combine, adding source info
  data <- files %>%
    set_names() %>%
    map_dfr(~ vroom::vroom(.x, show_col_types = FALSE) %>%
              mutate(source = basename(.x),
                     deaths = as.character(deaths),
                     ypll = as.character(ypll),
                     CrudeRate = as.character(CrudeRate),
                     CrudeRateypll = as.character(CrudeRateypll)
                     
                     
            )
            )%>%
      
    mutate(source = str_remove(source, "\\.csv\\.xz$")) %>%
    separate_wider_delim(
      source,
      delim = "_",
      names = c("type", "level", "age1", "age2", "age3"),
      too_few = "align_start"
    ) %>%
    # combine age columns into one (if present)
    rename(agegrp= agegp) %>%
    mutate(
      CrudeRate = gsub("**","",CrudeRate, fixed=T),
      deaths = gsub("**","",deaths, fixed=T),
      CrudeRate = as.numeric(CrudeRate),
      deaths = as.numeric(deaths),
      state = replace_na(state, "00"),
      agegrp = replace_na(agegrp, "Total"),
      agegrp = gsub("<1","0", agegrp),
      agegrp = gsub("-Unknown","+", agegrp),
      agegrp = paste0(agegrp, ' Years'),
      agegrp = gsub("Total Years","Total", agegrp),
      
      
      Mechlbl = str_to_lower(
        str_replace_all(Mechlbl, "[^a-zA-Z0-9]+", "_")
      ),
      Mechlbl = if_else(Mechlbl=='firearm' & type=='accident','firearm_accident',
                        if_else(Mechlbl=='firearm' & type=='violence','firearm_intentional',
                                Mechlbl  
                        )),
      time=paste0(year, '-01-01')
          ) %>%
    rename(geography = state,
           rate = CrudeRate,
           age= agegrp
           ) %>%
    filter(grepl('firearm',Mechlbl) | type=='accident') %>%
    dplyr::group_by(type,Mechlbl) |>
    dplyr::filter(sum(!is.na(rate)) > 100, age != "Unknown") |>
    ungroup()|>
    tidyr::pivot_wider(
      id_cols = c("geography", "time", "age"),
      #names_prefix = "wisqars_",
      names_from = c("Mechlbl"),
      values_from = c("rate", "deaths")
    )
  
  vroom::vroom_write(data, "standard/data.csv.gz", ",")
  
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}