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

  # Helper function to download all 9 patterns for a given intent
  # intent_name: prefix for raw filenames (e.g., "violence", "homicide")
  # intent_value: API intent parameter (e.g., "violence", "homicide", "legal")
  download_intent <- function(intent_name, intent_value, max_year) {

    # Pattern 1: state × age
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_state_age_", X[1], "_", X[2], ".csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR"),
        year_start = 2001,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 2: age only (national)
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_age_", X[1], "_", X[2], ".csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "YEAR"),
        year_start = 2001,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 3: state × age × sex
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_state_age_", X[1], "_", X[2], "_sex.csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "SEX"),
        year_start = 2001,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 4: state × age × race (2018+ only)
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_state_age_", X[1], "_", X[2], "_race.csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "RACE"),
        year_start = 2018,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'single'
      )
    })

    # Pattern 5: state × age × ethnicity
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_state_age_", X[1], "_", X[2], "_ethnicity.csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "ETHNICTY"),
        year_start = 2001,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 6: state × age × sex × ethnicity
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_state_age_", X[1], "_", X[2], "_sex_ethnicity.csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "SEX", "ETHNICTY"),
        year_start = 2001,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 7: state × age × sex × race × ethnicity (2018+ only)
    lapply(agegrps, function(X) {
      dcf::dcf_download_wisqars(
        paste0("raw/", intent_name, "_state_age_", X[1], "_", X[2], "_sex_race_ethnicity.csv.xz"),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "SEX", "RACE", "ETHNICTY"),
        year_start = 2018,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'single'
      )
    })

    # Pattern 8: state only (all ages)
    dcf::dcf_download_wisqars(
      paste0("raw/", intent_name, "_state.csv.xz"),
      intent = intent_value,
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      race_reporting = 'none'
    )

    # Pattern 9: national overall (all ages)
    dcf::dcf_download_wisqars(
      paste0("raw/", intent_name, ".csv.xz"),
      intent = intent_value,
      group_by = c("MECH", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      race_reporting = 'none'
    )
  }

  # Existing intents
  download_intent("violence", "violence", max_year)
  download_intent("accident", "unintentional", max_year)

  # New granular violence sub-intents
  download_intent("homicide", "homicide", max_year)
  download_intent("suicide", "suicide", max_year)
  download_intent("legal", "legal", max_year)

  ############################################################
  # Special mechanism-specific downloads (cycling and pedestrian)
  # These are specific to unintentional injuries and don't apply to violence sub-intents

  #cycling accident with MV, stratified by age and state
  lapply(agegrps, function(X) {
    dcf::dcf_download_wisqars(
      paste0("raw/cycle_accident_state_age_", X[1], "_", X[2], ".csv.xz"),
      mechanism = 20980,
      intent = "unintentional",
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  lapply(agegrps, function(X) {
    dcf::dcf_download_wisqars(
      paste0("raw/cycle_accident_age_", X[1], "_", X[2], ".csv.xz"),
      mechanism = 20980,
      intent = "unintentional",
      group_by = c("MECH", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  dcf::dcf_download_wisqars(
    "raw/cycle_accident.csv.xz",
    mechanism = 20980,
    intent = "unintentional",
    group_by = c("MECH", "YEAR"),
    year_start = 2001,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )

  dcf::dcf_download_wisqars(
    "raw/cycle_accident_state.csv.xz",
    mechanism = 20980,
    intent = "unintentional",
    group_by = c("MECH", "STATE", "YEAR"),
    year_start = 2001,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )

  ###########################################################
  #pedestrian accident with MV, stratified by age and state
  lapply(agegrps, function(X) {
    dcf::dcf_download_wisqars(
      paste0("raw/ped_accident_state_age_", X[1], "_", X[2], ".csv.xz"),
      mechanism = 21010,
      intent = "unintentional",
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  lapply(agegrps, function(X) {
    dcf::dcf_download_wisqars(
      paste0("raw/ped_accident_age_", X[1], "_", X[2], ".csv.xz"),
      mechanism = 21010,
      intent = "unintentional",
      group_by = c("MECH", "YEAR"),
      year_start = 2001,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  dcf::dcf_download_wisqars(
    "raw/ped_accident.csv.xz",
    mechanism = 21010,
    intent = "unintentional",
    group_by = c("MECH", "YEAR"),
    year_start = 2001,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )

  dcf::dcf_download_wisqars(
    "raw/ped_accident_state.csv.xz",
    mechanism = 21010,
    intent = "unintentional",
    group_by = c("MECH", "STATE", "YEAR"),
    year_start = 2001,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
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
  #test <- vroom::vroom('./raw/ped_accident_state.csv.xz')
  
  data <- files %>%
    set_names() %>%
    map_dfr(~ vroom::vroom(.x, show_col_types = FALSE) %>%
              dplyr::select(-any_of(c("ageadj", "ageadjypll"))) %>%
         mutate(source = basename(.x),
                     deaths = as.character(deaths),
                     ypll = as.character(ypll),
                     CrudeRate = as.character(CrudeRate),
                     CrudeRateypll = as.character(CrudeRateypll)
                    # ageadj = as.character(ageadj),
                    # ageadjypll = as.character(ageadjypll),
                     
            )
            )%>%
      
    mutate(source = str_remove(source, "\\.csv\\.xz$"),
           agegp = if_else(agegp=='<1-Unknown', NA_character_, agegp) ) %>%
    separate_wider_delim(
      source,
      delim = "_",
      names = c("type", "level", "age1", "age2", "age3", "demographic"),
      too_few = "align_start",
      too_many = "merge"
    ) %>%
    {
      # temporary fix to missing columns
      if (!"sex" %in% names(.)) .$sex <- NA
      if (!"race" %in% names(.)) .$race <- NA  
      # WISQARS uses "ethnicty" (typo) as column name
      if ("ethnicty" %in% names(.)) . <- rename(., ethnicity = ethnicty)
      if (!"ethnicity" %in% names(.)) .$ethnicity <- NA
      .
    } %>%
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
      sex = case_when(
        demographic == "sex" | grepl("sex", demographic) ~ case_when(
          sex == 1 ~ "Male",
          sex == 2 ~ "Female",
          TRUE ~ "All"
        ),
        TRUE ~ "All"
      ),
      race = case_when(
        demographic == "race" | grepl("race", demographic) ~ case_when(
          race == "01" | race == 1 ~ "White",
          race == "02" | race == 2 ~ "Black",
          race == "03" | race == 3 ~ "American Indian/Alaska Native",
          race == "04" | race == 4 ~ "Asian",
          race == "05" | race == 5 ~ "Native Hawaiian/Pacific Islander",
          race == "06" | race == 6 ~ "More than one race",
          TRUE ~ "All"
        ),
        TRUE ~ "All"
      ),
      ethnicity = case_when(
        demographic == "ethnicity" | grepl("ethnicity", demographic) ~ case_when(
          ethnicity == 1 ~ "Non-Hispanic",
          ethnicity == 2 ~ "Hispanic",
          ethnicity == 3 ~ "Unknown",
          TRUE ~ "All"
        ),
        TRUE ~ "All"
      ),
      
      Mechlbl = str_to_lower(
        str_replace_all(Mechlbl, "[^a-zA-Z0-9]+", "_")
      ),
      Mechlbl = case_when(
        Mechlbl == 'firearm' & type == 'accident'  ~ 'firearm_accident',
        Mechlbl == 'firearm' & type == 'violence'  ~ 'firearm_intentional',
        Mechlbl == 'firearm' & type == 'homicide'  ~ 'firearm_homicide',
        Mechlbl == 'firearm' & type == 'suicide'   ~ 'firearm_suicide',
        Mechlbl == 'firearm' & type == 'legal'     ~ 'firearm_legal_intervention',
        TRUE ~ Mechlbl
      ),
      time=paste0(year, '-01-01')
          ) %>%
    rename(geography = state,
           rate = CrudeRate,
           age= agegrp
           ) %>%
    filter(grepl('firearm',Mechlbl) | type=='accident'|type=='cycle'|type=='ped') %>%
    dplyr::group_by(type,Mechlbl) |>
    dplyr::filter(sum(!is.na(rate)) > 100, age != "Unknown", Mechlbl!='.', Mechlbl!='_') |>
    ungroup()|>
    tidyr::pivot_wider(
      id_cols = c("geography", "time", "age",  "sex", "race", "ethnicity"),
      #names_prefix = "wisqars_",
      names_from = c("Mechlbl"),
      values_from = c("rate", "deaths")
    )
  data <- data %>%
    rename_with(~ paste0("wisqars_", .x),
                .cols = which(grepl("^(rate_|deaths_)", names(data))))
  
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
