
# Process staging data

# if there was staging data, make new standard version from it..this function will automaticaly save relevant file
raw <- dcf::dcf_process_epic_staging(cleanup=T)

if (!is.null(raw)) {
  files <- list.files("raw", "\\.csv\\.xz", full.names = TRUE)
  data <- lapply(files, function(file) {
    d <- vroom::vroom(file, show_col_types = FALSE, guess_max = Inf)
    
    if(grepl('self_harm',file)){
      d <- d %>%
        rename( n_all_encounters = n_self_harm,
                pct_self_harm = "percent_with_self-harm_dx_(%)")
    }
    
    d2 <- dcf::dcf_standardize_epic(d)
    
    if('month' %in% names(d)){
      d2$time = paste0(d2$time, '-01')
    }
    
    if ("geography" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(geography = if_else(geography=='0','00', geography) 
      )
    }
    
    if ("time" %in% names(d2)) {
      
      d2 <- d2 %>%
        mutate(time = as.Date(time),
               weekday = weekdays(time),
             time = if_else(weekday=='Sunday', time+6, time), #week end date
             time = as.character(time)
      ) %>%
        dplyr::select(-weekday) 
    }
    
    if ("n_obesity_state" %in% names(d2)) {
      d2 <- d2 %>%
        rename(n_patients = n_obesity_state) 
    }

    if ("age" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(age = stringr::str_replace(age, "^Less than\\s+(\\d+)", "<\\1 Years"),
             age = stringr::str_replace(age, "^(\\d+) or more$", "≥\\1 Years"),
             age = stringr::str_replace(age, "^≥\\s*(\\d+) and <\\s*(\\d+)$", "\\1-\\2 Years"),
             age = if_else( 
               age=="1-5 Years", "1-4 Years",
               if_else( age=="5-18 Years", "5-17 Years",
                        if_else(age=="18-25 Years", "18-24 Years",
                                if_else(age=="18-50 Years", "18-49 Years",
                            if_else( age=="25-35 Years" , "25-34 Years",
                                     if_else( age=="35-45 Years", "35-44 Years",
                                              if_else( age=="45-55 Years", "45-54 Years",
                                                       if_else( age=="50-65 Years", "50-64 Years",
                                                       if_else( age=="55-65 Years", "55-64 Years",
                                                                if_else( age=="≥65 Years", "65+ Years",
                                                                         age
                            ))))))))))
          
             )
       }
    
    return(d2)
  })
  names(data) <- sub("\\..*", "", basename(files))

  merged_weekly <- Reduce(
    function(a, b) merge(a, b, all = TRUE, sort = FALSE),
    data[c("all_encounters", "covid", "flu", "rsv", "rsv_tests")]
  )
  
  # add epic_ prefix to all columns except geography, time, age
  merged_weekly <- merged_weekly %>%
    mutate(pct_rsv = 100*n_rsv/n_all_encounters,
           pct_flu = 100*n_flu/n_all_encounters,
           pct_covid = 100*n_covid/n_all_encounters,
           ) %>%
    rename_with(~ paste0("epic_", .x), 
                .cols = -c(geography, time, age))%>%
    arrange(geography, age, time) %>%
    group_by(geography, age) %>%
    mutate(time= as.Date(time),
           epic_n_all_encounters_lag1 = lag(epic_n_all_encounters,1),
           remove = if_else(epic_n_all_encounters/epic_n_all_encounters_lag1<0.5 &
                              time == max(time, na.rm=T),1,0)
    ) %>%
    filter(remove != 1) %>%
    dplyr::select(-remove, -epic_n_all_encounters_lag1)
    
  
  vroom::vroom_write(
    merged_weekly,
    "standard/weekly.csv.gz",
    ","
  )
  
  #Monthly data
  merged_monthly <- Reduce(
    function(a, b) merge(a, b, all = TRUE, sort = FALSE),
    data[c("self_harm")]
  )
  
  # add epic_ prefix to all columns except geography, time, age
  merged_monthly <- merged_monthly %>%
    rename_with(~ paste0("epic_", .x), 
                .cols = -c(geography, time, age))%>%
    arrange(geography, age, time) %>%
    group_by(geography, age) %>%
    mutate(time= as.Date(time),
           epic_n_all_encounters_lag1 = lag(epic_n_all_encounters,1),
           remove = if_else(epic_n_all_encounters/epic_n_all_encounters_lag1<0.5 &
                              time == max(time, na.rm=T),1,0)
    ) %>%
    filter(remove != 1) %>%
    dplyr::select(-remove, -epic_n_all_encounters_lag1)
  
  
  vroom::vroom_write(
    merged_monthly,
    "standard/monthly.csv.gz",
    ","
  )
  
  vroom::vroom_write(
    Reduce(
      function(a, b) merge(a, b, all = TRUE, sort = FALSE),
      data[c("obesity_state")]
    ),
    "standard/state_no_time.csv.gz",
    ","
  )
  vroom::vroom_write(data$obesity_county, "standard/county_no_time.csv.gz", ",")
  vroom::vroom_write(data$rsv_tests, "standard/no_geo.csv.gz", ",")
  vroom::vroom_write(data$vaccine_mmr, "standard/children.csv.gz", ",")
}

#Test
# merged_weekly %>%
#   filter(geography=='00' & age=='Total') %>%
#   mutate(time=as.Date(time)) %>%
# ggplot(aes(x=time, y=epic_n_rsv))+
#   geom_line()
# merged_weekly %>%
#   filter(geography=='00' & age=='Total') %>%
#   mutate(time=as.Date(time)) %>%
#   ggplot(aes(x=time, y=epic_n_flu))+
#   geom_line()
# 
# # merged_weekly %>%
# #   filter(geography=='00' & age=='Total') %>%
# #   mutate(time=as.Date(time)) %>%
# #   ggplot(aes(x=time, y=epic_n_covid))+
# #   geom_line()
# # 
# merged_weekly %>%
#   filter(geography=='00' & age=='Total') %>%
#   mutate(time=as.Date(time)) %>%
#   ggplot(aes(x=time, y=epic_n_all_encounters))+
#   geom_line()
