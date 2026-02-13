library(httr)
library(jsonlite)
library(dplyr)
library(tidyverse)

get_medicaid_data_complete <- function(dataset_id, limit = 1000) {
  
  # First, get metadata
  metadata_url <- paste0("https://data.medicaid.gov/api/1/metastore/schemas/dataset/items/", dataset_id)
  metadata_response <- GET(metadata_url)
  
  if (status_code(metadata_response) == 200) {
    metadata <- fromJSON(content(metadata_response, "text", encoding = "UTF-8"))
    cat("Dataset:", metadata$title, "\n")
  }
  
  # Try to get data with pagination
  all_data <- list()
  offset <- 0
  
  repeat {
    # Try the datastore endpoint with pagination
    data_url <- paste0("https://data.medicaid.gov/api/1/datastore/query/", 
                       dataset_id, "/0?offset=", offset, "&limit=", limit)
    
    response <- GET(data_url)
    
    if (status_code(response) != 200) {
      cat("Request failed at offset", offset, "with status:", status_code(response), "\n")
      break
    }
    
    data_raw <- content(response, "text", encoding = "UTF-8")
    data_list <- fromJSON(data_raw)
    
    # Extract results
    if ("results" %in% names(data_list) && length(data_list$results) > 0) {
      current_batch <- data_list$results
      all_data[[length(all_data) + 1]] <- current_batch
      
      cat("Downloaded batch with", nrow(current_batch), "rows (total offset:", offset, ")\n")
      
      # Check if we got less than the limit (indicating last page)
      if (nrow(current_batch) < limit) {
        break
      }
      
      offset <- offset + limit
    } else {
      cat("No more results found\n")
      break
    }
    
    # Add a small delay to be respectful to the API
    Sys.sleep(0.1)
  }
  
  # Combine all data
  if (length(all_data) > 0) {
    final_data <- do.call(rbind, all_data)
    cat("Final dataset:", nrow(final_data), "rows,", ncol(final_data), "columns\n")
    return(final_data)
  } else {
    cat("No data retrieved\n")
    return(NULL)
  }
}

#initialize dcf process
process <- dcf::dcf_process_record()

# https://data.medicaid.gov/datasets?theme%5B0%5D=Quality
data_ids <- list(
  "e85033c7-367e-467e-9e81-8e85048102b8", #2023
  "dfd13757-d763-4f7a-9641-3f06ce21b4c6", #2022
  "a058ef78-e18b-4435-94aa-b70ab6ce5904", #2021
  "fbbe1734-b448-4e5a-bc94-3f8688534741", #2020
  "e36d89c0-f62e-56d5-bc7e-b0adf89262b8", #2019
  "229d6279-e614-5353-9226-f6a6f37d06c3", #2018
  "c1028fdf-2e43-5d5e-990b-51ed03428625", #2017
  "fc3c7c14-4b08-59c2-97db-0726e478dfdf", #2016
  "45a28339-17a5-55e6-8e74-e9004fc703d8", #2015
  "2b6a0ec0-efe6-5aec-9fe4-e168b8b6f553"  #2014
)

#raw state tracking
raw_state <- digest::digest(list(
  dataset_ids = data_ids,
  timestamp = format(Sys.time(), "%Y-%m-%d")
))

#processing if raw data has changed
if (!identical(process$raw_state, raw_state)) {
  
 df_ls <- lapply(data_ids, get_medicaid_data_complete)
  names(df_ls) <- paste0('year', 2014:2023)
  
  #save the raw files
 lapply(names(df_ls), function(X) {
    vroom::vroom_write(df_ls[[X]], paste0('raw/', X, '.csv.xz'))
  })
  
  #creating standard format
  #renaming columns for consistency across years and ease
  data1 <- bind_rows(df_ls, .id = "year") %>%
    mutate(year = gsub("year", "", year)) %>%
    { if (!"measure_description" %in% names(.)) add_column(., measure_description = NA_character_) else . } %>%
    { if (!"rate_definition" %in% names(.)) add_column(., rate_definition = NA_character_) else . } %>%
    { if (!"bottom_quartile" %in% names(.)) add_column(., bottom_quartile = NA_character_) else . } %>%
    { if (!"top_quartile" %in% names(.)) add_column(., top_quartile = NA_character_) else . } %>%
    { if (!"25th_percentile" %in% names(.)) add_column(., `25th_percentile` = NA_character_) else . } %>%
    { if (!"75th_percentile" %in% names(.)) add_column(., `75th_percentile` = NA_character_) else . } %>%
    { if (!"statespecific_comments" %in% names(.)) add_column(., statespecific_comments = NA_character_) else . } %>%
    { if (!"state_specific_comments" %in% names(.)) add_column(., state_specific_comments = NA_character_) else . } %>%
    { if (!"core_set_year" %in% names(.)) add_column(., core_set_year = NA_character_) else . } %>%
    { if (!"ffy" %in% names(.)) add_column(., ffy = NA_character_) else . } %>%
    rename(
      measure_abbr = measure_abbreviation,
      value        = state_rate,
      n_states     = number_of_states_reporting,
      pct25        = bottom_quartile,
      pct75        = top_quartile
    ) %>%
    mutate(
      time_year = if_else(is.na(core_set_year) | core_set_year == "",
                          if_else(is.na(ffy) | ffy == "", year, ffy),
                          core_set_year),
      state_comments = if_else(is.na(statespecific_comments),
                               state_specific_comments,
                               statespecific_comments),
      pct25        = if_else(is.na(pct25), `25th_percentile`, pct25),
      pct75        = if_else(is.na(pct75), `75th_percentile`, pct75),
      measure_info = if_else(is.na(measure_description), rate_definition, measure_description)
    ) %>%
    select(
      time_year, state, domain, reporting_program, measure_name,
      measure_abbr, measure_info, population, methodology,
      value, n_states, median, pct25, pct75, notes, source, state_comments
    )
  
  #creating wide format
  data_wide <- data1 %>%
    mutate(
      time            = paste0(time_year, "-01-01"),
      geography       = state,
      geography_level = "s",
      age             = "Total",
      sex             = "Total",
      race_ethnicity  = "Total",
      payer = case_when(
        grepl("Medicaid", population, ignore.case = TRUE) ~ "Medicaid",
        grepl("CHIP", population, ignore.case = TRUE)     ~ "CHIP",
        TRUE                                               ~ "Total"
      ),
      sub_metric = case_when(
        grepl("Within 7 Days|7-Day|7 Days", measure_info, ignore.case = TRUE)    ~ "7d",
        grepl("Within 30 Days|30-Day|30 Days", measure_info, ignore.case = TRUE) ~ "30d",
        grepl("Initiation Phase", measure_info, ignore.case = TRUE)               ~ "init",
        grepl("Continuation|Maintenance", measure_info, ignore.case = TRUE)       ~ "cont",
        grepl("Blood Glucose and Cholesterol", measure_info, ignore.case = TRUE)  ~ "gluc_chol",
        grepl("Blood Glucose", measure_info, ignore.case = TRUE)                  ~ "gluc",
        grepl("Cholesterol", measure_info, ignore.case = TRUE)                    ~ "chol",
        TRUE ~ ""
      ),
      measure_clean = tolower(gsub("[^A-Za-z0-9]+", "_", measure_abbr)),
      value = as.numeric(if_else(value == "DS", NA_character_, value)),
      pct25 = as.numeric(if_else(pct25 == "DS", NA_character_, pct25)),
      pct75 = as.numeric(if_else(pct75 == "DS", NA_character_, pct75))
    ) %>%
    select(geography, geography_level, time, age, sex, race_ethnicity,
           payer, domain, measure_clean, sub_metric, value, pct25, pct75) %>%
    pivot_longer(cols = c(value, pct25, pct75), names_to = "stat", values_to = "val") %>%
    mutate(
      stat = case_when(
        stat == "value" ~ "rate",
        stat == "pct25" ~ "pct_25",
        stat == "pct75" ~ "pct_75"
      ),
      var_name = paste("medicaid", measure_clean, sub_metric, stat, sep = "_"),
      var_name = gsub("_+$", "", gsub("__+", "_", var_name))
    ) %>%
    select(-measure_clean, -sub_metric, -stat) %>%
    distinct(geography, time, payer, domain, var_name, .keep_all = TRUE) %>%
    pivot_wider(names_from = var_name, values_from = val) %>%
    mutate(across(starts_with("medicaid_"), as.numeric))
  
  #writing standard file
  vroom::vroom_write(data_wide, "standard/data.csv.gz", delim = ",")
  
  #record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}