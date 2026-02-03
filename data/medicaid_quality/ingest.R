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

# https://data.medicaid.gov/datasets?theme%5B0%5D=Quality
data_ids <-  list("e85033c7-367e-467e-9e81-8e85048102b8",#2023
     "dfd13757-d763-4f7a-9641-3f06ce21b4c6", #2022
     "a058ef78-e18b-4435-94aa-b70ab6ce5904", #2021
     "fbbe1734-b448-4e5a-bc94-3f8688534741", #2020
     "e36d89c0-f62e-56d5-bc7e-b0adf89262b8", #2019
     "229d6279-e614-5353-9226-f6a6f37d06c3", #2018
     "c1028fdf-2e43-5d5e-990b-51ed03428625", #2017
     "fc3c7c14-4b08-59c2-97db-0726e478dfdf", #2016
     "45a28339-17a5-55e6-8e74-e9004fc703d8", #2015
     "2b6a0ec0-efe6-5aec-9fe4-e168b8b6f553" #2014
     )

df_ls <- lapply(data_ids, get_medicaid_data_complete)

names(df_ls) <- paste0('year',2014:2023)

#save the raw files
lapply(names(df_ls), function(X) vroom::vroom_write(df_ls[[X]], paste0('./raw/',X, '.csv.gz') ))

df_all <- bind_rows(df_ls)

vroom::vroom_write(df_all, './raw/all_years.csv.gz')
###########################################################################
###########################################################################

df_all <- vroom::vroom( './raw/all_years.csv.gz')

df_all <- df_all %>%
  rename(year= core_set_year) %>%
  mutate(year = if_else(is.na(year), ffy,year))

df_all <- vroom::vroom( './raw/all_years.csv.gz')

df_all <- df_all %>%
  rename(year= core_set_year) %>%
  mutate(year = if_else(is.na(year), ffy,year))

##stratified files

#create stratified directory
if(!dir.exists('./raw/stratified')) dir.create('./raw/stratified')

#by domain
cat("\nCreating domain-stratified files...\n")
df_all %>%
  filter(!is.na(domain)) %>%
  group_by(domain) %>%
  group_walk(~ vroom::vroom_write(.x, paste0('./raw/stratified/domain_', 
                                             gsub(" |/", "_", .y$domain), '.csv.gz')))

#by population
cat("Creating population-stratified files...\n")
df_all %>%
  filter(!is.na(population)) %>%
  group_by(population) %>%
  group_walk(~ vroom::vroom_write(.x, paste0('./raw/stratified/pop_', 
                                             gsub(" |/", "_", .y$population), '.csv.gz')))

#by measure type
cat("Creating measure-type-stratified files...\n")
df_all %>%
  filter(!is.na(measure_type)) %>%
  group_by(measure_type) %>%
  group_walk(~ vroom::vroom_write(.x, paste0('./raw/stratified/type_', 
                                             gsub(" |/", "_", .y$measure_type), '.csv.gz')))

#2020 and beyond...
cat("Creating recent years file (2020+)...\n")
df_all %>%
  filter(year >= 2020) %>%
  vroom::vroom_write('./raw/recent_2020plus.csv.gz')

#summary of stratifications
cat("\n=== STRATIFICATION SUMMARY ===\n")
cat("Files created in ./raw/stratified/\n\n")

cat("By domain:\n")
df_all %>% count(domain, sort = TRUE) %>% print()

cat("\nBy population:\n")
df_all %>% count(population, sort = TRUE) %>% print()

cat("\nBy measure type:\n")
df_all %>% count(measure_type, sort = TRUE) %>% print()

