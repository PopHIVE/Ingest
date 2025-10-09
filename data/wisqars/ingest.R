# Load necessary libraries
library(jsonlite) # For writing JSON files
library(tidyverse)    # For data tidying

all_files <- list.files(staging_path, full.names = T)

import_wisqars <- function(staging_file,
                           staging_path = './raw/staging/',
                           raw_path = './raw/') {
  file_name  <- gsub(staging_path, '', staging_file)
  file_name <- gsub('.csv', '', file_name)
  
  data <- read_csv(staging_file)
  
  # Find the start of metadata
  # Assuming the metadata starts when most columns have NA or empty values
  metadata_start_index <- which(rowSums(is.na(data) |
                                          data == "") > (ncol(data) - 3))[1]
  
  # Separate the main data and metadata
  main_data <- data[1:(metadata_start_index - 1), ]
  metadata <- data[metadata_start_index:nrow(data), ]
  
  # Reset row names for main_data and metadata
  rownames(main_data) <- NULL
  rownames(metadata) <- NULL
  
  # Convert metadata to a list and save to a JSON file
  metadata_list <- lapply(1:nrow(metadata), function(i) {
    row_data <- metadata[i, ]
    row_data <- row_data[!is.na(row_data)]
    return(list(row_data))
  })
  
  # Flatten list structure and remove empty elements
  metadata_clean <- lapply(metadata_list, function(x)
    unlist(x))
  metadata_clean <- metadata_clean[sapply(metadata_clean, length) > 0]
  
  # Convert to a named list using the first column for keys and all subsequent columns for values
  metadata_named_list <- setNames(lapply(metadata_clean, function(x)
    x[-1]),
    sapply(metadata_clean, function(x)
      x[1]))
  
  # Save the metadata to a JSON file
  metadata_json_path <- paste0(raw_path,file_name, ".json")
  write_json(metadata_named_list, metadata_json_path, pretty = TRUE)
  
  # Tidying the main data (optional step based on your needs)
  # For instance, remove rows where 'Deaths' is '--'
  main_data <- main_data %>%
    filter(Deaths != '--')
  
  # You can also convert specific columns to numeric if needed, e.g.:
  main_data$Year <- as.numeric(main_data$Year)
  main_data$Population <- as.numeric(gsub(",", "", main_data$Population))
  main_data$`Crude Rate` <- as.numeric(gsub("\\*\\*", "", main_data$`Crude Rate`))
  main_data$`Age-Adjusted Rate` <- as.numeric(gsub("--", NA_character_, main_data$`Age-Adjusted Rate`))
  
  # Save the tidy main data to a new CSV (if needed)
  clean_data_path <- paste0(raw_path,file_name, ".csv.gz")
  vroom::vroom_write(main_data, clean_data_path)
  
  
}

lapply(all_files,import_wisqars)
