#Queries
#Unntentional by state and age and Mechanism, 2023:
#https://wisqars.cdc.gov/reports/?o=MORT&y1=2023&y2=2023&t=0&i=1&m=20810&g=00&me=0&s=0&r=0&ry=2&e=0&yp=65&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=NONE

#Violence related by state and age and mechanism, 2023
#https://wisqars.cdc.gov/reports/?o=MORT&y1=2023&y2=2023&t=0&i=8&m=20810&g=00&me=0&s=0&r=0&ry=2&e=0&yp=65&a=ALL&g1=0&g2=199&a1=0&a2=199&r1=MECH&r2=AGEGP&r3=STATE&r4=NONE

#NVDRS intent, age, state
#https://wisqars.cdc.gov/nvdrs/?rt=3&rt2=0&y=2022&g=00&i=0&m=20810&s=0&r=0&e=0&rl=0&pc=0&pr=0&h=0&ml=0&a=ALL&a1=0&a2=199&g1=0&g2=199&r1=NVDRS-INTENT&r2=AGEGP&r3=STATE&r4=NONE

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

# separate the metadata and data, move to raw

lapply(all_files,import_wisqars)


a1 <- vroom::vroom('./raw/intentional_age_2023.csv.gz') %>%
  rename(age = 'Age Group') %>%
  mutate(Mechanism = str_to_lower(
    str_replace_all(Mechanism, "[^a-zA-Z0-9]+", "_")
  )) %>%
  filter(Mechanism =='firearm') %>%
  ungroup() %>%
  pivot_wider(id_cols= c(age,Year, State), names_prefix='rate_intentional_',names_from=Mechanism, values_from=`Crude Rate`) 

a2 <- vroom::vroom('./raw/unintentional_age_2023.csv.gz') %>%
  rename(age = 'Age Group') %>%
  mutate(Mechanism = str_to_lower(
    str_replace_all(Mechanism, "[^a-zA-Z0-9]+", "_")
  )) %>%
  group_by(Mechanism) %>%
  mutate( n_group =n(),
          n_nonmiss = sum(!is.na(`Crude Rate`)),
          keep = n_nonmiss > 100) %>% #remove if missing more than half
  filter(keep==1) %>%
  ungroup() %>%
  pivot_wider(id_cols= c(age, Year, State), names_prefix='rate_unintentional_',names_from=Mechanism, values_from=`Crude Rate`) 

geocodes <- vroom::vroom('../../resources/all_fips.csv.gz') %>%
  filter(geography_name %in% state.name)

b1 <- a1 %>%
  full_join(a2, by=c('age', 'Year', 'State')) %>%
  mutate(time = paste0(Year,'-01-01')
         ) %>%
  rename(geography_name = State) %>%
  left_join(geocodes ,by='geography_name') %>%
  relocate(geography, age, time) %>%
  dplyr::select(-state, -geography_name)

vroom::vroom_write(b1, './standard/data.csv.gz')


