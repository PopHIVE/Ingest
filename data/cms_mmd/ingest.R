##Notes: written with Gemini 2.5 Pro and Claude Sonnet 4
## Data from https://data.cms.gov/tools/mapping-medicare-disparities-by-population

# --- 1. Setup: Install and Load Packages ---
library(httr2)
library(dplyr)
library(readr)
library(glue)


process <- dcf::dcf_process_record()


# --- 2. Configuration ---
base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
output_dir <- "raw"

# Create the output directory if it doesn't already exist
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

years <- 2020:2022

# Geography levels configuration
geography_levels <- list(
  'national' = 'n',
  'state' = 's', 
  'county' = 'c'
)

# --- 3. Function to Discover Available Condition Codes ---
# discover_condition_codes <- function(year = 2022, geography = 'c') {
#   message(glue("Discovering available condition codes for {year} at {geography} level..."))
#   
#   year_short <- substr(as.character(year), 3, 4)
#   
#   # Determine the correct _source pattern based on the year
#   if (year >= 2021) {
#     source_string <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}_f")
#   } else {
#     source_string <- glue("prev_final_long_fltr12_year_{year_short}_racecat_all_sexcat_all")
#   }
#   
#   # Get a small sample to examine available conditions
#   params <- list(
#     `_source` = source_string,
#     population = 'f',
#     year = year_short,
#     geography = geography,
#     measure = 'v',
#     domain = 'p',
#     `_size` = 1000  # Small sample to check structure
#   )
#   
#   tryCatch({
#     req <- request(base_url) %>%
#       req_url_query(!!!params)
#     
#     resp <- req_perform(req)
#     data_list <- resp_body_json(resp)
#     
#     if (length(data_list) == 0) {
#       message("No data returned for condition discovery.")
#       return(NULL)
#     }
#     
#     df <- bind_rows(data_list)
#     
#     # Check what fields are available
#     message("Available fields in the data:")
#     print(names(df))
#     
#     # Look for condition-related fields
#     condition_fields <- names(df)[grepl("condition|cond", names(df), ignore.case = TRUE)]
#     message(glue("Condition-related fields: {paste(condition_fields, collapse = ', ')}"))
#     
#     if (length(condition_fields) > 0) {
#       for (field in condition_fields) {
#         unique_values <- unique(df[[field]])
#         message(glue("\nUnique values in '{field}':"))
#         print(unique_values)
#       }
#     }
#     
#     return(df)
#     
#   }, error = function(e) {
#     message(glue("Failed to discover condition codes. Error: {e$message}"))
#     return(NULL)
#   })
# }

# --- 4. Function to Get All Data with Pagination ---
download_all_data_paginated <- function(condition_code, year, geography_level, page_size = 50000) {
  
  year_short <- substr(as.character(year), 3, 4)
  
  # Determine the correct _source pattern based on the year
  if (year >= 2021) {
    source_string <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}_f")
  } else {
    source_string <- glue("prev_final_long_fltr12_year_{year_short}_racecat_all_sexcat_all")
  }
  
  all_data <- list()
  offset <- 0
  page <- 1
  
  repeat {
    message(glue("      Downloading page {page} (offset: {offset})..."))
    
    params <- list(
      `_source` = source_string,
      population = 'f',
      year = year_short,
      geography = geography_level,
      measure = 'v',
      domain = 'p',
      condition = condition_code,
      `_size` = page_size,
      `_offset` = offset
    )
    
    tryCatch({
      req <- request(base_url) %>%
        req_url_query(!!!params)
      
      resp <- req_perform(req)
      data_list <- resp_body_json(resp)
      
      if (length(data_list) == 0) {
        message(glue("      No more data returned. Stopping pagination."))
        break
      }
      
      df_page <- bind_rows(data_list)
      all_data[[page]] <- df_page
      
      message(glue("      Page {page}: {nrow(df_page)} records"))
      
      # If we got fewer records than requested, we've reached the end
      if (nrow(df_page) < page_size) {
        message(glue("      Reached end of data (got {nrow(df_page)} < {page_size} records)"))
        break
      }
      
      offset <- offset + page_size
      page <- page + 1
      
      # Safety check to prevent infinite loops
      if (page > 50) {
        message("      Stopping after 50 pages for safety")
        break
      }
      
      Sys.sleep(1) # Be respectful between pages
      
    }, error = function(e) {
      message(glue("      Error on page {page}: {e$message}"))
      break
    })
  }
  
  if (length(all_data) > 0) {
    combined_data <- bind_rows(all_data)
    message(glue("      Total records retrieved: {nrow(combined_data)}"))
    return(combined_data)
  } else {
    return(NULL)
  }
}

# --- 5. Discover Condition Codes ---
# message("=== STEP 1: DISCOVERING CONDITION CODES ===")
# sample_data <- discover_condition_codes(2022, 'c')  # Use county level for discovery
# 
# # Pause for user input
# message("\n=== MANUAL STEP REQUIRED ===")
# message("Please examine the output above to identify:")
# message("1. The correct field name for conditions")
# message("2. The available condition codes")
# message("3. Update the conditions list below based on what you found")
# message("\nPress Enter to continue when ready, or Ctrl+C to stop and update the code...")
# readline()

# --- 6. Define Conditions ---
conditions <- list(
  'acute_myocardial_infarction' = '2',
  'alzheimers' = '1',
  'anemia' = '147',
  'asthma' = '4',
  'atrial_fibrilation' = '11',
  'colorectal_breast_prostate_lung_cancer' = '5', #breast=78, colorectal=79, lung=80, prostate=81, endometrial=149,
  'chronic_kidney' = '12', 
  'copd' = '13', 
  'depression' = '14', 
  'diabetes' = '15', 
  'glaucoma' = '153',
  'heart_failure_non_ischemic' = '16',
  'hip_pelvic_fracture' = '152',
  'hyperlidipemia' = '18',
  'hypertension' ='17',
  'ischemic_heart_disease' = '19',
  'obesity' = '20',
  'osteoporosis' ='21',
  'parkinsons' = '155',
  'rheumoatoid_arthritis' = '3',
  'schizophrenia_and_psycotic' = '22',
  'stroke_ischemic_attack' = '23'
)

# --- 7. Main Download Logic ---
message("\n=== STEP 2: DOWNLOADING DATA ===")
message("--- Starting Comprehensive Chronic Condition Data Download ---")

all_data <- list()

for (condition_name in names(conditions)) {
  condition_code <- conditions[[condition_name]]
  
  message(glue("\nProcessing Condition: {condition_name} (Code: {condition_code})"))
  
  for (year in years) {
    message(glue("  Year: {year}"))
    
    for (geo_name in names(geography_levels)) {
      geo_code <- geography_levels[[geo_name]]
      
      message(glue("    Geography level: {geo_name} ({geo_code})"))
      
      data <- download_all_data_paginated(condition_code, year, geo_code)
      
      if (!is.null(data) && nrow(data) > 0) {
        # Add metadata columns
        data$condition_name <- condition_name
        data$condition_code <- condition_code
        data$data_year <- year
        data$geography_level <- geo_name
        
        # Save individual condition/year/geography file
        filename <- glue("{condition_name}_{year}_{geo_name}_all_combinations.csv.xz")
        filepath <- file.path(output_dir, filename)
        
        #write_csv(data, filepath)
      
        vroom::vroom_write(
          data,
          filepath,
          ","
        )
        
        message(glue("    > Success! {nrow(data)} records saved to {filepath}"))
        
        # Print summary
        if ("agecat" %in% names(data)) {
          message(glue("      - Age categories: {length(unique(data$agecat))}"))
        }
        if ("sexcat" %in% names(data)) {
          message(glue("      - Sex categories: {length(unique(data$sexcat))}"))
        }
        if ("racecat" %in% names(data)) {
          message(glue("      - Race/ethnicity categories: {length(unique(data$racecat))}"))
        }
        if ("geography" %in% names(data)) {
          message(glue("      - Geographic units: {length(unique(data$geography))}"))
        }
        
        # Store for combined dataset
        all_data[[glue("{condition_name}_{year}_{geo_name}")]] <- data
        
      } else {
        message(glue("    > No data available for {geo_name} level"))
      }
      
      Sys.sleep(1) # Be respectful between geography levels
    }
    
    Sys.sleep(2) # Be respectful between years
  }
}

# --- 8. Create Combined Datasets ---
if (length(all_data) > 0) {
  message("\n--- Creating combined datasets ---")
  
  # Create overall combined dataset
  combined_df <- bind_rows(all_data)
  combined_filename <- file.path(output_dir, "all_conditions_all_years_all_geographies_combined.csv.xz")
  #write_csv(combined_df, combined_filename)
  vroom::vroom_write(
    combined_df,
    combined_filename,
    ","
  )
  
  message(glue("Overall combined dataset with {nrow(combined_df)} total records saved to {combined_filename}"))
  
  # Create separate combined datasets by geography level
  for (geo_name in names(geography_levels)) {
    geo_data <- combined_df[combined_df$geography_level == geo_name, ]
    
    if (nrow(geo_data) > 0) {
      geo_filename <- file.path(output_dir, glue("all_conditions_all_years_{geo_name}_combined.csv.xz"))
     # write_csv(geo_data, geo_filename)
      vroom::vroom_write(
        geo_data,
        geo_filename,
        ","
      )
      message(glue("{geo_name} combined dataset with {nrow(geo_data)} records saved to {geo_filename}"))
    }
  }
  
  # Final summary
  message("\n=== FINAL SUMMARY ===")
  message(glue("- Total records: {nrow(combined_df)}"))
  message(glue("- Conditions: {length(unique(combined_df$condition_name))} ({paste(head(unique(combined_df$condition_name), 3), collapse = ', ')}{if(length(unique(combined_df$condition_name)) > 3) '...' else ''})"))
  message(glue("- Years: {paste(unique(combined_df$data_year), collapse = ', ')}"))
  message(glue("- Geography levels: {paste(unique(combined_df$geography_level), collapse = ', ')}"))
  
  # Count CSV files safely
  csv_files <- list.files(output_dir, pattern = ".*\\.csv$")
  message(glue("- Files created: {length(csv_files)}"))
  
  # Show breakdown by geography level
  geo_summary <- combined_df %>%
    group_by(geography_level) %>%
    summarise(
      records = n(),
      conditions = n_distinct(condition_name),
      years = n_distinct(data_year),
      .groups = 'drop'
    )
  
  message("\nRecords by geography level:")
  for (i in 1:nrow(geo_summary)) {
    row <- geo_summary[i, ]
    message(glue("- {row$geography_level}: {row$records} records, {row$conditions} conditions, {row$years} years"))
  }
  
  # Show breakdown by demographics if available
  if ("agecat" %in% names(combined_df)) {
    age_cats <- unique(combined_df$agecat[!is.na(combined_df$agecat)])
    message(glue("- Age categories: {length(age_cats)} ({paste(head(age_cats, 5), collapse = ', ')}{if(length(age_cats) > 5) '...' else ''})"))
  }
  
  if ("sexcat" %in% names(combined_df)) {
    sex_cats <- unique(combined_df$sexcat[!is.na(combined_df$sexcat)])
    message(glue("- Sex categories: {length(sex_cats)} ({paste(sex_cats, collapse = ', ')})"))
  }
  
  if ("racecat" %in% names(combined_df)) {
    race_cats <- unique(combined_df$racecat[!is.na(combined_df$racecat)])
    message(glue("- Race/ethnicity categories: {length(race_cats)} ({paste(head(race_cats, 3), collapse = ', ')}{if(length(race_cats) > 3) '...' else ''})"))
  }
}

message("\n--- All downloads complete! ---")

dcf::dcf_process_record(updated = process)
