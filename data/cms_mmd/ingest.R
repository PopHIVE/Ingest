##Notes: written with Gemini 2.5 Pro and Claude Sonnet 4
## Data from https://data.cms.gov/tools/mapping-medicare-disparities-by-population Mapping Medicare Disparities

# --- 1. Setup: Install and Load Packages ---
library(httr2)
library(dplyr)
library(vroom)  # Changed from readr to vroom
library(glue)

# --- 2. Configuration ---
base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
output_dir <- "raw/staging"

# Create the output directory if it doesn't already exist
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

years <- 2020:2023

# Geography levels configuration
geography_levels <- list(
  'national' = 'n',
  'state' = 's', 
  'county' = 'c'
)

# --- 3. Define All Conditions ---
conditions <- list(
  'acute_myocardial_infarction' = '2',
  'alzheimers' = '1',
  'anemia' = '147',
  'asthma' = '4',
  'atrial_fibrilation' = '11',
  'colorectal_breast_prostate_lung_cancer' = '5',
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

# --- 4. Year-Specific Source Pattern Function ---
get_source_string <- function(year) {
  year_short <- substr(as.character(year), 3, 4)
  
  if (year >= 2023) {
    # 2023+ pattern ends with _p instead of _f
    return(glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}_p"))
  } else if (year >= 2021) {
    # 2021-2022 pattern ends with _f
    return(glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}_f"))
  } else {
    # 2020 and earlier pattern
    return(glue("prev_final_long_fltr12_year_{year_short}_racecat_all_sexcat_all"))
  }
}

# --- 5. Download Function ---
download_all_data_paginated <- function(condition_code, year, geography_level, page_size = 50000) {
  
  year_short <- substr(as.character(year), 3, 4)
  source_string <- get_source_string(year)
  
  message(glue("      Using source pattern: {source_string}"))
  
  all_data <- list()
  offset <- 0
  page <- 1
  
  repeat {
    message(glue("      Downloading page {page} (offset: {offset})..."))
    
    # Base parameters that work for all years
    params <- list(
      `_source` = source_string,
      population = 'f',
      year = year_short,
      geography = geography_level,
      measure = 'v',
      condition = condition_code,
      fltr = '1',
      `_size` = page_size,
      `_offset` = offset
    )
    
    # Add domain parameter for 2021+
    if (year >= 2021) {
      params$domain <- 'p'
    }
    
    # Add demographic filters to get all combinations
    params$sexcat <- '.|IS NULL'
    params$agecat <- '.|IS NULL'
    params$dual <- '.|IS NULL'
    params$eligcat <- '.|IS NULL'
    params$racecat <- '.|IS NULL'
    
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
      
      if (nrow(df_page) < page_size) {
        message(glue("      Reached end of data (got {nrow(df_page)} < {page_size} records)"))
        break
      }
      
      offset <- offset + page_size
      page <- page + 1
      
      if (page > 50) {
        message("      Stopping after 50 pages for safety")
        break
      }
      
      Sys.sleep(1)
      
    }, error = function(e) {
      if (grepl("404", e$message)) {
        message(glue("      404 Error - data may not be available for this combination"))
      } else {
        message(glue("      Error on page {page}: {e$message}"))
      }
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

# --- 6. Main Download Logic for All Combinations ---
message("=== STARTING COMPREHENSIVE DOWNLOAD ===")
message("--- All Conditions × All Years × All Geography Levels ---")
message(glue("Total combinations to process: {length(conditions)} × {length(years)} × {length(geography_levels)} = {length(conditions) * length(years) * length(geography_levels)}"))

# Calculate total combinations for progress tracking
total_combinations <- length(conditions) * length(years) * length(geography_levels)
current_combination <- 0
all_data <- list()

# Triple nested loop: Conditions × Years × Geography Levels
for (condition_name in names(conditions)) {
  condition_code <- conditions[[condition_name]]
  
  message(glue("\n🏥 Processing Condition: {condition_name} (Code: {condition_code})"))
  message(glue("   [{match(condition_name, names(conditions))}/{length(conditions)}] conditions"))
  
  for (year in years) {
    message(glue("\n  📅 Year: {year}"))
    message(glue("     [{match(year, years)}/{length(years)}] years for {condition_name}"))
    
    for (geo_name in names(geography_levels)) {
      geo_code <- geography_levels[[geo_name]]
      current_combination <- current_combination + 1
      
      message(glue("\n    🌍 Geography: {geo_name} ({geo_code})"))
      message(glue("       Progress: {current_combination}/{total_combinations} ({round(100*current_combination/total_combinations, 1)}%)"))
      
      # Download data for this specific combination
      data <- download_all_data_paginated(condition_code, year, geo_code)
      
      if (!is.null(data) && nrow(data) > 0) {
        # Add metadata columns
        data$condition_name <- condition_name
        data$condition_code <- condition_code
        data$data_year <- year
        data$geography_level <- geo_name
        
        # Save individual file with xz compression
        filename <- glue("{condition_name}_{year}_{geo_name}_all_combinations.csv.xz")
        filepath <- file.path(output_dir, filename)
        vroom_write(data, filepath, delim = ",")
        
        # Get file size for reporting
        file_size <- file.size(filepath)
        file_size_mb <- round(file_size / 1024^2, 2)
        
        message(glue("    ✅ Success! {nrow(data)} records saved to {filename} ({file_size_mb} MB compressed)"))
        
        # Print summary statistics
        if ("agecat" %in% names(data)) {
          age_cats <- length(unique(data$agecat[!is.na(data$agecat)]))
          message(glue("       - Age categories: {age_cats}"))
        }
        if ("sexcat" %in% names(data)) {
          sex_cats <- length(unique(data$sexcat[!is.na(data$sexcat)]))
          message(glue("       - Sex categories: {sex_cats}"))
        }
        if ("racecat" %in% names(data)) {
          race_cats <- length(unique(data$racecat[!is.na(data$racecat)]))
          message(glue("       - Race/ethnicity categories: {race_cats}"))
        }
        if ("geography" %in% names(data)) {
          geo_units <- length(unique(data$geography[!is.na(data$geography)]))
          message(glue("       - Geographic units: {geo_units}"))
        }
        
        # Store for combined datasets
        key <- glue("{condition_name}_{year}_{geo_name}")
        all_data[[key]] <- data
        
      } else {
        message(glue("    ❌ No data available for {condition_name} {year} {geo_name}"))
      }
      
      # Be respectful to the server
      Sys.sleep(1)
    }
    
    # Longer pause between years
    Sys.sleep(2)
  }
  
  # Even longer pause between conditions
  Sys.sleep(3)
}

# --- 7. Create Combined Datasets ---
if (length(all_data) > 0) {
  message("\n=== CREATING COMBINED DATASETS ===")
  
  # Create overall combined dataset
  message("Creating overall combined dataset...")
  combined_df <- bind_rows(all_data)
  combined_filename <- file.path( "./raw/ALL_CONDITIONS_ALL_YEARS_ALL_GEOGRAPHIES_COMBINED.csv.xz")
  vroom_write(combined_df, combined_filename, delim = ",")
  
  combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
  message(glue("✅ Overall combined dataset: {format(nrow(combined_df), big.mark=',')} records → {basename(combined_filename)} ({combined_size_mb} MB compressed)"))
  
  
  # --- 8. Final Comprehensive Summary ---
  #message("\n" + paste(rep("=", 80), collapse=""))
  message("=== FINAL COMPREHENSIVE SUMMARY ===")
  message(paste(rep("=", 80), collapse=""))
  
  message(glue("📊 OVERALL STATISTICS"))
  message(glue("   Total records downloaded: {format(nrow(combined_df), big.mark=',')}"))
  message(glue("   Conditions processed: {length(unique(combined_df$condition_name))} of {length(conditions)}"))
  message(glue("   Years covered: {paste(sort(unique(combined_df$data_year)), collapse = ', ')}"))
  message(glue("   Geography levels: {paste(unique(combined_df$geography_level), collapse = ', ')}"))
  
  # Files created summary with compression info
  csv_files <- list.files(output_dir, pattern = ".*\\.csv\\.xz$")
  individual_files <- sum(grepl("_all_combinations\\.csv\\.xz$", csv_files))
  combined_files <- sum(grepl("_COMBINED\\.csv\\.xz$", csv_files))
  
  # Calculate total compressed size
  total_size_bytes <- sum(sapply(file.path(output_dir, csv_files), file.size))
  total_size_mb <- round(total_size_bytes / 1024^2, 2)
  
  message(glue("   Files created: {length(csv_files)} total ({individual_files} individual + {combined_files} combined)"))
  message(glue("   Total compressed size: {total_size_mb} MB"))
  
  # Breakdown by geography level
  message(glue("\n📍 RECORDS BY GEOGRAPHY LEVEL:"))
  geo_summary <- combined_df %>%
    group_by(geography_level) %>%
    summarise(
      records = n(),
      conditions = n_distinct(condition_name),
      years = n_distinct(data_year),
      .groups = 'drop'
    ) %>%
    arrange(desc(records))
  
  for (i in 1:nrow(geo_summary)) {
    row <- geo_summary[i, ]
    message(glue("   {row$geography_level}: {format(row$records, big.mark=',')} records ({row$conditions} conditions × {row$years} years)"))
  }
  
  # Breakdown by condition (top 10)
  message(glue("\n🏥 TOP 10 CONDITIONS BY RECORD COUNT:"))
  condition_summary <- combined_df %>%
    group_by(condition_name) %>%
    summarise(records = n(), .groups = 'drop') %>%
    arrange(desc(records)) %>%
    slice_head(n = 10)
  
  for (i in 1:nrow(condition_summary)) {
    row <- condition_summary[i, ]
    message(glue("   {i}. {row$condition_name}: {format(row$records, big.mark=',')} records"))
  }
  
  # Demographics summary
  if ("agecat" %in% names(combined_df)) {
    age_cats <- length(unique(combined_df$agecat[!is.na(combined_df$agecat)]))
    message(glue("\n👥 DEMOGRAPHIC BREAKDOWNS:"))
    message(glue("   Age categories: {age_cats}"))
  }
  
  if ("sexcat" %in% names(combined_df)) {
    sex_cats <- length(unique(combined_df$sexcat[!is.na(combined_df$sexcat)]))
    message(glue("   Sex categories: {sex_cats}"))
  }
  
  if ("racecat" %in% names(combined_df)) {
    race_cats <- length(unique(combined_df$racecat[!is.na(combined_df$racecat)]))
    message(glue("   Race/ethnicity categories: {race_cats}"))
  }
  
  message(paste(rep("=", 80), collapse=""))
  message(glue("💾 COMPRESSION: All files saved as .csv.xz format for optimal storage efficiency"))
}

message("\n🎉 ALL DOWNLOADS COMPLETE! 🎉")
message(glue("Check the '{output_dir}' directory for all your compressed data files."))

test <- vroom::vroom('./raw/ALL_CONDITIONS_ALL_YEARS_ALL_GEOGRAPHIES_COMBINED.csv.xz') %>%
  filter(geography_level=='state') %>%
  mutate(statename = cdlTools::fips(fips, to='Name') 
         )
  mutate
