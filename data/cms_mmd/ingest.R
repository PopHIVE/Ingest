## Code written by Claude Sonnet 4 and Gemini 2.5 Pro, with guidance from Dan Weinberger
# =============================================================================
# CMS MMD Tool - Complete Demographic Download Script (2020-2023 + All Races)
# Downloads all available combinations of age, sex, race, and condition for 2020-2023
# =============================================================================

library(httr2)
library(dplyr)
library(vroom)
library(glue)
library(purrr)

# Setup
base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
output_dir <- "raw/staging_fully_stratified"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Log file for tracking progress
log_file <- file.path(output_dir, "download_log.txt")
cat("CMS MMD Download Log - Started:", as.character(Sys.time()), "\n", file = log_file, append = FALSE)

message("🚀 CMS MMD Tool - Complete Demographic Download (2020-2023)")
message(glue("Output directory: {output_dir}"))

# =============================================================================
# CORE DOWNLOAD FUNCTION (ENHANCED FOR MULTI-YEAR + ALL RACES)
# =============================================================================

download_cms_data <- function(condition_code, age_code, race_code, sex_code = NULL,
                              year = 2023, geography = "c", page_size = 500000) {
  
  year_short <- substr(as.character(year), 3, 4)
  suffix <- if (year >= 2023) "_p" else if (year >= 2021) "_f" else ""
  
  # Build the _source pattern based on race-specific rules
  if (race_code == "1") {
    # Race 1 (White) has different patterns for each sex option
    if (is.null(sex_code)) {
      # All sexes for race 1
      source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
      sexcat_param <- '.|IS NULL'
    } else {
      # Specific sex for race 1
      source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_{sex_code}_{year_short}{suffix}")
      sexcat_param <- sex_code
    }
  } else if (race_code == "all") {
    # All races combined - test different possible patterns
    possible_patterns <- c(
      glue("prev_final_long_fltr12_racecat_all_{year_short}{suffix}"),
      glue("prev_final_long_fltr12_allrace_{year_short}{suffix}"),
      glue("prev_final_long_fltr12_{year_short}{suffix}")
    )
    
    # Try each pattern until one works
    for (pattern in possible_patterns) {
      tryCatch({
        params <- list(
          `_source` = pattern,
          population = 'f',
          year = year_short,
          geography = geography,
          measure = 'v',
          condition = condition_code,
          agecat = age_code,
          dual = '.|IS NULL',
          eligcat = '.|IS NULL',
          fltr = '1',
          `_size` = page_size
        )
        
        # Handle sex parameter
        if (is.null(sex_code)) {
          params$sexcat <- '.|IS NULL'  # All sexes
        } else {
          params$sexcat <- sex_code     # Specific sex
        }
        
        # Don't include racecat for "all races" - let it default to all
        
        req <- request(base_url) %>% req_url_query(!!!params)
        resp <- req_perform(req)
        
        if (resp$status_code == 200) {
          data_list <- resp_body_json(resp)
          
          if (length(data_list) > 0) {
            df <- bind_rows(data_list)
            
            # Add metadata
            df$age_code <- age_code
            df$race_code <- "all"
            df$sex_code <- ifelse(is.null(sex_code), "all", sex_code)
            df$year <- year
            df$source_pattern <- pattern
            df$download_timestamp <- Sys.time()
            
            return(df)
          }
        }
      }, error = function(e) {
        # Continue to next pattern
      })
    }
    
    return(NULL)
    
  } else {
    # All other races (2,4,5,6) do NOT use sexcat in _source
    source_pattern <- glue("prev_final_long_fltr12_racecat_{race_code}_{year_short}{suffix}")
    
    if (is.null(sex_code)) {
      sexcat_param <- '.|IS NULL'  # All sexes
    } else {
      sexcat_param <- sex_code     # Specific sex
    }
  }
  
  # Build parameters (for non-"all races" cases)
  if (race_code != "all") {
    params <- list(
      `_source` = source_pattern,
      population = 'f',
      year = year_short,
      geography = geography,
      measure = 'v',
      condition = condition_code,
      agecat = age_code,
      racecat = race_code,
      sexcat = sexcat_param,
      dual = '.|IS NULL',
      eligcat = '.|IS NULL',
      fltr = '1',
      `_size` = page_size
    )
    
    tryCatch({
      req <- request(base_url) %>% req_url_query(!!!params)
      resp <- req_perform(req)
      
      if (resp$status_code == 200) {
        data_list <- resp_body_json(resp)
        
        if (length(data_list) > 0) {
          df <- bind_rows(data_list)
          
          # Add metadata
          df$age_code <- age_code
          df$race_code <- race_code
          df$sex_code <- ifelse(is.null(sex_code), "all", sex_code)
          df$year <- year
          df$source_pattern <- source_pattern
          df$download_timestamp <- Sys.time()
          
          return(df)
        }
      }
      
      return(NULL)
      
    }, error = function(e) {
      # Log errors
      error_msg <- glue("Error downloading: condition={condition_code}, age={age_code}, race={race_code}, sex={ifelse(is.null(sex_code), 'all', sex_code)}, year={year}: {e$message}")
      cat(error_msg, "\n", file = log_file, append = TRUE)
      return(NULL)
    })
  }
}

# =============================================================================
# CONDITION DOWNLOAD FUNCTION (MULTI-YEAR + ALL RACES)
# =============================================================================

download_full_condition <- function(condition_code, condition_name, years = 2020:2023, save_individual = TRUE) {
  
  message(glue("\n📊 Downloading {condition_name} (condition {condition_code}) for years {min(years)}-{max(years)}..."))
  
  all_data <- list()
  successful_downloads <- 0
  total_records <- 0
  failed_combinations <- list()
  
  # Define demographic combinations
  age_codes <- c("0", "1", "2", "3", "4")  # All age groups
  race_codes <- c("1", "2", "4", "5", "6", "all")  # All race groups + all races combined
  sex_options <- list(
    list(code = "1", name = "male"),
    list(code = "2", name = "female"),
    list(code = NULL, name = "all")
  )
  
  combo_count <- 0
  total_combinations <- length(years) * length(age_codes) * length(race_codes) * length(sex_options)
  
  message(glue("   Testing {total_combinations} demographic combinations across {length(years)} years..."))
  
  # Log start for this condition
  cat(glue("Starting {condition_name} (condition {condition_code}) for years {paste(years, collapse=', ')} at {Sys.time()}"), "\n", file = log_file, append = TRUE)
  
  for (year in years) {
    message(glue("   📅 Processing year {year}..."))
    
    for (age_code in age_codes) {
      for (race_code in race_codes) {
        for (sex_option in sex_options) {
          combo_count <- combo_count + 1
          
          data <- download_cms_data(
            condition_code = condition_code,
            age_code = age_code,
            race_code = race_code,
            sex_code = sex_option$code,
            year = year
          )
          
          demo_label <- glue("Year:{year} Age:{age_code} Race:{race_code} Sex:{sex_option$name}")
          
          if (!is.null(data)) {
            # Format numbers without comma in glue
            record_count <- nrow(data)
            formatted_count <- format(record_count, big.mark = ",")
            
            message(glue("      [{combo_count}/{total_combinations}] {demo_label} → {formatted_count} records"))
            
            # Add condition metadata
            data$condition_name <- condition_name
            data$condition_code <- condition_code
            
            combo_key <- glue("year_{year}_age_{age_code}_race_{race_code}_sex_{sex_option$name}")
            all_data[[combo_key]] <- data
            successful_downloads <- successful_downloads + 1
            total_records <- total_records + nrow(data)
            
            # Log success
            cat(glue("SUCCESS: {condition_name} - {demo_label} - {nrow(data)} records"), "\n", file = log_file, append = TRUE)
            
          } else {
            if (combo_count %% 50 == 0) { # Only show every 50th failure to reduce noise
              message(glue("      [{combo_count}/{total_combinations}] {demo_label} → No data"))
            }
            
            # Track failed combinations
            failed_combinations[[length(failed_combinations) + 1]] <- list(
              condition = condition_code,
              year = year,
              age = age_code,
              race = race_code,
              sex = sex_option$name,
              demo_label = demo_label
            )
            
            # Log failure (but less verbose)
            if (combo_count %% 100 == 0) {
              cat(glue("FAILED: {condition_name} - {demo_label}"), "\n", file = log_file, append = TRUE)
            }
          }
          
          # Rate limiting to be respectful to the API
          Sys.sleep(0.2)  # Slightly faster since we have more combinations
        }
      }
    }
  }
  
  # Summary
  success_rate <- round(successful_downloads / total_combinations * 100, 1)
  formatted_total_records <- format(total_records, big.mark = ",")
  
  message(glue("\n   📊 {condition_name} Summary:"))
  message(glue("      Successful: {successful_downloads}/{total_combinations} combinations ({success_rate}%)"))
  message(glue("      Total records: {formatted_total_records}"))
  
  # Show breakdown by year
  if (length(all_data) > 0) {
    combined_data <- bind_rows(all_data)
    
    year_summary <- combined_data %>%
      group_by(year) %>%
      summarise(
        combinations = n(),
        total_records = n(),
        .groups = 'drop'
      ) %>%
      arrange(year)
    
    message("      Year breakdown:")
    for (i in 1:nrow(year_summary)) {
      yr <- year_summary$year[i]
      combo_count <- year_summary$combinations[i]
      rec_count <- format(year_summary$total_records[i], big.mark = ",")
      message(glue("        {yr}: {combo_count} combinations, {rec_count} records"))
    }
  }
  
  # Log summary
  cat(glue("SUMMARY: {condition_name} - {successful_downloads}/{total_combinations} successful ({success_rate}%) - {total_records} total records"), "\n", file = log_file, append = TRUE)
  
  if (length(all_data) > 0) {
    combined_data <- bind_rows(all_data)
    
    if (save_individual) {
      # Save individual condition file
      filename <- file.path(output_dir, glue("{condition_name}_2020_2023_FULL_DEMOGRAPHICS.csv.xz"))
      vroom_write(combined_data, filename, delim = ",")
      
      file_size_mb <- round(file.size(filename) / 1024^2, 2)
      message(glue("      💾 Saved: {basename(filename)} ({file_size_mb} MB)"))
    }
    
    return(list(
      data = combined_data,
      successful_downloads = successful_downloads,
      total_combinations = total_combinations,
      total_records = total_records,
      failed_combinations = failed_combinations
    ))
  }
  
  return(NULL)
}

# =============================================================================
# ALL CONDITIONS DOWNLOAD (MULTI-YEAR)
# =============================================================================

download_all_conditions <- function(years = 2020:2023) {
  
  message(glue("🚀 Starting download of ALL CONDITIONS for years {min(years)}-{max(years)}..."))
  
  # Define all conditions
  conditions <- list(
    list(code = "1", name = "alzheimer_dementia"),
    list(code = "2", name = "arthritis"), 
    list(code = "3", name = "asthma"),
    list(code = "4", name = "atrial_fibrillation"),
    list(code = "5", name = "cancer_breast"), 
    list(code = "6", name = "cancer_colorectal"),
    list(code = "7", name = "cancer_lung"),
    list(code = "8", name = "cancer_prostate"),
    list(code = "9", name = "chronic_kidney_disease"),
    list(code = "10", name = "copd"),
    list(code = "11", name = "depression"),
    list(code = "12", name = "diabetes"),
    list(code = "13", name = "heart_failure"),
    list(code = "14", name = "hyperlipidemia"),
    list(code = "15", name = "hypertension"),
    list(code = "16", name = "ischemic_heart_disease"),
    list(code = "17", name = "osteoporosis"),
    list(code = "18", name = "stroke")
  )
  
  message(glue("📋 Will download {length(conditions)} conditions across {length(years)} years"))
  message(glue("📂 Output directory: {output_dir}"))
  
  # Track overall progress
  all_condition_data <- list()
  overall_stats <- list(
    total_conditions = length(conditions),
    successful_conditions = 0,
    total_combinations = 0,
    successful_combinations = 0,
    total_records = 0,
    all_failed_combinations = list()
  )
  
  start_time <- Sys.time()
  
  for (i in seq_along(conditions)) {
    condition <- conditions[[i]]
    
    message(glue("\n{paste(rep('=', 80), collapse = '')}"))
    message(glue("CONDITION {i}/{length(conditions)}: {condition$name}"))
    message(glue("{paste(rep('=', 80), collapse = '')}"))
    
    result <- download_full_condition(
      condition_code = condition$code,
      condition_name = condition$name,
      years = years,
      save_individual = TRUE
    )
    
    if (!is.null(result)) {
      all_condition_data[[condition$name]] <- result$data
      overall_stats$successful_conditions <- overall_stats$successful_conditions + 1
      overall_stats$total_combinations <- overall_stats$total_combinations + result$total_combinations
      overall_stats$successful_combinations <- overall_stats$successful_combinations + result$successful_downloads
      overall_stats$total_records <- overall_stats$total_records + result$total_records
      
      # Collect failed combinations
      overall_stats$all_failed_combinations <- c(overall_stats$all_failed_combinations, result$failed_combinations)
      
      message(glue("✅ {condition$name} completed successfully"))
    } else {
      message(glue("❌ {condition$name} failed completely"))
    }
    
    # Show progress
    elapsed_time <- difftime(Sys.time(), start_time, units = "mins")
    estimated_time_per_condition <- as.numeric(elapsed_time) / i
    remaining_conditions <- length(conditions) - i
    estimated_remaining_time <- estimated_time_per_condition * remaining_conditions
    
    message(glue("⏱️  Progress: {i}/{length(conditions)} conditions completed"))
    message(glue("   Elapsed: {round(elapsed_time, 1)} minutes"))
    message(glue("   Estimated remaining: {round(estimated_remaining_time, 1)} minutes"))
  }
  
  # =============================================================================
  # FINAL SUMMARY AND COMBINED FILE
  # =============================================================================
  
  end_time <- Sys.time()
  total_time <- difftime(end_time, start_time, units = "mins")
  
  message(glue("\n{paste(rep('=', 100), collapse = '')}"))
  message(glue("🎉 ALL CONDITIONS DOWNLOAD COMPLETED! ({min(years)}-{max(years)})"))
  message(glue("{paste(rep('=', 100), collapse = '')}"))
  
  # Format large numbers properly
  formatted_successful_combinations <- format(overall_stats$successful_combinations, big.mark = ",")
  formatted_total_combinations <- format(overall_stats$total_combinations, big.mark = ",")
  formatted_total_records <- format(overall_stats$total_records, big.mark = ",")
  
  message(glue("📊 FINAL SUMMARY:"))
  message(glue("   Years: {paste(years, collapse = ', ')}"))
  message(glue("   Conditions: {overall_stats$successful_conditions}/{overall_stats$total_conditions}"))
  message(glue("   Combinations: {formatted_successful_combinations}/{formatted_total_combinations}"))
  message(glue("   Total records: {formatted_total_records}"))
  message(glue("   Success rate: {round(overall_stats$successful_combinations/overall_stats$total_combinations*100, 1)}%"))
  message(glue("   Total time: {round(total_time, 1)} minutes"))
  
  # Log final summary
  cat(glue("FINAL SUMMARY: {overall_stats$successful_conditions}/{overall_stats$total_conditions} conditions, {overall_stats$successful_combinations}/{overall_stats$total_combinations} combinations, {overall_stats$total_records} total records"), "\n", file = log_file, append = TRUE)
  cat(glue("Completed at: {Sys.time()}"), "\n", file = log_file, append = TRUE)
  
  # Create combined file with ALL conditions and years
  if (length(all_condition_data) > 0) {
    message("\n💾 Creating combined file with all conditions and years...")
    
    combined_all <- bind_rows(all_condition_data)
    combined_filename <- file.path(output_dir, glue("ALL_CONDITIONS_{min(years)}_{max(years)}_FULL_DEMOGRAPHICS.csv.xz"))
    vroom_write(combined_all, combined_filename, delim = ",")
    
    combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
    message(glue("   💾 Combined file: {basename(combined_filename)} ({combined_size_mb} MB)"))
    
    # Show breakdown by year in final data
    year_breakdown <- combined_all %>%
      group_by(year) %>%
      summarise(
        conditions = n_distinct(condition_code),
        total_records = n(),
        .groups = 'drop'
      ) %>%
      arrange(year)
    
    message("\n📅 Final data by year:")
    for (i in 1:nrow(year_breakdown)) {
      yr <- year_breakdown$year[i]
      cond_count <- year_breakdown$conditions[i]
      rec_count <- format(year_breakdown$total_records[i], big.mark = ",")
      message(glue("   {yr}: {cond_count} conditions, {rec_count} records"))
    }
  }
  
  # Save failed combinations summary
  if (length(overall_stats$all_failed_combinations) > 0) {
    failed_df <- bind_rows(overall_stats$all_failed_combinations)
    failed_filename <- file.path(output_dir, glue("failed_combinations_summary_{min(years)}_{max(years)}.csv"))
    vroom_write(failed_df, failed_filename, delim = ",")
    message(glue("   📝 Failed combinations: {basename(failed_filename)}"))
  }
  
  return(overall_stats)
}

# =============================================================================
# RUN THE COMPLETE DOWNLOAD
# =============================================================================

message("🎬 Starting complete CMS MMD download for 2020-2023...")
message("This will download all available age/sex/race combinations for all conditions across 4 years.")
message("This includes an 'all races combined' category in addition to individual races.")
message("Estimated time: 3-4 hours depending on API response times.")

# Run a quick test first
message("\n🧪 Running quick test with one condition for 2023...")
test_result <- download_full_condition("4", "asthma_test", years = 2023, save_individual = FALSE)

if (!is.null(test_result)) {
  formatted_downloads <- format(test_result$successful_downloads, big.mark = ",")
  message(glue("✅ Test successful! Found {formatted_downloads} working combinations"))
  message("📊 Proceeding with full download for 2020-2023...\n")
  
  # Run the complete download
  final_stats <- download_all_conditions(years = 2020:2023)
  
  message("\n🎉 DOWNLOAD COMPLETE!")
  message(glue("Check the '{output_dir}' directory for all downloaded files."))
  
}