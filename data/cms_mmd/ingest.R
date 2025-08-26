## Code written by Claude Sonnet 4 and Gemini 2.5 Pro, with guidance from Dan Weinberger
# =============================================================================
# CMS MMD Tool - OPTIMIZED Complete Demographic Download Script
# Fast downloads with adaptive rate limiting and parallel processing
# =============================================================================

library(httr2)
library(dplyr)
library(vroom)
library(glue)
library(purrr)
library(future)
library(furrr)
library(progressr)

update=F

if(update==T){

# Setup parallel processing
plan(multisession, workers = 4)  # Adjust based on your system

# Setup
base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
output_dir <- "raw/staging_fully_stratified"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Log file for tracking progress
log_file <- file.path(output_dir, "download_log.txt")
cat("CMS MMD Download Log - Started:", as.character(Sys.time()), "\n", file = log_file, append = FALSE)

message("🚀 CMS MMD Tool - OPTIMIZED Complete Demographic Download (2020-2023)")
message(glue("Output directory: {output_dir}"))

# =============================================================================
# ADAPTIVE RATE LIMITING CLASS
# =============================================================================

RateLimiter <- R6::R6Class("RateLimiter",
                           public = list(
                             delay = 0.1,        # Start with very fast requests
                             min_delay = 0.05,   # Minimum delay (50ms)
                             max_delay = 2.0,    # Maximum delay (2 seconds)
                             success_count = 0,  # Track consecutive successes
                             error_count = 0,    # Track consecutive errors
                             
                             initialize = function() {
                               self$delay <- 0.1
                               self$success_count <- 0
                               self$error_count <- 0
                             },
                             
                             wait = function() {
                               Sys.sleep(self$delay)
                             },
                             
                             record_success = function() {
                               self$success_count <- self$success_count + 1
                               self$error_count <- 0
                               
                               # Speed up if we've had many successes
                               if (self$success_count > 10 && self$delay > self$min_delay) {
                                 self$delay <- max(self$min_delay, self$delay * 0.9)
                                 # message(glue("🚀 Speeding up: delay now {round(self$delay, 3)}s"))
                               }
                             },
                             
                             record_error = function(status_code = NULL) {
                               self$error_count <- self$error_count + 1
                               self$success_count <- 0
                               
                               # Slow down on errors, especially rate limiting
                               if (!is.null(status_code) && status_code == 429) {
                                 # Rate limit hit - slow down significantly
                                 self$delay <- min(self$max_delay, self$delay * 3)
                                 message(glue("⚠️ Rate limit hit! Slowing down: delay now {round(self$delay, 3)}s"))
                               } else {
                                 # Other errors - moderate slowdown
                                 self$delay <- min(self$max_delay, self$delay * 1.5)
                                 if (self$error_count > 3) {
                                   message(glue("⚠️ Multiple errors: delay now {round(self$delay, 3)}s"))
                                 }
                               }
                             }
                           )
)

# Global rate limiter
rate_limiter <- RateLimiter$new()

# =============================================================================
# OPTIMIZED DOWNLOAD FUNCTION WITH ADAPTIVE RATE LIMITING
# =============================================================================

download_cms_data <- function(condition_code, age_code, race_code, sex_code = NULL,
                              year = 2023, geography = "c", page_size = 500000,
                              max_retries = 3) {
  
  year_short <- substr(as.character(year), 3, 4)
  suffix <- if (year >= 2023) "_p" else if (year >= 2021) "_f" else ""
  
  # Build source pattern and parameters (same logic as before)
  if (age_code == "all" && race_code == "all") {
    if (is.null(sex_code)) {
      source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
      sexcat_param <- '.|IS NULL'
    } else {
      source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_{sex_code}_{year_short}{suffix}")
      sexcat_param <- sex_code
    }
    agecat_param <- '.|IS NULL'
    racecat_param <- '.|IS NULL'
    
  } else if (age_code == "all" && race_code != "all") {
    if (race_code == "1") {
      if (is.null(sex_code)) {
        source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
        sexcat_param <- '.|IS NULL'
      } else {
        source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_{sex_code}_{year_short}{suffix}")
        sexcat_param <- sex_code
      }
    } else {
      source_pattern <- glue("prev_final_long_fltr12_racecat_{race_code}_{year_short}{suffix}")
      if (is.null(sex_code)) {
        sexcat_param <- '.|IS NULL'
      } else {
        sexcat_param <- sex_code
      }
    }
    agecat_param <- '.|IS NULL'
    racecat_param <- race_code
    
  } else if (age_code != "all" && race_code == "all") {
    if (is.null(sex_code)) {
      source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
      sexcat_param <- '.|IS NULL'
    } else {
      source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_{sex_code}_{year_short}{suffix}")
      sexcat_param <- sex_code
    }
    agecat_param <- age_code
    racecat_param <- '.|IS NULL'
    
  } else {
    if (race_code == "1") {
      if (is.null(sex_code)) {
        source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
        sexcat_param <- '.|IS NULL'
      } else {
        source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_{sex_code}_{year_short}{suffix}")
        sexcat_param <- sex_code
      }
      racecat_param <- race_code
    } else {
      source_pattern <- glue("prev_final_long_fltr12_racecat_{race_code}_{year_short}{suffix}")
      
      if (is.null(sex_code)) {
        sexcat_param <- '.|IS NULL'
      } else {
        sexcat_param <- sex_code
      }
      racecat_param <- race_code
    }
    agecat_param <- age_code
  }
  
  # Build parameters
  params <- list(
    `_source` = source_pattern,
    population = 'f',
    year = year_short,
    geography = geography,
    measure = 'v',
    condition = condition_code,
    agecat = agecat_param,
    racecat = racecat_param,
    sexcat = sexcat_param,
    dual = '.|IS NULL',
    eligcat = '.|IS NULL',
    fltr = '1',
    `_size` = page_size
  )
  
  # Retry loop with adaptive rate limiting
  for (attempt in 1:max_retries) {
    
    # Apply rate limiting
    rate_limiter$wait()
    
    tryCatch({
      req <- request(base_url) %>% 
        req_url_query(!!!params) %>%
        req_timeout(30)  # 30 second timeout
      
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
          
          # Record success
          rate_limiter$record_success()
          
          return(df)
        } else {
          # No data but successful request
          rate_limiter$record_success()
          return(NULL)
        }
      } else {
        # HTTP error - record and potentially retry
        rate_limiter$record_error(resp$status_code)
        
        if (resp$status_code == 429) {
          # Rate limit - wait longer before retry
          message(glue("Rate limit hit for condition={condition_code}, age={age_code}, race={race_code}, sex={ifelse(is.null(sex_code), 'all', sex_code)}, year={year}. Waiting..."))
          Sys.sleep(2)
        }
        
        if (attempt < max_retries) {
          next  # Try again
        }
      }
      
    }, error = function(e) {
      rate_limiter$record_error()
      
      if (attempt < max_retries) {
        message(glue("Attempt {attempt} failed for condition={condition_code}, age={age_code}, race={race_code}, sex={ifelse(is.null(sex_code), 'all', sex_code)}, year={year}. Retrying..."))
        Sys.sleep(1)
        next
      } else {
        # Log final error
        error_msg <- glue("Final error downloading: condition={condition_code}, age={age_code}, race={race_code}, sex={ifelse(is.null(sex_code), 'all', sex_code)}, year={year}: {e$message}")
        cat(error_msg, "\n", file = log_file, append = TRUE)
      }
    })
  }
  
  return(NULL)
}

# =============================================================================
# BATCH DOWNLOAD FUNCTION WITH PROGRESS TRACKING
# =============================================================================

download_condition_batch <- function(combinations, condition_code, condition_name) {
  
  message(glue("📦 Processing batch of {length(combinations)} combinations for {condition_name}..."))
  
  # Use progressr for progress tracking
  with_progress({
    p <- progressor(along = combinations)
    
    results <- map(combinations, function(combo) {
      
      result <- download_cms_data(
        condition_code = condition_code,
        age_code = combo$age,
        race_code = combo$race,
        sex_code = combo$sex,
        year = combo$year
      )
      
      p(sprintf("Age:%s Race:%s Sex:%s Year:%s", 
                combo$age, combo$race, 
                ifelse(is.null(combo$sex), "all", combo$sex), 
                combo$year))
      
      if (!is.null(result)) {
        result$condition_name <- condition_name
        result$condition_code <- condition_code
        return(result)
      }
      
      return(NULL)
    })
  })
  
  # Filter out NULL results
  successful_results <- results[!map_lgl(results, is.null)]
  
  return(successful_results)
}

# =============================================================================
# OPTIMIZED CONDITION DOWNLOAD FUNCTION
# =============================================================================

download_full_condition_optimized <- function(condition_code, condition_name, years = 2020:2023, 
                                              save_individual = TRUE, batch_size = 50) {
  
  message(glue("\n📊 OPTIMIZED: Downloading {condition_name} (condition {condition_code}) for years {min(years)}-{max(years)}..."))
  
  # Generate all combinations
  age_codes <- c("0", "1", "2", "3", "4", "all")
  race_codes <- c("1", "2", "4", "5", "6", "all")
  sex_options <- list(
    list(code = "1", name = "male"),
    list(code = "2", name = "female"),
    list(code = NULL, name = "all")
  )
  
  all_combinations <- expand.grid(
    year = years,
    age = age_codes,
    race = race_codes,
    sex_idx = 1:length(sex_options),
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      sex = map_chr(sex_idx, ~ sex_options[[.x]]$code %||% "all_sex"),
      sex = ifelse(sex == "all_sex", NA, sex)
    ) %>%
    select(-sex_idx)
  
  total_combinations <- nrow(all_combinations)
  message(glue("   Will test {total_combinations} combinations in batches of {batch_size}..."))
  
  # Split into batches
  combination_list <- split(all_combinations, ceiling(seq_len(nrow(all_combinations)) / batch_size))
  
  message(glue("   Created {length(combination_list)} batches"))
  
  start_time <- Sys.time()
  all_data <- list()
  successful_downloads <- 0
  total_records <- 0
  
  # Process batches
  for (i in seq_along(combination_list)) {
    batch <- combination_list[[i]]
    
    message(glue("   📦 Processing batch {i}/{length(combination_list)} ({nrow(batch)} combinations)..."))
    
    # Convert batch to list of named lists
    batch_combinations <- pmap(batch, function(year, age, race, sex) {
      list(year = year, age = age, race = race, sex = if(is.na(sex)) NULL else sex)
    })
    
    batch_results <- download_condition_batch(batch_combinations, condition_code, condition_name)
    
    if (length(batch_results) > 0) {
      batch_key <- glue("batch_{i}")
      all_data[[batch_key]] <- bind_rows(batch_results)
      successful_downloads <- successful_downloads + length(batch_results)
      total_records <- total_records + sum(map_int(batch_results, nrow))
      
      message(glue("     ✅ Batch {i}: {length(batch_results)} successful downloads, {sum(map_int(batch_results, nrow))} records"))
    } else {
      message(glue("     ❌ Batch {i}: No successful downloads"))
    }
    
    # Show overall progress
    elapsed_time <- difftime(Sys.time(), start_time, units = "mins")
    batches_remaining <- length(combination_list) - i
    estimated_time_per_batch <- as.numeric(elapsed_time) / i
    estimated_remaining_time <- estimated_time_per_batch * batches_remaining
    
    message(glue("     ⏱️  Batch progress: {i}/{length(combination_list)} completed"))
    message(glue("        Elapsed: {round(elapsed_time, 1)} min, Est. remaining: {round(estimated_remaining_time, 1)} min"))
    message(glue("        Current rate: {round(rate_limiter$delay, 3)}s delay between requests"))
  }
  
  # Summary
  success_rate <- round(successful_downloads / total_combinations * 100, 1)
  formatted_total_records <- format(total_records, big.mark = ",")
  total_time <- difftime(Sys.time(), start_time, units = "mins")
  
  message(glue("\n   📊 {condition_name} Summary:"))
  message(glue("      Successful: {successful_downloads}/{total_combinations} combinations ({success_rate}%)"))
  message(glue("      Total records: {formatted_total_records}"))
  message(glue("      Time: {round(total_time, 1)} minutes"))
  message(glue("      Average rate: {round(total_combinations / as.numeric(total_time), 1)} combinations/minute"))
  
  if (length(all_data) > 0) {
    combined_data <- bind_rows(all_data)
    
    if (save_individual) {
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
      time_minutes = as.numeric(total_time)
    ))
  }
  
  return(NULL)
}

# =============================================================================
# OPTIMIZED ALL CONDITIONS DOWNLOAD
# =============================================================================

download_all_conditions_optimized <- function(years = 2020:2023) {
  
  message(glue("🚀 OPTIMIZED: Starting download of ALL CONDITIONS for years {min(years)}-{max(years)}..."))
  message("📈 Using adaptive rate limiting and batch processing")
  
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
  
  total_combinations_expected <- length(conditions) * length(years) * 6 * 6 * 3
  
  message(glue("📋 Will download {length(conditions)} conditions"))
  message(glue("📊 Expected total combinations: {format(total_combinations_expected, big.mark = ',')}"))
  
  all_condition_data <- list()
  overall_stats <- list(
    total_conditions = length(conditions),
    successful_conditions = 0,
    total_combinations = 0,
    successful_combinations = 0,
    total_records = 0,
    total_time = 0
  )
  
  start_time <- Sys.time()
  
  for (i in seq_along(conditions)) {
    condition <- conditions[[i]]
    
    message(glue("\n{paste(rep('=', 100), collapse = '')}"))
    message(glue("CONDITION {i}/{length(conditions)}: {condition$name}"))
    message(glue("{paste(rep('=', 100), collapse = '')}"))
    
    result <- download_full_condition_optimized(
      condition_code = condition$code,
      condition_name = condition$name,
      years = years,
      save_individual = TRUE,
      batch_size = 75  # Larger batches for efficiency
    )
    
    if (!is.null(result)) {
      all_condition_data[[condition$name]] <- result$data
      overall_stats$successful_conditions <- overall_stats$successful_conditions + 1
      overall_stats$total_combinations <- overall_stats$total_combinations + result$total_combinations
      overall_stats$successful_combinations <- overall_stats$successful_combinations + result$successful_downloads
      overall_stats$total_records <- overall_stats$total_records + result$total_records
      overall_stats$total_time <- overall_stats$total_time + result$time_minutes
      
      message(glue("✅ {condition$name} completed successfully"))
    } else {
      message(glue("❌ {condition$name} failed completely"))
    }
    
    # Show overall progress
    elapsed_time <- difftime(Sys.time(), start_time, units = "mins")
    estimated_time_per_condition <- as.numeric(elapsed_time) / i
    remaining_conditions <- length(conditions) - i
    estimated_remaining_time <- estimated_time_per_condition * remaining_conditions
    
    message(glue("⏱️  Overall Progress: {i}/{length(conditions)} conditions completed"))
    message(glue("   Elapsed: {round(elapsed_time, 1)} minutes"))
    message(glue("   Estimated remaining: {round(estimated_remaining_time, 1)} minutes"))
    message(glue("   Current API rate: {round(rate_limiter$delay, 3)}s delay"))
  }
  
  end_time <- Sys.time()
  total_time <- difftime(end_time, start_time, units = "mins")
  
  message(glue("\n{paste(rep('=', 120), collapse = '')}"))
  message(glue("🎉 OPTIMIZED DOWNLOAD COMPLETED! ({min(years)}-{max(years)})"))
  message(glue("{paste(rep('=', 120), collapse = '')}"))
  
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
  message(glue("   Average rate: {round(overall_stats$successful_combinations / as.numeric(total_time), 1)} combinations/minute"))
  
  # Create combined file
  if (length(all_condition_data) > 0) {
    message("\n💾 Creating combined file...")
    
    combined_all <- bind_rows(all_condition_data)
    combined_filename <- file.path(output_dir, glue("ALL_CONDITIONS_{min(years)}_{max(years)}_FULL_DEMOGRAPHICS.csv.xz"))
    vroom_write(combined_all, combined_filename, delim = ",")
    
    combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
    message(glue("   💾 Combined file: {basename(combined_filename)} ({combined_size_mb} MB)"))
  }
  
  return(overall_stats)
}

# =============================================================================
# RUN THE OPTIMIZED DOWNLOAD
# =============================================================================

message("🎬 Starting OPTIMIZED CMS MMD download for 2020-2023...")
message("⚡ Using adaptive rate limiting that starts fast and adjusts based on API responses")
message("📦 Processing in batches with progress tracking")
message("Estimated time: 2-3 hours (much faster than previous version)")

# Quick test
message("\n🧪 Running quick test...")
test_result <- download_full_condition_optimized("12", "diabetes_test", years = 2023, save_individual = FALSE, batch_size = 20)

if (!is.null(test_result)) {
  message(glue("✅ Test successful! {test_result$successful_downloads} combinations in {round(test_result$time_minutes, 1)} minutes"))
  message(glue("   Rate: {round(test_result$successful_downloads / test_result$time_minutes, 1)} combinations/minute"))
  message("\n🚀 Proceeding with optimized full download...\n")
  
  final_stats <- download_all_conditions_optimized(years = 2020:2023)
  
  message("\n🎉 OPTIMIZED DOWNLOAD COMPLETE!")
  message(glue("Check the '{output_dir}' directory for all downloaded files."))
  
} else {
  message("❌ Test failed. Please check the API connection and try again.")
}

}