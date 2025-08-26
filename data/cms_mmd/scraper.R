## Code written by Claude Sonnet 4 and Gemini 2.5 Pro, with guidance from Dan Weinberger
# =============================================================================
# CMS MMD Tool - OPTIMIZED Complete Demographic Download Script
# Fast downloads with adaptive rate limiting and parallel processing
# =============================================================================

update=F

if(update==T){
  # =============================================================================
  # CMS MMD Tool - OPTIMIZED Complete Demographic Download Script (CORRECTED)
  # With correct condition codes and demographic labels
  # =============================================================================
  
  library(httr2)
  library(dplyr)
  library(vroom)
  library(glue)
  library(purrr)
  library(progressr)
  
  # Setup
  base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
  output_dir <- "raw/staging_fully_stratified"
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Log file for tracking progress
  log_file <- file.path(output_dir, "download_log.txt")
  cat("CMS MMD Download Log - Started:", as.character(Sys.time()), "\n", file = log_file, append = FALSE)
  
  message("🚀 CMS MMD Tool - OPTIMIZED Complete Demographic Download (2020-2023) - CORRECTED")
  message(glue("Output directory: {output_dir}"))
  
  # =============================================================================
  # DEMOGRAPHIC LABELS AND CONDITION MAPPINGS
  # =============================================================================
  
  # Age labels
  age_labels <- list(
    "0" = "Under_65",
    "1" = "65_to_74", 
    "2" = "75_to_84",
    "3" = "85_plus",
    "4" = "65_plus",
    "all" = "All_Ages"
  )
  
  # Race labels
  race_labels <- list(
    "1" = "White",
    "2" = "Black", 
    "4" = "Asian_Pacific_Islander",
    "5" = "Hispanic",
    "6" = "American_Indian_Native_American",
    "all" = "All_Races"
  )
  
  # Sex labels
  sex_labels <- list(
    "1" = "Male",
    "2" = "Female",
    "all" = "All_Sexes"
  )
  
  # CORRECT condition mappings
  conditions_map <- list(
    list(code = "2", name = "acute_myocardial_infarction"),
    list(code = "1", name = "alzheimers"),
    list(code = "147", name = "anemia"),
    list(code = "4", name = "asthma"),
    list(code = "11", name = "atrial_fibrilation"),
    list(code = "5", name = "colorectal_breast_prostate_lung_cancer"),
    list(code = "12", name = "chronic_kidney"),
    list(code = "13", name = "copd"),
    list(code = "14", name = "depression"),
    list(code = "15", name = "diabetes"),
    list(code = "153", name = "glaucoma"),
    list(code = "16", name = "heart_failure_non_ischemic"),
    list(code = "152", name = "hip_pelvic_fracture"),
    list(code = "18", name = "hyperlidipemia"),
    list(code = "17", name = "hypertension"),
    list(code = "19", name = "ischemic_heart_disease"),
    list(code = "20", name = "obesity"),
    list(code = "21", name = "osteoporosis"),
    list(code = "155", name = "parkinsons"),
    list(code = "3", name = "rheumoatoid_arthritis"),
    list(code = "22", name = "schizophrenia_and_psycotic"),
    list(code = "23", name = "stroke_ischemic_attack")
  )
  
  message(glue("📋 Will process {length(conditions_map)} conditions with correct codes"))
  message("📝 Age categories: Under_65, 65_to_74, 75_to_84, 85_plus, 65_plus, All_Ages")
  message("📝 Race categories: White, Black, Asian_Pacific_Islander, Hispanic, American_Indian_Native_American, All_Races")
  
  # =============================================================================
  # ADAPTIVE RATE LIMITING CLASS
  # =============================================================================
  
  RateLimiter <- R6::R6Class("RateLimiter",
                             public = list(
                               delay = 0.1,        # Start with fast requests
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
                                 }
                               },
                               
                               record_error = function(status_code = NULL) {
                                 self$error_count <- self$error_count + 1
                                 self$success_count <- 0
                                 
                                 # Slow down on errors, especially rate limiting
                                 if (!is.null(status_code) && status_code == 429) {
                                   self$delay <- min(self$max_delay, self$delay * 3)
                                   message(glue("⚠️ Rate limit hit! Slowing down: delay now {round(self$delay, 3)}s"))
                                 } else {
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
  # ENHANCED DOWNLOAD FUNCTION WITH DEMOGRAPHIC LABELS
  # =============================================================================
  
  download_cms_data_safe <- function(condition_code, age_code, race_code, sex_code = NULL,
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
    
    # Retry loop
    for (attempt in 1:max_retries) {
      
      # Apply rate limiting
      rate_limiter$wait()
      
      result <- tryCatch({
        req <- request(base_url) %>% 
          req_url_query(!!!params) %>%
          req_timeout(30)
        
        resp <- req_perform(req)
        
        if (resp$status_code == 200) {
          data_list <- resp_body_json(resp)
          
          if (length(data_list) > 0) {
            df <- bind_rows(data_list)
            
            # Add metadata with LABELS
            df$age_code <- age_code
            df$age_label <- age_labels[[as.character(age_code)]]
            df$race_code <- race_code
            df$race_label <- race_labels[[as.character(race_code)]]
            df$sex_code <- ifelse(is.null(sex_code), "all", sex_code)
            df$sex_label <- sex_labels[[ifelse(is.null(sex_code), "all", sex_code)]]
            df$year <- year
            df$source_pattern <- source_pattern
            df$download_timestamp <- Sys.time()
            
            rate_limiter$record_success()
            return(df)
          } else {
            # No data but successful request
            rate_limiter$record_success()
            return(NULL)
          }
        } else {
          # HTTP error
          rate_limiter$record_error(resp$status_code)
          
          if (resp$status_code == 429) {
            # Rate limit - wait longer before retry
            Sys.sleep(2)
          }
          
          return("retry")  # Signal to retry
        }
        
      }, error = function(e) {
        rate_limiter$record_error()
        
        # Log error but continue
        if (attempt == max_retries) {
          error_msg <- glue("Final error downloading: condition={condition_code}, age={age_code}, race={race_code}, sex={ifelse(is.null(sex_code), 'all', sex_code)}, year={year}: {e$message}")
          cat(error_msg, "\n", file = log_file, append = TRUE)
        }
        
        Sys.sleep(1)  # Wait before retry
        return("retry")
      })
      
      # Check result
      if (is.null(result)) {
        # Success with no data
        return(NULL)
      } else if (is.data.frame(result)) {
        # Success with data
        return(result)
      } else if (result == "retry" && attempt < max_retries) {
        # Retry needed and attempts remaining
        # Continue to next attempt
      } else {
        # Max retries reached or other issue
        return(NULL)
      }
    }
    
    return(NULL)
  }
  
  # =============================================================================
  # BATCH DOWNLOAD FUNCTION
  # =============================================================================
  
  download_condition_batch_safe <- function(combinations, condition_code, condition_name) {
    
    # Use progressr for progress tracking
    with_progress({
      p <- progressor(along = combinations)
      
      results <- map(combinations, function(combo) {
        
        result <- download_cms_data_safe(
          condition_code = condition_code,
          age_code = combo$age,
          race_code = combo$race,
          sex_code = combo$sex,
          year = combo$year
        )
        
        # Create readable progress message
        age_label <- age_labels[[as.character(combo$age)]]
        race_label <- race_labels[[as.character(combo$race)]]
        sex_label <- sex_labels[[ifelse(is.null(combo$sex), "all", combo$sex)]]
        
        p(sprintf("%s %s-%s-%s", combo$year, age_label, race_label, sex_label))
        
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
  # CONDITION DOWNLOAD FUNCTION
  # =============================================================================
  
  download_full_condition_optimized_safe <- function(condition_code, condition_name, years = 2020:2023, 
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
      
      batch_results <- download_condition_batch_safe(batch_combinations, condition_code, condition_name)
      
      if (length(batch_results) > 0) {
        batch_key <- glue("batch_{i}")
        all_data[[batch_key]] <- bind_rows(batch_results)
        successful_downloads <- successful_downloads + length(batch_results)
        total_records <- total_records + sum(map_int(batch_results, nrow))
        
        formatted_records <- format(sum(map_int(batch_results, nrow)), big.mark = ",")
        message(glue("     ✅ Batch {i}: {length(batch_results)} successful downloads, {formatted_records} records"))
      } else {
        message(glue("     ❌ Batch {i}: No successful downloads"))
      }
      
      # Show overall progress
      elapsed_time <- difftime(Sys.time(), start_time, units = "mins")
      batches_remaining <- length(combination_list) - i
      if (i > 0) {
        estimated_time_per_batch <- as.numeric(elapsed_time) / i
        estimated_remaining_time <- estimated_time_per_batch * batches_remaining
        
        message(glue("     ⏱️  Batch progress: {i}/{length(combination_list)} completed"))
        message(glue("        Elapsed: {round(elapsed_time, 1)} min, Est. remaining: {round(estimated_remaining_time, 1)} min"))
        message(glue("        Current rate: {round(rate_limiter$delay, 3)}s delay between requests"))
      }
    }
    
    # Summary
    success_rate <- if(total_combinations > 0) round(successful_downloads / total_combinations * 100, 1) else 0
    formatted_total_records <- format(total_records, big.mark = ",")
    total_time <- difftime(Sys.time(), start_time, units = "mins")
    
    message(glue("\n   📊 {condition_name} Summary:"))
    message(glue("      Successful: {successful_downloads}/{total_combinations} combinations ({success_rate}%)"))
    message(glue("      Total records: {formatted_total_records}"))
    message(glue("      Time: {round(total_time, 1)} minutes"))
    if (as.numeric(total_time) > 0) {
      message(glue("      Average rate: {round(successful_downloads / as.numeric(total_time), 1)} combinations/minute"))
    }
    
    # Show demographic breakdown
    if (length(all_data) > 0) {
      combined_data <- bind_rows(all_data)
      
      # Summary by demographic labels
      demo_summary <- combined_data %>%
        group_by(age_label, race_label, sex_label) %>%
        summarise(
          combinations = n(),
          total_records = n(),
          .groups = 'drop'
        ) %>%
        arrange(age_label, race_label, sex_label) %>%
        head(10)  # Show top 10
      
      message("      Top demographic combinations found:")
      for (i in 1:min(nrow(demo_summary), 5)) {
        age_lab <- demo_summary$age_label[i]
        race_lab <- demo_summary$race_label[i]
        sex_lab <- demo_summary$sex_label[i]
        rec_count <- format(demo_summary$total_records[i], big.mark = ",")
        message(glue("        {age_lab} + {race_lab} + {sex_lab}: {rec_count} records"))
      }
      
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
  # ALL CONDITIONS DOWNLOAD WITH CORRECT CODES
  # =============================================================================
  
  download_all_conditions_optimized_safe <- function(years = 2020:2023) {
    
    message(glue("🚀 OPTIMIZED: Starting download of ALL CONDITIONS for years {min(years)}-{max(years)}..."))
    message("📈 Using CORRECT condition codes and demographic labels")
    
    total_combinations_expected <- length(conditions_map) * length(years) * 6 * 6 * 3
    
    message(glue("📋 Will download {length(conditions_map)} conditions"))
    message(glue("📊 Expected total combinations: {format(total_combinations_expected, big.mark = ',')}"))
    
    all_condition_data <- list()
    overall_stats <- list(
      total_conditions = length(conditions_map),
      successful_conditions = 0,
      total_combinations = 0,
      successful_combinations = 0,
      total_records = 0,
      total_time = 0
    )
    
    start_time <- Sys.time()
    
    for (i in seq_along(conditions_map)) {
      condition <- conditions_map[[i]]
      
      message(glue("\n{paste(rep('=', 100), collapse = '')}"))
      message(glue("CONDITION {i}/{length(conditions_map)}: {condition$name} (code: {condition$code})"))
      message(glue("{paste(rep('=', 100), collapse = '')}"))
      
      result <- download_full_condition_optimized_safe(
        condition_code = condition$code,
        condition_name = condition$name,
        years = years,
        save_individual = TRUE,
        batch_size = 50
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
      if (i > 0) {
        estimated_time_per_condition <- as.numeric(elapsed_time) / i
        remaining_conditions <- length(conditions_map) - i
        estimated_remaining_time <- estimated_time_per_condition * remaining_conditions
        
        message(glue("⏱️  Overall Progress: {i}/{length(conditions_map)} conditions completed"))
        message(glue("   Elapsed: {round(elapsed_time, 1)} minutes"))
        message(glue("   Estimated remaining: {round(estimated_remaining_time, 1)} minutes"))
        message(glue("   Current API rate: {round(rate_limiter$delay, 3)}s delay"))
      }
    }
    
    end_time <- Sys.time()
    total_time <- difftime(end_time, start_time, units = "mins")
    
    message(glue("\n{paste(rep('=', 120), collapse = '')}"))
    message(glue("🎉 CORRECTED DOWNLOAD COMPLETED! ({min(years)}-{max(years)})"))
    message(glue("{paste(rep('=', 120), collapse = '')}"))
    
    formatted_successful_combinations <- format(overall_stats$successful_combinations, big.mark = ",")
    formatted_total_combinations <- format(overall_stats$total_combinations, big.mark = ",")
    formatted_total_records <- format(overall_stats$total_records, big.mark = ",")
    
    message(glue("📊 FINAL SUMMARY:"))
    message(glue("   Years: {paste(years, collapse = ', ')}"))
    message(glue("   Conditions: {overall_stats$successful_conditions}/{overall_stats$total_conditions}"))
    message(glue("   Combinations: {formatted_successful_combinations}/{formatted_total_combinations}"))
    message(glue("   Total records: {formatted_total_records}"))
    if (overall_stats$total_combinations > 0) {
      message(glue("   Success rate: {round(overall_stats$successful_combinations/overall_stats$total_combinations*100, 1)}%"))
    }
    message(glue("   Total time: {round(total_time, 1)} minutes"))
    if (as.numeric(total_time) > 0) {
      message(glue("   Average rate: {round(overall_stats$successful_combinations / as.numeric(total_time), 1)} combinations/minute"))
    }
    
    # Create combined file
    if (length(all_condition_data) > 0) {
      message("\n💾 Creating combined file with labeled demographics...")
      
      combined_all <- bind_rows(all_condition_data)
      combined_filename <- file.path(output_dir, glue("ALL_CONDITIONS_{min(years)}_{max(years)}_FULL_DEMOGRAPHICS_LABELED.csv.xz"))
      vroom_write(combined_all, combined_filename, delim = ",")
      
      combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
      message(glue("   💾 Combined file: {basename(combined_filename)} ({combined_size_mb} MB)"))
      
      # Show final breakdown
      final_demo_summary <- combined_all %>%
        group_by(condition_name, age_label, race_label) %>%
        summarise(
          total_records = n(),
          .groups = 'drop'
        ) %>%
        arrange(desc(total_records)) %>%
        head(10)
      
      message("\n📈 Top condition-demographic combinations:")
      for (i in 1:min(nrow(final_demo_summary), 5)) {
        cond <- final_demo_summary$condition_name[i]
        age_lab <- final_demo_summary$age_label[i]
        race_lab <- final_demo_summary$race_label[i]
        rec_count <- format(final_demo_summary$total_records[i], big.mark = ",")
        message(glue("   {cond} + {age_lab} + {race_lab}: {rec_count} records"))
      }
    }
    
    return(overall_stats)
  }
  
  # =============================================================================
  # RUN THE CORRECTED DOWNLOAD
  # =============================================================================
  
  message("🎬 Starting CORRECTED CMS MMD download for 2020-2023...")
  message("✅ Using correct condition codes from your specification")
  message("🏷️  Adding proper demographic labels")
  
  # Quick test with correct condition code
  message("\n🧪 Running quick test with diabetes (code 15)...")
  test_result <- download_full_condition_optimized_safe("15", "diabetes_test", years = 2023, save_individual = FALSE, batch_size = 20)
  
  if (!is.null(test_result)) {
    message(glue("✅ Test successful! {test_result$successful_downloads} combinations in {round(test_result$time_minutes, 1)} minutes"))
    if (test_result$time_minutes > 0) {
      message(glue("   Rate: {round(test_result$successful_downloads / test_result$time_minutes, 1)} combinations/minute"))
    }
    
    # Show sample of what we got
    if (nrow(test_result$data) > 0) {
      sample_data <- test_result$data %>%
        select(condition_name, age_label, race_label, sex_label, year) %>%
        distinct() %>%
        head(5)
      
      message("\n📊 Sample demographic combinations found:")
      for (i in 1:nrow(sample_data)) {
        message(glue("   {sample_data$condition_name[i]} - {sample_data$age_label[i]} + {sample_data$race_label[i]} + {sample_data$sex_label[i]} ({sample_data$year[i]})"))
      }
    }
    
    message("\n🚀 Proceeding with corrected full download...\n")
    
    final_stats <- download_all_conditions_optimized_safe(years = 2020:2023)
    
    message("\n🎉 CORRECTED DOWNLOAD COMPLETE!")
    message(glue("Check the '{output_dir}' directory for all downloaded files."))
    
  } else {
    message("❌ Test failed. Please check the API connection and try again.")
  }
}