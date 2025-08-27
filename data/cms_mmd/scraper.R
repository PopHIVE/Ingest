## Code written by Claude Sonnet 4 and Gemini 2.5 Pro, with guidance from Dan Weinberger

update=F

if(update==T){
  # =============================================================================
  # CMS MMD Tool - AUTO-LAUNCH NUCLEAR OPTION (COMPLETE FIXED VERSION)
  # Ultra-fast download with proper data frame handling
  # =============================================================================
  
  library(httr2)
  library(dplyr)
  library(vroom)
  library(glue)
  library(purrr)
  library(progressr)
  library(future)
  library(furrr)
  
  # Setup parallel processing
  plan(multisession, workers = 6)
  
  # Setup directories
  base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
  output_dir <- "raw/staging_nuclear_fixed"
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Log file
  log_file <- file.path(output_dir, "nuclear_download_log.txt")
  cat("CMS MMD NUCLEAR Download Log - Started:", as.character(Sys.time()), "\n", file = log_file, append = FALSE)
  
  message("💥💥💥 CMS MMD Tool - AUTO-LAUNCH NUCLEAR OPTION (FIXED) 💥💥💥")
  message("🚀 Ultra-fast download with proper data frame handling")
  message(glue("📁 Output directory: {output_dir}"))
  
  # =============================================================================
  # COMPLETE DEMOGRAPHIC LABELS AND MAPPINGS
  # =============================================================================
  
  age_labels <- list(
    "0" = "Under_65", 
    "1" = "65_to_74", 
    "2" = "75_to_84",
    "3" = "85_plus", 
    "4" = "65_plus", 
    "all" = "All_Ages"
  )
  
  race_labels <- list(
    "1" = "White", 
    "2" = "Black", 
    "4" = "Asian_Pacific_Islander",
    "5" = "Hispanic", 
    "6" = "American_Indian_Native_American", 
    "all" = "All_Races"
  )
  
  sex_labels <- list(
    "1" = "Male", 
    "2" = "Female", 
    "all" = "All_Sexes"
  )
  
  # Complete conditions mapping
  conditions_map <- list(
    list(code = "15", name = "diabetes"),
    list(code = "17", name = "hypertension"),
    list(code = "18", name = "hyperlidipemia"),
    list(code = "1", name = "alzheimers"),
    list(code = "2", name = "acute_myocardial_infarction"),
    list(code = "147", name = "anemia"),
    list(code = "4", name = "asthma"),
    list(code = "11", name = "atrial_fibrilation"),
    list(code = "5", name = "colorectal_breast_prostate_lung_cancer"),
    list(code = "12", name = "chronic_kidney"),
    list(code = "13", name = "copd"),
    list(code = "14", name = "depression"),
    list(code = "153", name = "glaucoma"),
    list(code = "16", name = "heart_failure_non_ischemic"),
    list(code = "152", name = "hip_pelvic_fracture"),
    list(code = "19", name = "ischemic_heart_disease"),
    list(code = "20", name = "obesity"),
    list(code = "21", name = "osteoporosis"),
    list(code = "155", name = "parkinsons"),
    list(code = "3", name = "rheumoatoid_arthritis"),
    list(code = "22", name = "schizophrenia_and_psycotic"),
    list(code = "23", name = "stroke_ischemic_attack")
  )
  
  message(glue("📋 Will process {length(conditions_map)} conditions"))
  
  # =============================================================================
  # SAFE LABEL LOOKUP FUNCTION
  # =============================================================================
  
  safe_label_lookup <- function(code, label_list, default = "Unknown") {
    if (is.null(code) || is.na(code) || code == "") return(default)
    code_str <- as.character(code)
    result <- label_list[[code_str]]
    if (is.null(result)) return(default)
    return(as.character(result))
  }
  
  # =============================================================================
  # NUCLEAR JSON PARSER - FIXED VERSION
  # =============================================================================
  
  nuclear_json_parser <- function(json_list) {
    
    if (length(json_list) == 0) {
      return(data.frame())
    }
    
    # Extract data manually to avoid all tibble/column issues
    all_data <- list()
    
    for (i in seq_along(json_list)) {
      record <- json_list[[i]]
      
      if (is.list(record) && length(record) > 0) {
        
        # Create clean record with fixed column names - ensure no NULLs
        clean_record <- list(
          year = as.character(record$year %||% ""),
          geography = as.character(record$geography %||% ""),
          fips = as.character(record$fips %||% ""),
          measure = as.character(record$measure %||% ""),
          condition = as.character(record$condition %||% ""),
          prevalence = as.numeric(record$prevalence %||% NA),
          beneficiaries = as.numeric(record$beneficiaries %||% NA),
          agecat = as.character(record$agecat %||% ""),
          racecat = as.character(record$racecat %||% ""),
          sexcat = as.character(record$sexcat %||% ""),
          dual = as.character(record$dual %||% ""),
          eligcat = as.character(record$eligcat %||% ""),
          population = as.character(record$population %||% ""),
          fltr = as.character(record$fltr %||% ""),
          record_id = i
        )
        
        all_data[[i]] <- clean_record
      }
    }
    
    if (length(all_data) == 0) {
      return(data.frame())
    }
    
    # Convert to data.frame manually - completely avoid tibble
    result_df <- data.frame(
      year = map_chr(all_data, ~ as.character(.x$year %||% "")),
      geography = map_chr(all_data, ~ as.character(.x$geography %||% "")),
      fips = map_chr(all_data, ~ as.character(.x$fips %||% "")),
      measure = map_chr(all_data, ~ as.character(.x$measure %||% "")),
      condition = map_chr(all_data, ~ as.character(.x$condition %||% "")),
      prevalence = map_dbl(all_data, ~ as.numeric(.x$prevalence) %||% NA_real_),
      beneficiaries = map_dbl(all_data, ~ as.numeric(.x$beneficiaries) %||% NA_real_),
      agecat = map_chr(all_data, ~ as.character(.x$agecat %||% "")),
      racecat = map_chr(all_data, ~ as.character(.x$racecat %||% "")),
      sexcat = map_chr(all_data, ~ as.character(.x$sexcat %||% "")),
      dual = map_chr(all_data, ~ as.character(.x$dual %||% "")),
      eligcat = map_chr(all_data, ~ as.character(.x$eligcat %||% "")),
      population = map_chr(all_data, ~ as.character(.x$population %||% "")),
      fltr = map_chr(all_data, ~ as.character(.x$fltr %||% "")),
      record_id = map_dbl(all_data, ~ as.numeric(.x$record_id) %||% NA_real_),
      stringsAsFactors = FALSE
    )
    
    return(result_df)
  }
  
  # =============================================================================
  # NUCLEAR DOWNLOAD WORKER FUNCTION (FIXED)
  # =============================================================================
  
  nuclear_download_worker <- function(combo_list, worker_id = 1) {
    
    delay_between_requests <- 0.05
    results <- list()
    success_count <- 0
    error_count <- 0
    
    cat(glue("🚀 Worker {worker_id} starting with {length(combo_list)} combinations...\n"))
    
    for (i in seq_along(combo_list)) {
      combo <- combo_list[[i]]
      
      if (i > 1) Sys.sleep(delay_between_requests)
      
      # Build URL parameters
      year_short <- substr(as.character(combo$year), 3, 4)
      suffix <- if (combo$year >= 2023) "_p" else if (combo$year >= 2021) "_f" else ""
      
      # Determine source pattern
      source_pattern <- if (combo$age == "all" && combo$race == "all") {
        if (is.null(combo$sex)) {
          glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
        } else {
          glue("prev_final_long_fltr12_racecat_all_sexcat_{combo$sex}_{year_short}{suffix}")
        }
      } else if (combo$age == "all") {
        if (combo$race == "1") {
          if (is.null(combo$sex)) {
            glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
          } else {
            glue("prev_final_long_fltr12_racecat_1_sexcat_{combo$sex}_{year_short}{suffix}")
          }
        } else {
          glue("prev_final_long_fltr12_racecat_{combo$race}_{year_short}{suffix}")
        }
      } else if (combo$race == "all") {
        if (is.null(combo$sex)) {
          glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
        } else {
          glue("prev_final_long_fltr12_racecat_all_sexcat_{combo$sex}_{year_short}{suffix}")
        }
      } else {
        if (combo$race == "1") {
          if (is.null(combo$sex)) {
            glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
          } else {
            glue("prev_final_long_fltr12_racecat_1_sexcat_{combo$sex}_{year_short}{suffix}")
          }
        } else {
          glue("prev_final_long_fltr12_racecat_{combo$race}_{year_short}{suffix}")
        }
      }
      
      # Build filter parameters
      agecat_param <- if (combo$age == "all") '.|IS NULL' else combo$age
      racecat_param <- if (combo$race == "all") '.|IS NULL' else combo$race
      sexcat_param <- if (is.null(combo$sex)) '.|IS NULL' else combo$sex
      
      # Make API request
      result <- tryCatch({
        resp <- request(base_url) %>% 
          req_url_query(
            `_source` = source_pattern,
            population = 'f',
            year = year_short,
            geography = 'c',
            measure = 'v',
            condition = combo$condition_code,
            agecat = agecat_param,
            racecat = racecat_param,
            sexcat = sexcat_param,
            dual = '.|IS NULL',
            eligcat = '.|IS NULL',
            fltr = '1',
            `_size` = 500000
          ) %>%
          req_timeout(10) %>%
          req_perform()
        
        if (resp$status_code == 200) {
          json_data <- resp_body_json(resp)
          
          if (length(json_data) > 0) {
            df <- nuclear_json_parser(json_data)
            
            if (nrow(df) > 0) {
              # Add demographic metadata with SAFE lookups - no NULLs allowed
              df$age_code <- as.character(combo$age)
              df$age_label <- safe_label_lookup(combo$age, age_labels, "Unknown_Age")
              df$race_code <- as.character(combo$race)
              df$race_label <- safe_label_lookup(combo$race, race_labels, "Unknown_Race")
              df$sex_code <- if(is.null(combo$sex)) "all" else as.character(combo$sex)
              df$sex_label <- safe_label_lookup(if(is.null(combo$sex)) "all" else combo$sex, sex_labels, "Unknown_Sex")
              df$year_requested <- as.numeric(combo$year)
              df$condition_name <- as.character(combo$condition_name)
              df$condition_code_requested <- as.character(combo$condition_code)
              df$worker_id <- as.numeric(worker_id)
              df$request_id <- as.character(paste0("W", worker_id, "_R", i))
              df$source_pattern <- as.character(source_pattern)
              df$download_timestamp <- as.character(Sys.time())
              
              success_count <- success_count + 1
              return(df)
            }
          }
        }
        
        return(NULL)
        
      }, error = function(e) {
        error_count <- error_count + 1
        if (error_count > 5 && success_count < 3) {
          delay_between_requests <<- 0.1
        }
        return(NULL)
      })
      
      if (!is.null(result)) {
        results[[length(results) + 1]] <- result
        error_count <- 0
      }
      
      # Progress updates
      if (i %% 15 == 0) {
        success_rate <- round(length(results) / i * 100, 1)
        cat(glue("Worker {worker_id}: {i}/{length(combo_list)} ({success_rate}%)\n"))
      }
    }
    
    # Final worker summary
    final_success_rate <- round(length(results) / length(combo_list) * 100, 1)
    total_records <- sum(map_int(results, nrow))
    cat(glue("Worker {worker_id} COMPLETE: {length(results)}/{length(combo_list)} ({final_success_rate}%), {total_records} records\n"))
    
    return(results)
  }
  
  # =============================================================================
  # NUCLEAR CONDITION DOWNLOAD FUNCTION (FIXED)
  # =============================================================================
  
  nuclear_condition_download <- function(condition_code, condition_name, years = 2020:2023, 
                                         save_individual = TRUE) {
    
    message(glue("\n💥 NUCLEAR: {condition_name} (condition {condition_code}) for years {min(years)}-{max(years)}..."))
    
    # Generate all demographic combinations
    age_codes <- c("0", "1", "2", "3", "4", "all")
    race_codes <- c("1", "2", "4", "5", "6", "all")
    sex_options <- list(
      list(code = "1", name = "male"),
      list(code = "2", name = "female"),
      list(code = NULL, name = "all")
    )
    
    # Create full combination matrix
    all_combinations <- expand.grid(
      year = years,
      age = age_codes,
      race = race_codes,
      sex_idx = 1:length(sex_options),
      stringsAsFactors = FALSE
    ) %>%
      mutate(
        sex = map_chr(sex_idx, ~ sex_options[[.x]]$code %||% "all_sex"),
        sex = ifelse(sex == "all_sex", NA, sex),
        condition_code = condition_code,
        condition_name = condition_name
      ) %>%
      select(-sex_idx)
    
    # Convert to list format
    combo_list <- pmap(all_combinations, function(year, age, race, sex, condition_code, condition_name) {
      list(
        year = year, 
        age = age, 
        race = race, 
        sex = if(is.na(sex)) NULL else sex,
        condition_code = condition_code, 
        condition_name = condition_name
      )
    })
    
    total_combinations <- length(combo_list)
    
    # Split into 6 parallel batches
    batch_size <- ceiling(total_combinations / 6)
    batches <- split(combo_list, ceiling(seq_along(combo_list) / batch_size))
    
    message(glue("   💥 Processing {total_combinations} combinations with {length(batches)} nuclear workers..."))
    
    start_time <- Sys.time()
    
    # Process batches in parallel
    batch_results <- future_map(seq_along(batches), function(i) {
      nuclear_download_worker(batches[[i]], worker_id = i)
    }, .options = furrr_options(seed = TRUE))
    
    # Combine results using FIXED method
    all_results <- unlist(batch_results, recursive = FALSE)
    successful_downloads <- length(all_results)
    
    if (length(all_results) > 0) {
      message("   💥 Nuclear combination of results...")
      
      # FIXED: Proper data frame combination with validation
      combined_data <- NULL
      
      if (length(all_results) == 1) {
        combined_data <- all_results[[1]]
      } else {
        # Get the first valid data frame as template
        template_df <- all_results[[1]]
        template_cols <- names(template_df)
        
        message(glue("   🔧 Combining {length(all_results)} data frames..."))
        
        # Validate and combine each data frame
        valid_dfs <- list()
        
        for (i in seq_along(all_results)) {
          df <- all_results[[i]]
          
          # Check if it's a proper data frame
          if (is.data.frame(df) && nrow(df) > 0) {
            
            # Ensure all columns exist and are in the same order
            missing_cols <- setdiff(template_cols, names(df))
            extra_cols <- setdiff(names(df), template_cols)
            
            if (length(missing_cols) > 0) {
              # Add missing columns with appropriate defaults
              for (col in missing_cols) {
                if (col %in% c("prevalence", "beneficiaries", "year_requested", "worker_id", "record_id")) {
                  df[[col]] <- NA_real_
                } else {
                  df[[col]] <- NA_character_
                }
              }
            }
            
            if (length(extra_cols) > 0) {
              # Remove extra columns
              df <- df[, template_cols, drop = FALSE]
            }
            
            # Reorder columns to match template
            df <- df[, template_cols, drop = FALSE]
            
            # Ensure all columns are proper types
            for (col in names(df)) {
              if (is.list(df[[col]])) {
                df[[col]] <- as.character(df[[col]])
              }
            }
            
            valid_dfs[[length(valid_dfs) + 1]] <- df
          }
        }
        
        if (length(valid_dfs) > 0) {
          # Use dplyr::bind_rows for safer combining
          tryCatch({
            combined_data <- dplyr::bind_rows(valid_dfs)
          }, error = function(e) {
            message("   ⚠️  bind_rows failed, using manual rbind...")
            combined_data <- do.call(rbind, valid_dfs)
          })
        }
      }
      
      if (is.null(combined_data) || nrow(combined_data) == 0) {
        message(glue("   💥 ❌ {condition_name}: No valid data to combine"))
        return(NULL)
      }
      
      total_records <- nrow(combined_data)
      total_time <- difftime(Sys.time(), start_time, units = "mins")
      success_rate <- round(successful_downloads / total_combinations * 100, 1)
      rate_per_minute <- round(successful_downloads / as.numeric(total_time), 1)
      
      message(glue("   💥 {condition_name} NUCLEAR RESULTS:"))
      message(glue("      ✅ {successful_downloads}/{total_combinations} combinations ({success_rate}%)"))
      message(glue("      📊 {format(total_records, big.mark = ',')} total records"))
      message(glue("      ⏱️  {round(total_time, 1)} minutes ({format(rate_per_minute, big.mark = ',')} comb/min)"))
      message(glue("      🚀 Speed: {round(rate_per_minute / 13.4, 1)}x faster than baseline"))
      
      # FIXED: Save with proper CSV format
      if (save_individual) {
        filename <- file.path(output_dir, glue("{condition_name}_{min(years)}_{max(years)}_NUCLEAR.csv"))
        
        message("      💾 Saving as CSV...")
        
        # Validate data frame before saving
        if (is.data.frame(combined_data) && nrow(combined_data) > 0) {
          
          # Final cleanup - ensure all columns are properly formatted
          for (col in names(combined_data)) {
            if (is.list(combined_data[[col]])) {
              combined_data[[col]] <- as.character(combined_data[[col]])
            }
          }
          
          write.csv(combined_data, filename, row.names = FALSE)
          
          # Verify the file was written correctly
          if (file.exists(filename)) {
            file_size_mb <- round(file.size(filename) / 1024^2, 2)
            
            # Quick validation read
            tryCatch({
              test_read <- read.csv(filename, nrows = 5)
              message(glue("      ✅ {basename(filename)} ({file_size_mb} MB) - {ncol(test_read)} cols"))
            }, error = function(e) {
              message(glue("      ⚠️  File written but validation failed: {e$message}"))
            })
          }
        } else {
          message("      ❌ Invalid data frame structure - cannot save")
        }
      }
      
      return(list(
        data = combined_data,
        successful_downloads = successful_downloads,
        total_combinations = total_combinations,
        total_records = total_records,
        time_minutes = as.numeric(total_time),
        rate_per_minute = rate_per_minute
      ))
    } else {
      message(glue("   💥 ❌ {condition_name}: No data retrieved"))
      return(NULL)
    }
  }
  
  # =============================================================================
  # NUCLEAR FULL DOWNLOAD - ALL CONDITIONS (FIXED)
  # =============================================================================
  
  nuclear_full_download <- function(years = 2020:2023) {
    
    message(glue("\n💥💥💥 NUCLEAR FULL DOWNLOAD: All conditions for {min(years)}-{max(years)} 💥💥💥"))
    message("🚀 Expected completion time: 10-20 minutes for all 22 conditions!")
    
    overall_stats <- list(
      total_conditions = length(conditions_map),
      successful_conditions = 0,
      total_combinations = 0,
      successful_combinations = 0,
      total_records = 0,
      total_time = 0,
      rates = c(),
      condition_results = list()
    )
    
    start_time <- Sys.time()
    all_condition_data <- list()
    
    # Process each condition
    for (i in seq_along(conditions_map)) {
      condition <- conditions_map[[i]]
      
      message(glue("\n💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥"))
      message(glue("💥 CONDITION {i}/{length(conditions_map)}: {condition$name} (code: {condition$code})"))
      message(glue("💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥"))
      
      condition_start <- Sys.time()
      
      result <- nuclear_condition_download(
        condition_code = condition$code,
        condition_name = condition$name,
        years = years,
        save_individual = TRUE
      )
      
      condition_time <- difftime(Sys.time(), condition_start, units = "mins")
      
      if (!is.null(result)) {
        all_condition_data[[condition$name]] <- result$data
        overall_stats$successful_conditions <- overall_stats$successful_conditions + 1
        overall_stats$total_combinations <- overall_stats$total_combinations + result$total_combinations
        overall_stats$successful_combinations <- overall_stats$successful_combinations + result$successful_downloads
        overall_stats$total_records <- overall_stats$total_records + result$total_records
        overall_stats$total_time <- overall_stats$total_time + result$time_minutes
        overall_stats$rates <- c(overall_stats$rates, result$rate_per_minute)
        
        message(glue("💥 ✅ {condition$name}: {format(result$rate_per_minute, big.mark = ',')} comb/min in {round(condition_time, 1)} min"))
        message(glue("   📊 {format(result$total_records, big.mark = ',')} records captured"))
        
        # Log to file
        cat(glue("{condition$name}: {result$successful_downloads}/{result$total_combinations} combinations, {result$total_records} records, {round(condition_time, 1)} min\n"), 
            file = log_file, append = TRUE)
        
      } else {
        message(glue("💥 ❌ {condition$name} failed completely"))
        cat(glue("{condition$name}: FAILED\n"), file = log_file, append = TRUE)
      }
      
      # Overall progress tracking
      elapsed_time <- difftime(Sys.time(), start_time, units = "mins")
      if (i > 0) {
        avg_time_per_condition <- as.numeric(elapsed_time) / i
        remaining_time <- avg_time_per_condition * (length(conditions_map) - i)
        
        message(glue("💥 📊 PROGRESS: {i}/{length(conditions_map)} conditions completed"))
        message(glue("   ⏱️  Elapsed: {round(elapsed_time, 1)} min, Est. remaining: {round(remaining_time, 1)} min"))
        
        if (length(overall_stats$rates) > 0) {
          current_avg_rate <- round(mean(overall_stats$rates), 0)
          message(glue("   🚀 Average rate so far: {format(current_avg_rate, big.mark = ',')} comb/min"))
        }
        
        message(glue("   📊 Total records so far: {format(overall_stats$total_records, big.mark = ',')}"))
      }
    }
    
    total_elapsed <- difftime(Sys.time(), start_time, units = "mins")
    
    message(glue("\n💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥"))
    message(glue("💥💥💥 NUCLEAR DOWNLOAD COMPLETE! 💥💥💥"))
    message(glue("💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥"))
    
    message(glue("🏁 FINAL NUCLEAR RESULTS:"))
    message(glue("   🎯 Conditions completed: {overall_stats$successful_conditions}/{overall_stats$total_conditions}"))
    message(glue("   📊 Total combinations: {format(overall_stats$successful_combinations, big.mark = ',')}"))
    message(glue("   📈 Total records: {format(overall_stats$total_records, big.mark = ',')}"))
    message(glue("   ⏱️  Total time: {round(total_elapsed, 1)} minutes"))
    
    if (length(overall_stats$rates) > 0) {
      avg_rate <- round(mean(overall_stats$rates), 0)
      max_rate <- round(max(overall_stats$rates), 0)
      
      message(glue("   🚀 PERFORMANCE:"))
      message(glue("     Average rate: {format(avg_rate, big.mark = ',')} combinations/minute"))
      message(glue("     Peak rate: {format(max_rate, big.mark = ',')} combinations/minute"))
      message(glue("     Speed improvement: {round(avg_rate / 13.4, 0)}x faster"))
      
      # Calculate time saved
      original_estimated_hours <- (overall_stats$total_combinations / 13.4) / 60
      nuclear_actual_hours <- as.numeric(total_elapsed) / 60
      time_saved_hours <- original_estimated_hours - nuclear_actual_hours
      
      message(glue("   ⚡ TIME SAVED: ~{round(time_saved_hours, 1)} hours!"))
    }
    
    # Create massive combined file with FIXED combining
    if (length(all_condition_data) > 0) {
      message("\n💥 Creating FINAL COMBINED FILE...")
      
      # FIXED: Use proper data frame combining
      tryCatch({
        combined_all <- dplyr::bind_rows(all_condition_data)
      }, error = function(e) {
        message("   ⚠️  bind_rows failed for combined file, using rbind...")
        combined_all <- do.call(rbind, all_condition_data)
      })
      
      combined_filename <- file.path(output_dir, glue("ALL_CONDITIONS_{min(years)}_{max(years)}_NUCLEAR_COMPLETE.csv"))
      
      message("   💾 Writing final combined CSV file...")
      write.csv(combined_all, combined_filename, row.names = FALSE)
      
      combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
      message(glue("   💾 💥 FINAL FILE: {basename(combined_filename)} ({combined_size_mb} MB)"))
      
      # Show breakdown by condition
      if ("condition_name" %in% names(combined_all)) {
        condition_counts <- sort(table(combined_all$condition_name), decreasing = TRUE)
        message("\n📊 RECORDS PER CONDITION:")
        for (i in 1:min(length(condition_counts), 10)) {
          cond_name <- names(condition_counts)[i]
          count <- format(condition_counts[[cond_name]], big.mark = ",")
          message(glue("   {i}. {cond_name}: {count} records"))
        }
      }
    }
    
    return(overall_stats)
  }
  # =============================================================================
  # AUTO-LAUNCH SECTION - ACTUAL EXECUTION
  # =============================================================================
  
  message("\n💥💥💥 AUTO-LAUNCHING NUCLEAR DOWNLOAD (FIXED) 💥💥💥")
  message("🚀 No confirmation needed - starting immediately!")
  message("⚡ Downloading ALL 22 conditions at maximum speed...")
  message(glue("📁 Files will be saved to: {output_dir}"))
  
  # Record start time for total mission time
  mission_start <- Sys.time()
  
  # Execute nuclear download automatically
  message("\n💥 NUCLEAR LAUNCH INITIATED AUTOMATICALLY! 💥")
  final_nuclear_stats <- nuclear_full_download(years = 2020:2023)
  
  # Calculate total mission time
  total_mission_time <- difftime(Sys.time(), mission_start, units = "mins")
  
  message(glue("\n🎉🎉🎉 NUCLEAR MISSION ACCOMPLISHED! 🎉🎉🎉"))
  message(glue("⏱️  Total mission time: {round(total_mission_time, 1)} minutes"))
  message(glue("📁 All files saved to: {output_dir}"))
  
  if (final_nuclear_stats$successful_conditions > 0) {
    avg_rate <- round(mean(final_nuclear_stats$rates), 0)
    message(glue("🚀 Final average speed: {format(avg_rate, big.mark = ',')} combinations/minute"))
    message(glue("⚡ Speed achievement: {round(avg_rate / 13.4, 0)}x faster than baseline!"))
    
    # Achievement badges
    if (avg_rate > 5000) {
      message("🏆 ACHIEVEMENT UNLOCKED: NUCLEAR SPEED DEMON!")
    } else if (avg_rate > 2000) {
      message("🥇 ACHIEVEMENT UNLOCKED: SPEED MASTER!")
    } else if (avg_rate > 1000) {
      message("🥈 ACHIEVEMENT UNLOCKED: RAPID FIRE!")
    } else {
      message("🥉 ACHIEVEMENT UNLOCKED: FASTER THAN BASELINE!")
    }
  }
  
  message("\n💥 Auto-launch nuclear download complete! 💥")
  message(glue("📋 Check log file: {log_file}"))
  message("🎯 Data ready for analysis!")
  
  # =============================================================================
  # UTILITY FUNCTIONS FOR CHECKING RESULTS
  # =============================================================================
  
  # Function to check results
  check_nuclear_results <- function() {
    csv_files <- list.files(output_dir, pattern = "*.csv$", full.names = TRUE)
    message(glue("📁 Found {length(csv_files)} CSV files"))
    
    total_size <- 0
    for (file in csv_files) {
      size_mb <- round(file.size(file) / 1024^2, 2)
      total_size <- total_size + size_mb
      message(glue("   📄 {basename(file)}: {size_mb} MB"))
    }
    
    message(glue("💾 Total data size: {round(total_size, 2)} MB"))
    return(csv_files)
  }
  
  # Show final status
  message("\n📖 Available commands:")
  message("   check_nuclear_results()  # Check all downloaded files")
  message(glue("   list.files('{output_dir}')  # List all files"))
  
  # Final message
  message("\n💥💥💥 NUCLEAR LAUNCH COMPLETE - READY FOR ANALYSIS! 💥💥💥")
 
  }