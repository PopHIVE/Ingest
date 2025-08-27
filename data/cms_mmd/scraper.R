## Code written by Claude Sonnet 4 and Gemini 2.5 Pro, with guidance from Dan Weinberger

update=F

if(update==T){
  # =============================================================================
  # CMS MMD Tool - ULTRA-FAST Download Script
  # Optimized for maximum speed while avoiding rate limits
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
  plan(multisession, workers = 3)  # 3 parallel workers
  
  # Setup
  base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
  output_dir <- "raw/staging_fully_stratified"
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Log file for tracking progress
  log_file <- file.path(output_dir, "download_log_fast.txt")
  cat("CMS MMD FAST Download Log - Started:", as.character(Sys.time()), "\n", file = log_file, append = FALSE)
  
  message("🚀 CMS MMD Tool - ULTRA-FAST Download (2020-2023)")
  message(glue("Output directory: {output_dir}"))
  
  # =============================================================================
  # DEMOGRAPHIC LABELS AND CONDITION MAPPINGS (SAME AS BEFORE)
  # =============================================================================
  
  age_labels <- list(
    "0" = "Under_65", "1" = "65_to_74", "2" = "75_to_84",
    "3" = "85_plus", "4" = "65_plus", "all" = "All_Ages"
  )
  
  race_labels <- list(
    "1" = "White", "2" = "Black", "4" = "Asian_Pacific_Islander",
    "5" = "Hispanic", "6" = "American_Indian_Native_American", "all" = "All_Races"
  )
  
  sex_labels <- list("1" = "Male", "2" = "Female", "all" = "All_Sexes")
  
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
  
  # =============================================================================
  # ULTRA-FAST DOWNLOAD FUNCTION
  # =============================================================================
  
  download_cms_data_fast <- function(combo_list, batch_id = 1) {
    
    # Very fast - only 0.1 second between requests within each worker
    delay_between_requests <- 0.1
    
    results <- list()
    
    for (i in seq_along(combo_list)) {
      combo <- combo_list[[i]]
      
      # Apply minimal delay
      if (i > 1) Sys.sleep(delay_between_requests)
      
      # Build URL parameters (same logic as before but streamlined)
      year_short <- substr(as.character(combo$year), 3, 4)
      suffix <- if (combo$year >= 2023) "_p" else if (combo$year >= 2021) "_f" else ""
      
      # Determine source pattern
      if (combo$age == "all" && combo$race == "all") {
        if (is.null(combo$sex)) {
          source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
          sexcat_param <- '.|IS NULL'
        } else {
          source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_{combo$sex}_{year_short}{suffix}")
          sexcat_param <- combo$sex
        }
        agecat_param <- '.|IS NULL'
        racecat_param <- '.|IS NULL'
        
      } else if (combo$age == "all") {
        if (combo$race == "1") {
          if (is.null(combo$sex)) {
            source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
            sexcat_param <- '.|IS NULL'
          } else {
            source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_{combo$sex}_{year_short}{suffix}")
            sexcat_param <- combo$sex
          }
        } else {
          source_pattern <- glue("prev_final_long_fltr12_racecat_{combo$race}_{year_short}{suffix}")
          sexcat_param <- if(is.null(combo$sex)) '.|IS NULL' else combo$sex
        }
        agecat_param <- '.|IS NULL'
        racecat_param <- combo$race
        
      } else if (combo$race == "all") {
        if (is.null(combo$sex)) {
          source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
          sexcat_param <- '.|IS NULL'
        } else {
          source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_{combo$sex}_{year_short}{suffix}")
          sexcat_param <- combo$sex
        }
        agecat_param <- combo$age
        racecat_param <- '.|IS NULL'
        
      } else {
        if (combo$race == "1") {
          if (is.null(combo$sex)) {
            source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
            sexcat_param <- '.|IS NULL'
          } else {
            source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_{combo$sex}_{year_short}{suffix}")
            sexcat_param <- combo$sex
          }
        } else {
          source_pattern <- glue("prev_final_long_fltr12_racecat_{combo$race}_{year_short}{suffix}")
          sexcat_param <- if(is.null(combo$sex)) '.|IS NULL' else combo$sex
        }
        agecat_param <- combo$age
        racecat_param <- combo$race
      }
      
      # Build parameters
      params <- list(
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
      )
      
      # Make request with quick timeout
      tryCatch({
        req <- request(base_url) %>% 
          req_url_query(!!!params) %>%
          req_timeout(15)  # Shorter timeout
        
        resp <- req_perform(req)
        
        if (resp$status_code == 200) {
          data_list <- resp_body_json(resp)
          
          if (length(data_list) > 0) {
            df <- bind_rows(data_list)
            
            # Add metadata
            df$age_code <- combo$age
            df$age_label <- age_labels[[as.character(combo$age)]]
            df$race_code <- combo$race
            df$race_label <- race_labels[[as.character(combo$race)]]
            df$sex_code <- ifelse(is.null(combo$sex), "all", combo$sex)
            df$sex_label <- sex_labels[[ifelse(is.null(combo$sex), "all", combo$sex)]]
            df$year <- combo$year
            df$condition_name <- combo$condition_name
            df$condition_code <- combo$condition_code
            df$source_pattern <- source_pattern
            df$download_timestamp <- Sys.time()
            df$batch_id <- batch_id
            
            results[[length(results) + 1]] <- df
          }
        }
      }, error = function(e) {
        # Just skip errors and continue
      })
    }
    
    return(results)
  }
  
  # =============================================================================
  # PARALLEL BATCH PROCESSING
  # =============================================================================
  
  download_condition_ultra_fast <- function(condition_code, condition_name, years = 2020:2023, 
                                            save_individual = TRUE) {
    
    message(glue("\n⚡ ULTRA-FAST: Downloading {condition_name} (condition {condition_code}) for years {min(years)}-{max(years)}..."))
    
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
        sex = ifelse(sex == "all_sex", NA, sex),
        condition_code = condition_code,
        condition_name = condition_name
      ) %>%
      select(-sex_idx)
    
    # Convert to list format for parallel processing
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
    
    # Split into parallel batches (larger batches for efficiency)
    batch_size <- ceiling(total_combinations / 3)  # 3 workers
    batches <- split(combo_list, ceiling(seq_along(combo_list) / batch_size))
    
    message(glue("   Will process {total_combinations} combinations in {length(batches)} parallel batches..."))
    
    start_time <- Sys.time()
    
    # Process batches in parallel
    with_progress({
      p <- progressor(steps = length(batches))
      
      batch_results <- future_map(seq_along(batches), function(i) {
        batch_data <- download_cms_data_fast(batches[[i]], batch_id = i)
        p()
        return(batch_data)
      }, .options = furrr_options(seed = TRUE))
    })
    
    # Combine results
    all_results <- unlist(batch_results, recursive = FALSE)
    successful_downloads <- length(all_results)
    
    if (length(all_results) > 0) {
      combined_data <- bind_rows(all_results)
      total_records <- nrow(combined_data)
      
      total_time <- difftime(Sys.time(), start_time, units = "mins")
      success_rate <- round(successful_downloads / total_combinations * 100, 1)
      
      message(glue("   ⚡ {condition_name} Summary:"))
      message(glue("      Successful: {successful_downloads}/{total_combinations} combinations ({success_rate}%)"))
      message(glue("      Total records: {format(total_records, big.mark = ',')}"))
      message(glue("      Time: {round(total_time, 1)} minutes"))
      message(glue("      Rate: {round(successful_downloads / as.numeric(total_time), 1)} combinations/minute"))
      
      if (save_individual) {
        filename <- file.path(output_dir, glue("{condition_name}_2020_2023_ULTRA_FAST.csv.xz"))
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
  # ULTRA-FAST ALL CONDITIONS DOWNLOAD
  # =============================================================================
  
  download_all_conditions_ultra_fast <- function(years = 2020:2023) {
    
    message(glue("⚡ ULTRA-FAST: Starting download of ALL CONDITIONS for years {min(years)}-{max(years)}..."))
    message("🚀 Using parallel processing with minimal delays")
    message("📈 Expected 20-30x speed improvement")
    
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
      
      message(glue("\n{paste(rep('⚡', 50), collapse = '')}"))
      message(glue("CONDITION {i}/{length(conditions_map)}: {condition$name} (code: {condition$code})"))
      message(glue("{paste(rep('⚡', 50), collapse = '')}"))
      
      result <- download_condition_ultra_fast(
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
        overall_stats$total_time <- overall_stats$total_time + result$time_minutes
        
        message(glue("✅ {condition$name} completed in {round(result$time_minutes, 1)} minutes"))
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
      }
    }
    
    end_time <- Sys.time()
    total_time <- difftime(end_time, start_time, units = "mins")
    
    message(glue("\n{paste(rep('🎉', 50), collapse = '')}"))
    message(glue("⚡ ULTRA-FAST DOWNLOAD COMPLETED! ({min(years)}-{max(years)})"))
    message(glue("{paste(rep('🎉', 50), collapse = '')}"))
    
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
      message("\n💾 Creating ultra-fast combined file...")
      
      combined_all <- bind_rows(all_condition_data)
      combined_filename <- file.path(output_dir, glue("ALL_CONDITIONS_{min(years)}_{max(years)}_ULTRA_FAST.csv.xz"))
      vroom_write(combined_all, combined_filename, delim = ",")
      
      combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
      message(glue("   💾 Combined file: {basename(combined_filename)} ({combined_size_mb} MB)"))
    }
    
    return(overall_stats)
  }
  
  # =============================================================================
  # RUN THE ULTRA-FAST DOWNLOAD
  # =============================================================================
  
  message("⚡ Starting ULTRA-FAST CMS MMD download...")
  message("🚀 Using 3 parallel workers with 0.1s delays")
  message("📈 Target: 60+ combinations/minute per condition")
  
  # Quick test
  message("\n🧪 Running ultra-fast test with diabetes...")
  test_result <- download_condition_ultra_fast("15", "diabetes_ultra_test", years = 2023, save_individual = FALSE)
  
  if (!is.null(test_result)) {
    message(glue("⚡ Ultra-fast test successful! {test_result$successful_downloads} combinations in {round(test_result$time_minutes, 1)} minutes"))
    message(glue("   Rate: {round(test_result$successful_downloads / test_result$time_minutes, 1)} combinations/minute"))
    message(glue("   Speed improvement: {round((test_result$successful_downloads / test_result$time_minutes) / 13.4, 1)}x faster"))
    
    message("\n🚀 Proceeding with ultra-fast full download...\n")
    
    final_stats <- download_all_conditions_ultra_fast(years = 2020:2023)
    
    message("\n⚡ ULTRA-FAST DOWNLOAD COMPLETE!")
    message(glue("Check the '{output_dir}' directory for all downloaded files."))
    
  } else {
    message("❌ Test failed. Please check the API connection and try again.")
  }
    
  }