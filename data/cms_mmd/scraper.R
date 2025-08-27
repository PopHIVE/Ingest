## Code written by Claude Sonnet 4 and Gemini 2.5 Pro, with guidance from Dan Weinberger
## Data from the Medicare/CMS Mapping Medicare Disparities Tool: https://data.cms.gov/tools/mapping-medicare-disparities-by-population


update=F

if(update==T){
  # =============================================================================
  # FULL SCALE CMS MMD DOWNLOADER - YOUR ACTUAL CONDITIONS + ALL DEMOGRAPHICS
  # =============================================================================
  
  library(httr2)
  library(dplyr)
  library(glue)
  library(purrr)
  
  # Setup
  base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
  output_dir <- "raw/cms_mmd_full_scale"
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  message("🚀🚀🚀 CMS MMD TOOL - FULL SCALE ALL CONDITIONS + ALL DEMOGRAPHICS 🚀🚀🚀")
  message(glue("📁 Output directory: {output_dir}"))
  
  # Labels
  age_labels <- list("0" = "Under_65", "1" = "65_to_74", "2" = "75_to_84", "3" = "85_plus", "4" = "65_plus", "all" = "All_Ages")
  race_labels <- list("1" = "White", "2" = "Black", "4" = "Asian_Pacific_Islander", "5" = "Hispanic", "6" = "American_Indian_Native_American", "all" = "All_Races")
  sex_labels <- list("1" = "Male", "2" = "Female", "all" = "All_Sexes")
  geography_labels <- list("n" = "National", "s" = "State", "c" = "County")
  
  # =============================================================================
  # YOUR ACTUAL CMS CONDITIONS
  # =============================================================================
  
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
  
  message(glue("📋 Processing {length(conditions_map)} conditions with codes: {paste(sapply(conditions_map, function(x) x$code), collapse=', ')}"))
  
  safe_label_lookup <- function(code, label_list, default = "Unknown") {
    if (is.null(code) || is.na(code) || code == "") return(default)
    code_str <- as.character(code)
    result <- label_list[[code_str]]
    if (is.null(result)) return(default)
    return(as.character(result))
  }
  
  # =============================================================================
  # ENHANCED JSON PARSER
  # =============================================================================
  
  parse_cms_json_enhanced <- function(json_list) {
    if (length(json_list) == 0) return(data.frame())
    
    data.frame(
      year = map_chr(json_list, ~ as.character(.x$year %||% "")),
      geography = map_chr(json_list, ~ as.character(.x$geography %||% "")),
      fips = map_chr(json_list, ~ as.character(.x$fips %||% "")),
      measure = map_chr(json_list, ~ as.character(.x$measure %||% "")),
      condition = map_chr(json_list, ~ as.character(.x$condition %||% "")),
      prevalence_rate = map_dbl(json_list, ~ as.numeric(.x$rate %||% NA)),
      agecat = map_chr(json_list, ~ as.character(.x$agecat %||% "")),
      racecat = map_chr(json_list, ~ as.character(.x$racecat %||% "")),
      sexcat = map_chr(json_list, ~ as.character(.x$sexcat %||% "")),
      dual = map_chr(json_list, ~ as.character(.x$dual %||% "")),
      eligcat = map_chr(json_list, ~ as.character(.x$eligcat %||% "")),
      fltr = map_chr(json_list, ~ as.character(.x$fltr %||% "")),
      dencat = map_chr(json_list, ~ as.character(.x$dencat %||% "")),
      stringsAsFactors = FALSE
    )
  }
  
  # =============================================================================
  # FULL SCALE DOWNLOAD FUNCTION - ALL DEMOGRAPHICS
  # =============================================================================
  
  full_scale_download_condition <- function(condition_code, condition_name, years = 2023) {
    
    message(glue("\n💥💥💥 FULL SCALE: {condition_name} (code: {condition_code}) 💥💥💥"))
    
    all_data <- list()
    successful_requests <- 0
    start_time <- Sys.time()
    total_requests <- 0
    
    # ALL DEMOGRAPHIC COMBINATIONS
    race_codes <- c("1", "2", "4", "5", "6", "all")  # All races
    sex_codes <- c("1", "2", "all")  # Male, Female, All
    age_codes <- c("0", "1", "2", "3", "4", "all")  # All age groups
    geo_codes <- c("c", "s", "n")  # County, State, National
    
    # Generate all combinations
    all_combinations <- expand.grid(
      year = years,
      race = race_codes,
      sex = sex_codes,
      age = age_codes,
      geography = geo_codes,
      stringsAsFactors = FALSE
    )
    
    total_combinations <- nrow(all_combinations)
    message(glue("   📊 Processing {format(total_combinations, big.mark = ',')} demographic combinations..."))
    
    # Process in batches to show progress
    batch_size <- 50
    batches <- split(1:nrow(all_combinations), ceiling(seq_len(nrow(all_combinations)) / batch_size))
    
    for (batch_num in seq_along(batches)) {
      batch_indices <- batches[[batch_num]]
      
      message(glue("   🔄 Batch {batch_num}/{length(batches)} ({length(batch_indices)} combinations)..."))
      
      for (i in batch_indices) {
        combo <- all_combinations[i, ]
        
        year_short <- substr(as.character(combo$year), 3, 4)
        suffix <- if (combo$year >= 2023) "_p" else if (combo$year >= 2021) "_f" else ""
        
        # Build source pattern based on race and sex
        if (combo$race == "1" && combo$sex == "all") {
          source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_all_{year_short}{suffix}")
        } else if (combo$race == "1" && combo$sex != "all") {
          source_pattern <- glue("prev_final_long_fltr12_racecat_1_sexcat_{combo$sex}_{year_short}{suffix}")
        } else if (combo$race == "all" && combo$sex == "all") {
          source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_all_{year_short}{suffix}")
        } else if (combo$race == "all" && combo$sex != "all") {
          source_pattern <- glue("prev_final_long_fltr12_racecat_all_sexcat_{combo$sex}_{year_short}{suffix}")
        } else if (combo$sex == "all") {
          source_pattern <- glue("prev_final_long_fltr12_racecat_{combo$race}_{year_short}{suffix}")
        } else {
          source_pattern <- glue("prev_final_long_fltr12_racecat_{combo$race}_sexcat_{combo$sex}_{year_short}{suffix}")
        }
        
        # Set up parameters
        agecat_param <- if (combo$age == "all") '.|IS NULL' else combo$age
        racecat_param <- if (combo$race == "all") '.|IS NULL' else combo$race
        sexcat_param <- if (combo$sex == "all") '.|IS NULL' else combo$sex
        
        # API request
        tryCatch({
          total_requests <- total_requests + 1
          Sys.sleep(0.015)  # Fast but respectful rate limiting
          
          resp <- request(base_url) %>% 
            req_url_query(
              `_source` = source_pattern,
              year = year_short,
              geography = combo$geography,
              measure = "v",
              condition = condition_code,
              sexcat = sexcat_param,
              agecat = agecat_param,
              racecat = racecat_param,
              dual = ".|IS NULL",
              eligcat = ".|IS NULL",
              fltr = "1",
              `_size` = 500000
            ) %>%
            req_timeout(8) %>%
            req_perform()
          
          if (resp$status_code == 200) {
            json_data <- resp_body_json(resp)
            
            if (length(json_data) > 0) {
              df <- parse_cms_json_enhanced(json_data)
              
              if (is.data.frame(df) && nrow(df) > 0) {
                # Add comprehensive metadata
                df$age_code <- combo$age
                df$age_label <- safe_label_lookup(combo$age, age_labels)
                df$race_code <- combo$race
                df$race_label <- safe_label_lookup(combo$race, race_labels)
                df$sex_code <- combo$sex
                df$sex_label <- safe_label_lookup(combo$sex, sex_labels)
                df$geography_code <- combo$geography
                df$geography_label <- safe_label_lookup(combo$geography, geography_labels)
                df$year_requested <- combo$year
                df$condition_name <- condition_name
                df$condition_code_requested <- condition_code
                df$source_pattern <- source_pattern
                df$request_id <- i
                df$batch_id <- batch_num
                df$download_timestamp <- as.character(Sys.time())
                
                all_data[[length(all_data) + 1]] <- df
                successful_requests <- successful_requests + 1
              }
            }
          }
          
        }, error = function(e) {
          # Continue processing
        })
      }
      
      # Batch progress report
      batch_success_rate <- round(successful_requests / total_requests * 100, 1)
      elapsed <- difftime(Sys.time(), start_time, units = "mins")
      rate_per_min <- round(total_requests / as.numeric(elapsed), 0)
      
      message(glue("      ✅ Batch {batch_num} complete: {successful_requests} successful ({batch_success_rate}%, {rate_per_min}/min)"))
    }
    
    # Combine and deduplicate results
    if (length(all_data) > 0) {
      message(glue("   🔧 Combining {length(all_data)} data frames..."))
      
      combined_data <- do.call(rbind, all_data)
      
      # Remove duplicates
      if (nrow(combined_data) > 1) {
        combined_data$unique_key <- paste(
          combined_data$year, combined_data$geography, combined_data$fips,
          combined_data$condition, combined_data$agecat, combined_data$racecat,
          combined_data$sexcat, sep = "_"
        )
        
        original_rows <- nrow(combined_data)
        combined_data <- combined_data[!duplicated(combined_data$unique_key), ]
        combined_data$unique_key <- NULL
        
        if (original_rows > nrow(combined_data)) {
          message(glue("   🔧 Removed {original_rows - nrow(combined_data)} duplicate records"))
        }
      }
      
      # Final analysis
      total_time <- difftime(Sys.time(), start_time, units = "mins")
      total_records <- nrow(combined_data)
      final_rate <- round(successful_requests / as.numeric(total_time), 0)
      non_na_rates <- sum(!is.na(combined_data$prevalence_rate))
      
      # Comprehensive breakdowns
      geo_summary <- combined_data %>% 
        group_by(geography_label) %>% 
        summarise(count = n(), non_na = sum(!is.na(prevalence_rate)), 
                  avg_rate = round(mean(prevalence_rate, na.rm = TRUE), 2), .groups = 'drop')
      
      race_summary <- combined_data %>% 
        group_by(race_label) %>% 
        summarise(count = n(), non_na = sum(!is.na(prevalence_rate)), 
                  avg_rate = round(mean(prevalence_rate, na.rm = TRUE), 2), .groups = 'drop')
      
      sex_summary <- combined_data %>% 
        group_by(sex_label) %>% 
        summarise(count = n(), non_na = sum(!is.na(prevalence_rate)), 
                  avg_rate = round(mean(prevalence_rate, na.rm = TRUE), 2), .groups = 'drop')
      
      message(glue("   ✅ FULL SCALE SUCCESS: {condition_name}"))
      message(glue("      📊 {successful_requests}/{total_requests} requests successful ({round(successful_requests/total_requests*100, 1)}%)"))
      message(glue("      📈 {format(total_records, big.mark = ',')} total records"))
      message(glue("      📊 {format(non_na_rates, big.mark = ',')} records with prevalence data"))
      message(glue("      ⏱️  {round(total_time, 1)} minutes ({format(final_rate, big.mark = ',')} req/min)"))
      
      message("      📍 GEOGRAPHY BREAKDOWN:")
      for (i in 1:nrow(geo_summary)) {
        geo <- geo_summary[i, ]
        message(glue("         {geo$geography_label}: {format(geo$count, big.mark = ',')} records (avg: {geo$avg_rate}%)"))
      }
      
      message("      👥 RACE BREAKDOWN:")
      for (i in 1:nrow(race_summary)) {
        race <- race_summary[i, ]
        message(glue("         {race$race_label}: {format(race$count, big.mark = ',')} records (avg: {race$avg_rate}%)"))
      }
      
      message("      ⚧ SEX BREAKDOWN:")
      for (i in 1:nrow(sex_summary)) {
        sex <- sex_summary[i, ]
        message(glue("         {sex$sex_label}: {format(sex$count, big.mark = ',')} records (avg: {sex$avg_rate}%)"))
      }
      
      # Save file
      filename <- file.path(output_dir, glue("{condition_name}_{min(years)}_{max(years)}_FULL_SCALE.csv"))
      write.csv(combined_data, filename, row.names = FALSE)
      
      if (file.exists(filename)) {
        file_size_mb <- round(file.size(filename) / 1024^2, 2)
        message(glue("      💾 {basename(filename)} ({file_size_mb} MB)"))
      }
      
      return(combined_data)
    }
    
    message(glue("   ❌ FULL SCALE FAILED: {condition_name}"))
    return(NULL)
  }
  
  # =============================================================================
  # FULL SCALE MULTI-CONDITION PROCESSOR
  # =============================================================================
  
  full_scale_multi_condition_download <- function(conditions, years = 2023) {
    
    message(glue("\n🚀🚀🚀 FULL SCALE MULTI-CONDITION DOWNLOAD 🚀🚀🚀"))
    message(glue("📋 Processing {length(conditions)} conditions"))
    message(glue("📅 Years: {paste(years, collapse=', ')}"))
    message(glue("👥 All race/sex/age/geography combinations"))
    
    overall_start <- Sys.time()
    all_results <- list()
    total_records <- 0
    summary_stats <- data.frame()
    
    for (i in seq_along(conditions)) {
      cond <- conditions[[i]]
      
      message(glue("\n💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥"))
      message(glue("💥 CONDITION {i}/{length(conditions)}: {cond$name} (code: {cond$code})"))
      message(glue("💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥"))
      
      condition_start <- Sys.time()
      result <- full_scale_download_condition(cond$code, cond$name, years)
      condition_time <- difftime(Sys.time(), condition_start, units = "mins")
      
      if (!is.null(result)) {
        all_results[[cond$name]] <- result
        condition_records <- nrow(result)
        total_records <- total_records + condition_records
        
        # Add to summary stats
        summary_stats <- rbind(summary_stats, data.frame(
          condition_name = cond$name,
          condition_code = cond$code,
          records = condition_records,
          time_minutes = round(as.numeric(condition_time), 2),
          records_per_minute = round(condition_records / as.numeric(condition_time), 0)
        ))
        
        message(glue("💥 ✅ {cond$name}: {format(condition_records, big.mark = ',')} records ({round(condition_time, 1)} min)"))
      } else {
        message(glue("💥 ❌ {cond$name}: Failed"))
        
        summary_stats <- rbind(summary_stats, data.frame(
          condition_name = cond$name,
          condition_code = cond$code,
          records = 0,
          time_minutes = round(as.numeric(condition_time), 2),
          records_per_minute = 0
        ))
      }
      
      # Progress update
      remaining <- length(conditions) - i
      elapsed_total <- difftime(Sys.time(), overall_start, units = "mins")
      avg_time_per_condition <- as.numeric(elapsed_total) / i
      estimated_remaining <- remaining * avg_time_per_condition
      
      message(glue("💥 Progress: {i}/{length(conditions)} complete"))
      message(glue("💥 Estimated time remaining: {round(estimated_remaining, 1)} minutes"))
    }
    
    total_time <- difftime(Sys.time(), overall_start, units = "hours")
    
    message(glue("\n🚀🚀🚀 FULL SCALE MULTI-CONDITION COMPLETE 🚀🚀🚀"))
    message(glue("✅ Completed: {length(all_results)}/{length(conditions)} conditions"))
    message(glue("📊 Total records: {format(total_records, big.mark = ',')}"))
    message(glue("⏱️  Total time: {round(total_time, 2)} hours"))
    
    # Show summary statistics
    if (nrow(summary_stats) > 0) {
      message("\n📈 CONDITION SUMMARY STATISTICS:")
      summary_stats <- summary_stats[order(-summary_stats$records), ]
      
      for (i in 1:nrow(summary_stats)) {
        stat <- summary_stats[i, ]
        message(glue("   {stat$condition_name}: {format(stat$records, big.mark = ',')} records ({stat$time_minutes} min)"))
      }
      
      # Save summary
      summary_file <- file.path(output_dir, glue("SUMMARY_STATISTICS_{min(years)}_{max(years)}.csv"))
      write.csv(summary_stats, summary_file, row.names = FALSE)
      message(glue("\n💾 Summary saved: {basename(summary_file)}"))
    }
    
    # Create master combined file
    if (length(all_results) > 0) {
      message("\n💾 Creating master combined file...")
      combined_all <- do.call(rbind, all_results)
      
      combined_filename <- file.path(output_dir, glue("ALL_CONDITIONS_{min(years)}_{max(years)}_MASTER.csv"))
      write.csv(combined_all, combined_filename, row.names = FALSE)
      
      combined_size_mb <- round(file.size(combined_filename) / 1024^2, 2)
      message(glue("💾 Master file: {basename(combined_filename)} ({combined_size_mb} MB)"))
      
      # Final master statistics
      message(glue("\n🎯 MASTER FILE STATISTICS:"))
      message(glue("   📊 Total records: {format(nrow(combined_all), big.mark = ',')}"))
      message(glue("   🏥 Conditions: {length(unique(combined_all$condition_name))}"))
      message(glue("   📍 Geographic levels: {paste(unique(combined_all$geography_label), collapse=', ')}"))
      message(glue("   👥 Race categories: {length(unique(combined_all$race_label[!is.na(combined_all$race_label)]))}"))
      message(glue("   ⚧ Sex categories: {length(unique(combined_all$sex_label[!is.na(combined_all$sex_label)]))}"))
      message(glue("   📈 Records with prevalence data: {format(sum(!is.na(combined_all$prevalence_rate)), big.mark = ',')}"))
    }
    
    return(all_results)
  }
  
  # =============================================================================
  # RUN FULL SCALE DOWNLOAD FOR ALL YOUR CONDITIONS
  # =============================================================================
  
  message(glue("\n🎯 STARTING FULL SCALE DOWNLOAD FOR ALL {length(conditions_map)} CONDITIONS..."))
  message("🚨 WARNING: This will take several hours and make thousands of API requests!")
  message("📊 Expected: ~324 requests per condition × 22 conditions = ~7,128 total requests")
  
  # Show condition list
  message("\n📋 CONDITIONS TO PROCESS:")
  for (i in seq_along(conditions_map)) {
    cond <- conditions_map[[i]]
    message(glue("   {i}. {cond$name} (code: {cond$code})"))
  }
  
  # Estimate time
  estimated_hours <- (length(conditions_map) * 324 * 0.02) / 60  # 0.02 seconds per request
  message(glue("⏰ Estimated time: ~{round(estimated_hours, 1)} hours"))
  
  # Ask for confirmation (comment out if running automatically)
  # readline("Press Enter to continue or Ctrl+C to cancel...")
  
  # Start the full scale download
  full_results <- full_scale_multi_condition_download(conditions_map, years = 2023)
  
  message("\n🎉🎉🎉 FULL SCALE DOWNLOAD COMPLETE! 🎉🎉🎉")
  message("🎯 You now have comprehensive CMS MMD data for all YOUR conditions and demographics!")
  }