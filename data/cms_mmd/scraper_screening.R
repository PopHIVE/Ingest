#This code was written using Claude Sonnet 4, Gemini 2.5 Pro, with Guidance from Dan Weinberger
# The script downloads screening behavior data from CMS/Medicare from the Mapping Medicare Disparities tool: https://data.cms.gov/tools/mapping-medicare-disparities-by-population

library(httr2)
library(dplyr)
library(glue)
library(purrr)
library(future)
library(furrr)
library(readr)

base_url <- "https://data.cms.gov/data-api/v1/mmd-tool/"
output_dir <- "raw/staging_screening"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
age_labels <- list("0" = "Under_65", "1" = "65_to_74", "2" = "75_to_84", "3" = "85_plus", "4" = "65_plus", "all" = "All_Ages")
race_labels <- list("1" = "White", "2" = "Black", "4" = "Asian_Pacific_Islander", "5" = "Hispanic", "6" = "American_Indian_Native_American", "all" = "All_Races")
sex_labels <- list("1" = "Male", "2" = "Female", "all" = "All_Sexes")
geography_labels <- list("n" = "National", "s" = "State", "c" = "County")


# =============================================================================
# OPTIMIZED DOWNLOAD FUNCTION - SELF-CONTAINED FOR PARALLELISM
# =============================================================================

full_scale_download_condition_optimized <- function(condition_code, condition_name, years = 2023) {
  
  message(glue("\n💥💥💥 STARTING PARALLEL DOWNLOAD (PREVENTIVE CARE): {condition_name} (code: {condition_code}) 💥💥💥"))
  start_time <- Sys.time()
  
  # ... (keep all the helper functions the same) ...
  safe_label_lookup <- function(code, label_list, default = "Unknown") {
    if (is.null(code) || is.na(code) || code == "") return(default)
    code_str <- as.character(code)
    result <- label_list[[code_str]]
    if (is.null(result)) return(default)
    return(as.character(result))
  }
  
  parse_cms_json_enhanced <- function(json_list) {
    if (length(json_list) == 0) return(tibble())
    tibble(
      year = map_chr(json_list, ~ as.character(.x$year %||% NA_character_)),
      geography = map_chr(json_list, ~ as.character(.x$geography %||% NA_character_)),
      fips = map_chr(json_list, ~ as.character(.x$fips %||% NA_character_)),
      measure = map_chr(json_list, ~ as.character(.x$measure %||% NA_character_)),
      condition = map_chr(json_list, ~ as.character(.x$condition %||% NA_character_)),
      care_rate = map_dbl(json_list, ~ as.numeric(.x$rate %||% NA_real_)), # Changed from prevalence_rate
      agecat = map_chr(json_list, ~ as.character(.x$agecat %||% NA_character_)),
      racecat = map_chr(json_list, ~ as.character(.x$racecat %||% NA_character_)),
      sexcat = map_chr(json_list, ~ as.character(.x$sexcat %||% NA_character_)),
      dual = map_chr(json_list, ~ as.character(.x$dual %||% NA_character_)),
      eligcat = map_chr(json_list, ~ as.character(.x$eligcat %||% NA_character_)),
      fltr = map_chr(json_list, ~ as.character(.x$fltr %||% NA_character_)),
      dencat = map_chr(json_list, ~ as.character(.x$dencat %||% NA_character_))
    )
  }
  
  # Generate all combinations (same as before)
  all_combinations <- expand.grid(
    year = years,
    race = c("1", "2", "4", "5", "6", "all"),
    sex = c("1", "2", "all"),
    age = c("0", "1", "2", "3", "4", "all"),
    geography = c("c", "s", "n"),
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      condition_code_requested = condition_code,
      condition_name = condition_name
    )
  
  total_combinations <- nrow(all_combinations)
  message(glue("   📊 Preparing to make {format(total_combinations, big.mark = ',')} API requests in parallel..."))
  
  # Modified function for preventive care data
  fetch_combo_data <- function(combo_row) {
    year_short <- substr(as.character(combo_row$year), 3, 4)
    suffix <- if (combo_row$year >= 2023) "_p" else if (combo_row$year >= 2021) "_f" else ""
    
    # Keep source pattern consistent - don't vary it by race/sex
    source_pattern <- glue("precare_final_long_eligcat_all_dual_all_{year_short}{suffix}")
    
    # Updated request parameters for preventive care
    req_params <- list(
      `_source` = source_pattern,
      population = "f", 
      year = year_short,
      geography = combo_row$geography,
      measure = "c", 
      condition = combo_row$condition_code_requested,
      sexcat = if (combo_row$sex == "all") '.|IS NULL' else combo_row$sex,
      agecat = if (combo_row$age == "all") '.|IS NULL' else combo_row$age,
      racecat = if (combo_row$race == "all") '.|IS NULL' else combo_row$race,
      dual = ".|IS NULL",
      eligcat = ".|IS NULL",
      fltr = "1",
      `_size` = 500000
    )
    
    tryCatch({
      resp <- request(base_url) %>% 
        req_url_query(!!!req_params) %>%
        req_timeout(15) %>%
        req_retry(max_tries = 3, is_transient = ~ resp_status(.x) %in% c(429, 500, 503)) %>%
        req_perform()
      
      if (resp_status(resp) == 200) {
        json_data <- resp_body_json(resp)
        if (length(json_data) > 0) {
          df <- parse_cms_json_enhanced(json_data)
          if(nrow(df) > 0) {
            df <- df %>% mutate(
              age_label = safe_label_lookup(combo_row$age, age_labels),
              race_label = safe_label_lookup(combo_row$race, race_labels),
              sex_label = safe_label_lookup(combo_row$sex, sex_labels),
              geography_label = safe_label_lookup(combo_row$geography, geography_labels),
              condition_name = combo_row$condition_name
            )
            return(df)
          }
        }
      }
      return(tibble())
    }, error = function(e) {
      return(tibble())
    })
  }
  
  # Rest of the function remains the same...
  combinations_list <- split(all_combinations, seq(nrow(all_combinations)))
  
  combined_data <- future_map_dfr(
    combinations_list, 
    fetch_combo_data, 
    .progress = TRUE, 
    .options = furrr_options(seed = TRUE, packages = c("httr2", "dplyr", "purrr", "glue", "jsonlite"))
  )
  
  if (nrow(combined_data) > 0) {
    message(glue("   🔧 De-duplicating results..."))
    original_rows <- nrow(combined_data)
    combined_data <- combined_data %>%
      distinct(year, geography, fips, condition, agecat, racecat, sexcat, .keep_all = TRUE)
    deduped_rows <- original_rows - nrow(combined_data)
    if (deduped_rows > 0) {
      message(glue("   🔧 Removed {deduped_rows} duplicate records"))
    }
    
    total_time <- difftime(Sys.time(), start_time, units = "mins")
    total_records <- nrow(combined_data)
    
    message(glue("\n   ✅ PARALLEL DOWNLOAD SUCCESS: {condition_name}"))
    message(glue("      📈 {format(total_records, big.mark = ',')} total records retrieved"))
    message(glue("      ⏱️  Total time: {round(total_time, 1)} minutes"))
    
    filename <- file.path(output_dir, glue("{condition_name}_preventive_care_{min(years)}_{max(years)}_FULL_SCALE.csv.xz"))
    vroom::vroom_write(combined_data, filename)
    
    file_size_mb <- round(file.size(filename) / 1024^2, 2)
    message(glue("      💾 Saved to {basename(filename)} ({file_size_mb} MB)"))
    
    return(combined_data)
  }
  
  message(glue("   ❌ DOWNLOAD FAILED OR RETURNED NO DATA: {condition_name}"))
  return(NULL)
}
  
 
# =============================================================================
# RUN THE OPTIMIZED DOWNLOAD
# =============================================================================
# workers <- max(1, future::availableCores() - 1)
# message(glue("⚙️  Setting up parallel plan with {workers} workers..."))
# plan(multisession, workers = workers)
# 
# # Now, running this should work without the error
# diabetes_data <- full_scale_download_condition_optimized(
#   condition_code = "17",
#   condition_name = "hypertension",
#   years = c(2020:2023)
# )
# plan(sequential) # Clean up

all_conditions <- list(
  list(code = "83", name = "annual_wellness"),
  list(code = "85", name = "cardiovascular_disease"),
  list(code = "86", name = "colorectal_cancer"),
  list(code = "88", name = "depression"),
  list(code = "89", name = "diabetes"),
  list(code = "95", name = "influenza_vaccine"),
  list(code = "101", name = "pneumococcal_vaccine"),
  list(code = "102", name = "prostate_cancer"),
  list(code = "104", name = "sti"),
  list(code = "105", name = "mammogram"),
  list(code = "106", name = "pap_test"),
  list(code = "107", name = "pelvic_exam")
)


for(j in 1:length(all_conditions)){
  print(paste(j, '/', length(all_conditions)))
  print(all_conditions[[j]]$name)
  print(all_conditions[[j]]$code)
  
  #workers <- max(1, future::availableCores() - 1)
  workers <- 4 # Use fewer workers
  
  message(glue("⚙️  Setting up parallel plan with {workers} workers..."))
  plan(multisession, workers = workers)
  
  condition_data <- full_scale_download_condition_optimized(
    condition_code = all_conditions[[j]]$code, 
    condition_name = all_conditions[[j]]$name, 
    years = c(2020:2023)
  )
  
  plan(sequential) # Clean up
  
  # Wait between conditions (increase this if still getting blocked)
  wait_time <- 60 # 1 minute between conditions
  message(glue("⏳ Waiting {wait_time} seconds to respect rate limits..."))
  Sys.sleep(wait_time)
  
}
plan(sequential) # Clean up

##Combine together
files <- list.files(output_dir, full.names = TRUE)

df <- files %>%
  lapply(vroom::vroom) %>%
  bind_rows()

# write to parquet
arrow::write_parquet(df, "./raw/screening_combined.parquet")

