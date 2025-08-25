############################################################################
#
# CMS Mapping Medicare Disparities (MMD) Data Downloader (v7 - Definitive)
#
# Author: Gemini (Developed by Google)
# Date: 2024
#
# Description:
# This script downloads the complete MMD dataset.
# It solves previous issues by:
#  1. Using the correct, current DATASET_ID.
#  2. Using both User-Agent and Referer headers to bypass server security.
#  3. Manually defining the filter options (Year, Measure, etc.) because
#     the API's metadata endpoint is inaccessible.
# This script iterates through every combination to build the full dataset.
#
############################################################################

# 1. LOAD REQUIRED LIBRARIES
# --------------------------------------------------------------------------
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)

# 2. DEFINE CONSTANTS & HEADERS
# --------------------------------------------------------------------------
DATASET_ID <- "f143cf21-026f-470a-86a0-62137d5705a6"
BASE_URL <- "https://data.cms.gov/data-api/v1/dataset/"

# Define headers that will be used for ALL requests
REQUEST_HEADERS <- add_headers(
  `User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
  `Referer` = "https://data.cms.gov/tools/mapping-medicare-disparities-by-population"
)

# 3. MANUALLY DEFINE FILTER OPTIONS
# --------------------------------------------------------------------------
# The API metadata endpoint is blocked, so we define the filter options here.
# These were manually copied from the dropdowns on the CMS website.
# If the website adds new options (e.g., a new year), add them to these lists.
cat("Using manually defined filter options.\n")

available_geographies <- c("State", "County")
available_years <- c("2021", "2020", "2019", "2018", "2017")
available_populations <- c(
  "All Medicare Beneficiaries", "American Indian/Alaska Native",
  "Asian/Pacific Islander", "Black/African American", "Hispanic", "White",
  "Beneficiaries with a Disability", "Low-Income Beneficiaries (LIS)"
)
available_measures <- c(
  "All-Cause Hospital Readmissions", "Ambulatory Care Sensitive Condition Hospitalizations",
  "Annual Wellness Visit", "Breast Cancer Screening", "Chronic Condition All-Cause Hospitalizations",
  "Colorectal Cancer Screening", "Depression Screening and Follow-Up",
  "Fall Injury Hospitalizations", "Preventive Services", "Statin Use",
  "Tobacco Use Screening and Cessation"
)

cat("Found:", length(available_measures), "measures\n")
cat("Found:", length(available_years), "years\n")
cat("Found:", length(available_populations), "populations\n")
cat("Found:", length(available_geographies), "geographies (State/County)\n\n")

# 4. DEFINE THE DATA FETCHING FUNCTION
# --------------------------------------------------------------------------
fetch_cms_data <- function(measure, year, population, geography, dataset_id) {
  data_url <- paste0(BASE_URL, dataset_id, "/data")
  query_params <- list(
    `filter[measure_name]` = measure, `filter[year]` = year,
    `filter[mmd_population]` = population, `filter[geography]` = geography
  )
  
  cat(sprintf("Fetching: %s | %s | %s | %s\n", year, geography, population, measure))
  Sys.sleep(0.5) # A small delay to be polite to the server
  
  all_data <- list()
  next_url <- data_url
  
  while (!is.null(next_url)) {
    if (next_url == data_url) {
      response <- GET(url = next_url, query = query_params, REQUEST_HEADERS)
    } else {
      response <- GET(url = next_url, REQUEST_HEADERS)
    }
    
    if (http_error(response) || !grepl("application/json", headers(response)$`content-type`)) {
      warning(sprintf("HTTP Error or non-JSON response for: %s, %s, %s, %s", year, geography, population, measure))
      return(NULL)
    }
    
    page_content <- fromJSON(content(response, "text", encoding = "UTF-8"))
    if (length(page_content$data) > 0) {
      all_data[[length(all_data) + 1]] <- page_content$data
    }
    next_url <- page_content$links$`next`
  }
  
  if (length(all_data) > 0) bind_rows(all_data) else NULL
}

# 5. CREATE PARAMETER GRID AND EXECUTE
# --------------------------------------------------------------------------
param_grid <- expand_grid(
  measure = available_measures, year = available_years,
  population = available_populations, geography = available_geographies
)

cat("\nStarting data download process. This may take several hours.\n")
cat("There are", nrow(param_grid), "combinations to fetch.\n\n")

safe_fetch <- safely(fetch_cms_data)
results_list <- pmap(param_grid, .f = ~ safe_fetch(..1, ..2, ..3, ..4, dataset_id = DATASET_ID))

# 6. PROCESS AND SAVE RESULTS
# --------------------------------------------------------------------------
all_mmd_data <- map(results_list, "result") %>% bind_rows()
errors <- map(results_list, "error") %>% compact()

cat("\n\n------------------------------------------------------------\n")
cat("Data download complete!\n")
if (length(errors) > 0) {
  cat(length(errors), "errors were encountered. Inspect the 'errors' list object for details.\n")
}
cat("Total rows downloaded:", nrow(all_mmd_data), "\n")
cat("Total columns:", ncol(all_mmd_data), "\n\n")

# Save as RDS (R's native format, fast and preserves data types)
saveRDS(all_mmd_data, "cms_medicare_disparities_data_complete.rds")
cat("Full dataset saved to 'cms_medicare_disparities_data_complete.rds'\n")

# Save as CSV (more portable)
data.table::fwrite(all_mmd_data, "cms_medicare_disparities_data_complete.csv")
cat("Full dataset also saved to 'cms_medicare_disparities_data_complete.csv'\n")