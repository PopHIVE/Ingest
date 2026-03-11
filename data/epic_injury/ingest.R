# =============================================================================
# Cosmos Injury Data Ingestion
# Source: https://github.com/PopHIVE/epic_preprocessing
#         tree/main/data/cosmos_injury
# Pulls pre-processed standard files from the epic_preprocessing repository.
# Includes: ED visits for opioid overdose, firearm, and heat-related injuries
# at state level (monthly and yearly) and county level (heat, annual).
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/cosmos_injury"

# Standard files to download
standard_files <- c(
  "monthly_injury.csv.gz",
  "yearly_injury.csv.gz",
  "heat_year_county.csv.gz"
)

# Download each standard file and track hashes for change detection
current_hashes <- list()

for (f in standard_files) {
  url <- paste0(base_url, "/standard/", f)
  dest <- file.path("standard", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", f, ": ", e$message)
  })
}

# Download measure_info.json
tryCatch({
  download.file(
    paste0(base_url, "/measure_info.json"),
    "measure_info.json",
    mode = "wb",
    quiet = TRUE
  )
}, error = function(e) {
  message("Warning: failed to download measure_info.json: ", e$message)
})

# Update process record only if files have changed
if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
