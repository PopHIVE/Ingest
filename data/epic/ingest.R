# =============================================================================
# Epic Cosmos Data Ingestion
# Source: https://github.com/PopHIVE/epic_preprocessing
# Pulls pre-processed standard files from the epic_preprocessing repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL for standard files
base_url <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/bundle_cosmos/standard"

# Files to download from the remote standard folder
standard_files <- c(
  "county_no_time.csv.gz",
  "county_year.csv.gz",
  "monthly.csv.gz",
  "monthly_injury.csv.gz",
  "monthly_tests.csv.gz",
  "no_geo.csv.gz",
  "state_no_time.csv.gz",
  "state_year.csv.gz",
  "weekly.csv.gz",
  "yearly_injury.csv.gz"
)

# Download each file and track hashes for change detection
current_hashes <- list()
any_changed <- FALSE

for (f in standard_files) {
  url <- paste0(base_url, "/", f)
  dest <- file.path("standard", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", f, ": ", e$message)
  })
}

# Also download the measure_info.json
measure_url <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/bundle_cosmos/measure_info.json"
tryCatch({
  download.file(measure_url, "measure_info.json", mode = "wb", quiet = TRUE)
}, error = function(e) {
  message("Warning: failed to download measure_info.json: ", e$message)
})

# Check if anything changed
if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
