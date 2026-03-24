# =============================================================================
# Cosmos Respiratory Infections Data Ingestion
# Source: https://github.com/PopHIVE/epic_preprocessing
#         tree/main/data/cosmos_resp_infections
# Pulls pre-processed standard files from the epic_preprocessing repository.
# Includes: Weekly ED visits for COVID, influenza, RSV (state and national,
# by age) and monthly RSV test positivity among pneumonia admissions.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/cosmos_resp_infections"

# Standard files to download
standard_files <- c(
  "weekly.csv.gz",
  "monthly_tests.csv.gz",
  "no_geo.csv.gz"
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
