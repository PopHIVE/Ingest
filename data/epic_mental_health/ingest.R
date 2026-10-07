# =============================================================================
# Cosmos Mental Health ED Data Ingestion
# Source: https://github.com/PopHIVE/epic_preprocessing/tree/add-MH/data/cosmos_mental_health
# Pulls pre-processed standard files from the epic_preprocessing repository.
# NOTE: points at the `add-MH` branch until it is merged; switch `branch`
# to "main" afterwards.
# Includes: monthly ED length of stay and mental health diagnosis measures by
# state and age band.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL
branch <- "add-MH"
base_url <- paste0(
  "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/", branch,
  "/data/cosmos_mental_health"
)

# Standard files to download
standard_files <- c("data.csv.gz")

# Download each standard file and track hashes for change detection
current_hashes <- list()

for (f in standard_files) {
  url <- paste0(base_url, "/standard/", f)
  dest <- file.path("standard", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- unname(tools::md5sum(dest))
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
