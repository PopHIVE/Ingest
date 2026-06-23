# =============================================================================
# Cosmos Respiratory Infections Data Ingestion
# Source: https://github.com/PopHIVE/epic_preprocessing
#         tree/main/data/cosmos_resp_infections
#         tree/main/data/cosmos_gas
# Pulls pre-processed standard files from the epic_preprocessing repository.
# Includes: Weekly ED visits for COVID, influenza, RSV (state and national,
# by age), monthly RSV test positivity among pneumonia admissions, and
# quarterly Group A Streptococcus (GAS) ED encounters.
# =============================================================================

library(dplyr)
library(jsonlite)

process <- dcf::dcf_process_record()

# GitHub raw base URLs
base_url     <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/cosmos_resp_infections"
gas_base_url <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/cosmos_gas"

# Standard files to download from cosmos_resp_infections
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

# Download quarterly GAS (Group A Streptococcus) standard file from cosmos_gas
tryCatch({
  download.file(
    paste0(gas_base_url, "/standard/data.csv.gz"),
    file.path("standard", "quarterly_gas.csv.gz"),
    mode = "wb", quiet = TRUE
  )
  current_hashes[["quarterly_gas.csv.gz"]] <- tools::md5sum(
    file.path("standard", "quarterly_gas.csv.gz")
  )
}, error = function(e) {
  message("Warning: failed to download quarterly_gas.csv.gz: ", e$message)
})

# Download and merge measure_info.json from both sources
tryCatch({
  tmp_resp <- tempfile(fileext = ".json")
  tmp_gas  <- tempfile(fileext = ".json")
  download.file(paste0(base_url,     "/measure_info.json"), tmp_resp, mode = "wb", quiet = TRUE)
  download.file(paste0(gas_base_url, "/measure_info.json"), tmp_gas,  mode = "wb", quiet = TRUE)

  mi_resp <- jsonlite::read_json(tmp_resp)
  mi_gas  <- jsonlite::read_json(tmp_gas)

  # Add entries from cosmos_gas that are not already present (skip _sources)
  new_keys <- setdiff(names(mi_gas), c(names(mi_resp), "_sources"))
  for (k in new_keys) {
    mi_resp[[k]] <- mi_gas[[k]]
  }

  jsonlite::write_json(mi_resp, "measure_info.json", auto_unbox = TRUE, pretty = TRUE)
  unlink(c(tmp_resp, tmp_gas))
}, error = function(e) {
  message("Warning: failed to merge measure_info.json: ", e$message)
})

# Update process record only if files have changed
if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
