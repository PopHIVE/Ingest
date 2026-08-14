# =============================================================================
# USDA Food Access Data
# Source: https://github.com/PopHIVE/usda-food-access
# Pulls pre-processed standard files from the usda-food-access repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

base_url <- "https://raw.githubusercontent.com/PopHIVE/usda-food-access/main"

standard_files <- c(
  "data_county.csv.gz"
)

# Download to a temp path first, only replacing dest on success, so a failed
# fetch (e.g. upstream repo not yet pushed) can't overwrite good data with a
# truncated/corrupt file.
fetch <- function(url, dest) {
  tmp <- paste0(dest, ".tmp")
  tryCatch({
    download.file(url, tmp, mode = "wb", quiet = TRUE)
    file.rename(tmp, dest)
    tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", url, ": ", e$message)
    unlink(tmp)
    NULL
  })
}

current_hashes <- list()

for (f in standard_files) {
  hash <- fetch(paste0(base_url, "/standard/", f), file.path("standard", f))
  if (!is.null(hash)) current_hashes[[f]] <- hash
}

fetch(paste0(base_url, "/measure_info.json"), "measure_info.json")

if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
