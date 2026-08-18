# =============================================================================
# HUD CHAS Data
# Source: https://github.com/PopHIVE/hud-chas
# Pulls pre-processed standard files from the hud-chas repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

base_url <- "https://raw.githubusercontent.com/PopHIVE/hud-chas/main"

standard_files <- c(
  "data_county.csv.gz",
  "data_state.csv.gz"
)

current_hashes <- list()

for (f in standard_files) {
  url  <- paste0(base_url, "/standard/", f)
  dest <- file.path("standard", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", f, ": ", e$message)
  })
}

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

if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
