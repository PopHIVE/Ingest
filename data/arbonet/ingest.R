# =============================================================================
# CDC ArboNET Historic Arboviral Disease Surveillance
# Source: https://github.com/PopHIVE/arbonet
# Pulls pre-processed standard files from the arbonet repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

base_url <- "https://raw.githubusercontent.com/PopHIVE/arbonet/main"

standard_files <- c(
  "data_state.csv.gz",
  "data_county.csv.gz"
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

# Preserve the local `_catalog` block (drives the website data-sources index)
# across re-downloads: the upstream measure_info.json doesn't carry it, so
# overwriting the file outright would erase it on every ingest run.
local_catalog <- NULL
if (file.exists("measure_info.json")) {
  local_catalog <- tryCatch({
    jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)[["_catalog"]]
  }, error = function(e) NULL)
}

tryCatch({
  download.file(
    paste0(base_url, "/measure_info.json"),
    "measure_info.json",
    mode = "wb",
    quiet = TRUE
  )
  if (!is.null(local_catalog)) {
    downloaded <- jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)
    downloaded[["_catalog"]] <- local_catalog
    jsonlite::write_json(downloaded, "measure_info.json", auto_unbox = TRUE, pretty = TRUE)
  }
}, error = function(e) {
  message("Warning: failed to download measure_info.json: ", e$message)
})

if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
