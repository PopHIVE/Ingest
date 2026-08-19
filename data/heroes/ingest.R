# =============================================================================
# HEROES Data
# Source: https://github.com/PopHIVE/heroes
# Pulls pre-processed standard files from the heroes repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

base_url <- "https://raw.githubusercontent.com/PopHIVE/heroes/main"

standard_files <- c(
  "data_maternal.csv.gz",
  "data_ascvd.csv.gz",
  "data_opioid.csv.gz"
)

# Check heroes' own published datapackage.json before downloading anything,
# so an unchanged upstream doesn't cost a re-download.
datapackage_tmp <- tempfile(fileext = ".json")
upstream_datapackage <- tryCatch({
  download.file(paste0(base_url, "/standard/datapackage.json"), datapackage_tmp, mode = "wb", quiet = TRUE)
  jsonlite::fromJSON(datapackage_tmp, simplifyVector = FALSE)
}, error = function(e) NULL)

upstream_state <- if (!is.null(upstream_datapackage)) {
  resources <- upstream_datapackage$resources
  stats::setNames(
    lapply(resources, function(r) r$md5),
    vapply(resources, function(r) r$filename, character(1))
  )[standard_files]
} else {
  NULL
}

if (!is.null(upstream_state) && identical(process$upstream_state, upstream_state)) {
  message("heroes: upstream standard/ unchanged since last pull -- skipping download.")
} else {
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

  if (!identical(process$raw_state, current_hashes) || !identical(process$upstream_state, upstream_state)) {
    process$raw_state <- current_hashes
    process$upstream_state <- upstream_state
    dcf::dcf_process_record(updated = process)
  }
}
