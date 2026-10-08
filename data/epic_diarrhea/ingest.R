# =============================================================================
# Cosmos Diarrhea Data Ingestion
# Source: https://github.com/PopHIVE/epic_preprocessing
#         tree/main/data/cosmos_diarrhea
# Pulls pre-processed standard files from the epic_preprocessing repository.
# Includes: all-cause diarrhea ED visits (weekly, by state and
# age), all-encounter (non-ED) diarrhea, and cyclospora lab test results.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/epic_preprocessing/main/data/cosmos_diarrhea"

# Standard files to download
standard_files <- c(
  "data_weekly.csv.gz",
  "weekly_tests.csv.gz"
)

# Download each standard file and track hashes for change detection
current_hashes <- list()

for (f in standard_files) {
  url <- paste0(base_url, "/standard/", f)
  # data_weekly.csv.gz is split below, so keep the upstream copy in raw/
  dest <- file.path(if (f == "data_weekly.csv.gz") "raw" else "standard", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", f, ": ", e$message)
  })
}

# Split data_weekly into emergency department (ED) and all-encounter files
weekly <- vroom::vroom("raw/data_weekly.csv.gz", show_col_types = FALSE, altrep = FALSE)
id_cols <- c("geography", "age", "time")
ed_cols <- grep("_ed_", names(weekly), value = TRUE)
vroom::vroom_write(
  weekly[, c(id_cols, ed_cols)],
  "standard/data_ed.csv.gz",
  delim = ","
)
vroom::vroom_write(
  weekly[, setdiff(names(weekly), ed_cols)],
  "standard/data_encounters.csv.gz",
  delim = ","
)
unlink("standard/data_weekly.csv.gz")

# Download measure_info.json, preserving the local `_catalog` block (drives
# the website data-sources index) across re-downloads: the upstream file
# doesn't carry it, so overwriting outright would erase it on every ingest run.
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

# Re-apply the discontinuation note (the download above overwrites local edits)
discontinued_note <- "NOTE: as of 10/08/2026, we are no longer updating this measure."
mi <- jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)
for (nm in setdiff(names(mi), c("age", "_sources", "_catalog"))) {
  if (!grepl(discontinued_note, mi[[nm]]$long_description, fixed = TRUE)) {
    mi[[nm]]$long_description <- paste(mi[[nm]]$long_description, discontinued_note)
  }
}
jsonlite::write_json(mi, "measure_info.json", auto_unbox = TRUE, pretty = TRUE)

# Update process record only if files have changed
if (!identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
