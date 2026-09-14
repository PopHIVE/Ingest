# =============================================================================
# Area Health Resource File (AHRF)
# Source: https://github.com/PopHIVE/area-health-resource-files
# Pulls the pre-standardized file from the area-health-resource-files
# repository, where the actual download from HRSA (1999-present editions)
# and transform happen.
# =============================================================================

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/area-health-resource-files/main"

# Download a file to a temp path first, only replacing dest on success so a
# failed fetch (e.g. upstream repo not yet pushed) can't wipe out good data.
fetch <- function(path, dest) {
  tmp <- paste0(dest, ".tmp")
  result <- tryCatch({
    download.file(paste0(base_url, "/", path), tmp, mode = "wb", quiet = TRUE)
    file.rename(tmp, dest)
    tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", path, ": ", e$message)
    unlink(tmp)
    NULL
  })
  result
}

current_hashes <- list(
  "data.csv.gz" = fetch("standard/data.csv.gz", "standard/data.csv.gz")
)
# Preserve the local `_catalog` block (drives the website data-sources index)
# across re-downloads: the upstream measure_info.json doesn't carry it, so
# overwriting the file outright would erase it on every ingest run.
local_catalog <- NULL
if (file.exists("measure_info.json")) {
  local_catalog <- tryCatch({
    jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)[["_catalog"]]
  }, error = function(e) NULL)
}

fetch("measure_info.json", "measure_info.json")

if (!is.null(local_catalog) && file.exists("measure_info.json")) {
  downloaded <- tryCatch(jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE), error = function(e) NULL)
  if (!is.null(downloaded)) {
    downloaded[["_catalog"]] <- local_catalog
    jsonlite::write_json(downloaded, "measure_info.json", auto_unbox = TRUE, pretty = TRUE)
  }
}

# Update process record only if the standard file was fetched and changed
if (!is.null(current_hashes[["data.csv.gz"]]) &&
    !identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
