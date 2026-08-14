# =============================================================================
# CDC VSRR Provisional Maternal Death Counts and Rates
# Source: https://github.com/PopHIVE/cdc_vssr
# Pulls the pre-standardized file from the cdc_vssr repository, where the
# actual download from CDC (dataset e2d5-ggg7) and transform happen.
# =============================================================================

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/cdc_vssr/main"

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
fetch("measure_info.json", "measure_info.json")

# Update process record only if the standard file was fetched and changed
if (!is.null(current_hashes[["data.csv.gz"]]) &&
    !identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
