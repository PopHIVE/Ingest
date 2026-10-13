# =============================================================================
# NEISS (National Electronic Injury Surveillance System)
# Source: https://github.com/PopHIVE/neiss
# Pulls the pre-standardized files from the neiss repository, where the
# actual download from CPSC and transform happen.
# =============================================================================

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/neiss/main"

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

standard_files <- c(
  "data_infant_diagnosis.csv.gz",
  "data_infant_diagnosis_rate.csv.gz",
  "data_infant_product.csv.gz",
  "data_infant_product_rate.csv.gz",
  "data_agegroup_diagnosis.csv.gz",
  "data_agegroup_diagnosis_rate.csv.gz",
  "data_agegroup_product.csv.gz",
  "data_agegroup_product_rate.csv.gz"
)

current_hashes <- lapply(
  setNames(standard_files, standard_files),
  function(f) fetch(file.path("standard", f), file.path("standard", f))
)
fetch("measure_info.json", "measure_info.json")

# Update process record only if every standard file was fetched and any changed
if (!any(vapply(current_hashes, is.null, logical(1))) &&
    !identical(process$raw_state, current_hashes)) {
  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
