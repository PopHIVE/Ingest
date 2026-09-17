# =============================================================================
# Crisis Text Line: Crisis Trends
# Source: https://github.com/PopHIVE/crisis-text-line (private repo)
#
# Pulls the pre-processed, cell-suppressed standard file from the
# crisis-text-line repository. That repo is private (it holds the raw,
# unsuppressed conversation data upstream of this pull, which must never be
# exposed here), so this cannot use the plain `raw.githubusercontent.com`
# download that public pulled-in sources like arbonet use -- it authenticates
# via the GitHub API instead. Only `standard/data.csv.gz` and
# `measure_info.json` are pulled; the repo's `raw/` data and its
# QC-only `standard/data_unsuppressed.csv.gz` are intentionally never touched.
# =============================================================================

library(dplyr)
library(gh)

process <- dcf::dcf_process_record()

owner <- "PopHIVE"
repo  <- "crisis-text-line"
ref   <- "main"
token <- Sys.getenv("CRISIS_TEXT_LINE_TOKEN")

if (identical(token, "")) {
  stop(
    "crisis_text_line: CRISIS_TEXT_LINE_TOKEN is not set. The upstream repo ",
    "(PopHIVE/crisis-text-line) is private, so a GitHub PAT with read access ",
    "to it is required -- set it as a repo secret for GitHub Actions, and as ",
    "a local environment variable for manual runs."
  )
}

download_private_file <- function(path, dest) {
  gh::gh(
    "GET /repos/{owner}/{repo}/contents/{path}",
    owner = owner, repo = repo, path = path, ref = ref,
    .token = token,
    .send_headers = c(Accept = "application/vnd.github.raw"),
    .destfile = dest,
    .overwrite = TRUE
  )
  invisible(dest)
}

standard_files <- c("data.csv.gz")

current_hashes <- list()

for (f in standard_files) {
  dest <- file.path("standard", f)

  tryCatch({
    download_private_file(paste0("standard/", f), dest)
    current_hashes[[f]] <- unname(tools::md5sum(dest))
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
  download_private_file("measure_info.json", "measure_info.json")
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
