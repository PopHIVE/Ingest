# =============================================================================
# Fail a workflow run when a source or bundle did not process
#
# dcf_build() and dcf_process() return normally when an ingest.R or build.R
# errors, so a scheduled run stays green while a source is broken. This script
# is the last step of the data workflows: it exits with status 1 if anything
# failed, after the data that did build has been committed.
#
# Usage (from the repo root):
#   Rscript scripts/check_build_status.R [log file ...] [--allow=name1,name2]
#
# With log files, a source or bundle counts as failed when a log has a
# "✖ processing source|bundle <name>" line for it, so only what ran in that
# workflow is judged. Without log files (e.g. run by hand), every
# data/<name>/process.json is checked for success = false instead.
# =============================================================================

# Sources that are expected to fail everywhere (no standard files yet)
KNOWN_FAILURES <- c("atlas_amr")

args      <- commandArgs(trailingOnly = TRUE)
allow_arg <- grep("^--allow=", args, value = TRUE)
allowed   <- c(KNOWN_FAILURES, unlist(strsplit(sub("^--allow=", "", allow_arg), ",")))
log_files <- setdiff(args, allow_arg)

# 1. Failures printed in the run logs
log_failed <- character()
for (f in log_files[file.exists(log_files)]) {
  # Match the cross mark by its bytes so this also works in the C locale
  lines <- readLines(f, warn = FALSE)
  hits  <- regmatches(lines, regexpr("\xe2\x9c\x96 processing (source|bundle) [^ ]+",
                                     lines, useBytes = TRUE))
  log_failed <- c(log_failed, sub("^.* ", "", hits))
}

# 2. Failures recorded in process.json (only when no log was given)
record_failed <- character()
if (length(log_files) == 0) {
  for (f in Sys.glob("data/*/process.json")) {
    p  <- jsonlite::read_json(f)
    ok <- vapply(p$scripts, function(s) !isFALSE(s$last_status$success), logical(1))
    if (!all(ok)) record_failed <- c(record_failed, basename(dirname(f)))
  }
}

failed <- sort(setdiff(unique(c(log_failed, record_failed)), allowed))
if (length(failed) > 0) {
  message("Failed to process: ", paste(failed, collapse = ", "))
  for (name in failed) {
    message("::error::", name, " failed to process",
            if (name %in% log_failed) " in this run" else " (recorded in process.json)")
  }
  quit(status = 1)
}
message("All sources and bundles processed.")
