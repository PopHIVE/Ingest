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

# -----------------------------------------------------------------------------
# Percent scale. PopHIVE's standard is 0-100: a percent measure stores 18.44,
# not 0.1844. The upstream source publishes its percent measures as 0-1
# proportions, so they are converted here, AFTER the fetch, because this project
# is a pass-through -- the standard files and measure_info.json above are
# re-downloaded and overwritten on every run, so a change made anywhere else in
# this project would be erased. Fixing it in the upstream repo would be the
# better home; this block can be deleted once that happens.
#
# Guarded so it is a no-op when the values are already on 0-100. That makes
# re-running safe (the download always restores the upstream 0-1 file) and makes
# the block self-disabling if the upstream source ever adopts the standard.
# The threshold is 2 rather than 1 so a legitimate value slightly above 1 in a
# 0-1 file cannot be misread as "already converted".
# -----------------------------------------------------------------------------
PERCENT_SCALE_CEILING <- 2

percent_measures <- local({
  if (!file.exists("measure_info.json")) return(character())
  mi <- jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)
  mi[["_sources"]] <- NULL
  is_pct <- vapply(mi, function(e) {
    identical(tolower(as.character(if (is.null(e[["measure_type"]])) "" else e[["measure_type"]])), "percent")
  }, logical(1))
  names(mi)[is_pct]
})

to_percent_scale_file <- function(path) {
  if (!file.exists(path) || !length(percent_measures)) return(invisible(NULL))
  df <- vroom::vroom(path, show_col_types = FALSE)
  cols <- intersect(names(df), percent_measures)
  if (!length(cols)) return(invisible(NULL))

  # Guarded PER COLUMN, not per file. A file can legitimately be mixed: CHR&R's
  # standard files carry retired measures alongside current ones, so a
  # file-level guard would see one converted column and skip every
  # unconverted one beside it.
  converted <- character()
  skipped   <- character()
  for (cl in cols) {
    x  <- suppressWarnings(as.numeric(df[[cl]]))
    mx <- suppressWarnings(max(x, na.rm = TRUE))
    if (!is.finite(mx)) next
    if (mx > PERCENT_SCALE_CEILING) {
      skipped <- c(skipped, cl)
    } else {
      df[[cl]] <- x * 100
      converted <- c(converted, cl)
    }
  }

  if (!length(converted)) {
    message("  ", basename(path), ": all ", length(skipped),
            " percent measure(s) already on 0-100 -- nothing to do")
    return(invisible(NULL))
  }
  message("  ", basename(path), ": converted ", length(converted),
          " percent measure(s) from 0-1 to 0-100",
          if (length(skipped)) paste0(" (", length(skipped), " already on 0-100)") else "")
  vroom::vroom_write(df, path, delim = ",")
}

for (f in Sys.glob(file.path("standard", "*.csv.gz"))) to_percent_scale_file(f)

# measure_info.json is overwritten on every run too, so the scale declaration
# has to be re-applied here rather than edited into the file.
local({
  if (!file.exists("measure_info.json") || !length(percent_measures)) return(invisible(NULL))
  mi <- jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)

  # Only measures the file ALREADY declares. percent_measures deliberately
  # includes column names that are not declared here -- CHR&R ships data for
  # measures it has retired -- and `mi[[nm]][["scale"]] <- ...` on a name that
  # does not exist CREATES it, which silently added 26 stub entries holding
  # nothing but a scale. The data conversion above needs those names; the
  # declared catalog must not gain them.
  targets <- intersect(percent_measures, names(mi))
  targets <- targets[vapply(targets, function(nm) is.list(mi[[nm]]), logical(1))]
  todo <- targets[vapply(targets, function(nm) {
    !identical(mi[[nm]][["scale"]], "0-100")
  }, logical(1))]

  # Rewrite only when something actually changes. This file is re-fetched on
  # every run, so an unconditional write churns it on every build and makes
  # every merge conflict.
  if (!length(todo)) {
    message("  measure_info.json: scale already declared on all ",
            length(targets), " percent measure(s)")
    return(invisible(NULL))
  }
  for (nm in todo) mi[[nm]][["scale"]] <- "0-100"
  jsonlite::write_json(mi, "measure_info.json", auto_unbox = TRUE, pretty = 2, null = "null")
  message("  measure_info.json: declared scale 0-100 on ", length(todo),
          " measure(s) (", length(targets) - length(todo), " already declared)")
})
