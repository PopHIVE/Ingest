# =============================================================================
# BLS Local Area Unemployment Statistics (LAUS) Data
# Source: https://github.com/PopHIVE/bureau-labor-statistics
# Pulls pre-processed standard files from the bureau-labor-statistics repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

base_url <- "https://raw.githubusercontent.com/PopHIVE/bureau-labor-statistics/main"

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
  for (nm in percent_measures) mi[[nm]][["scale"]] <- "0-100"
  jsonlite::write_json(mi, "measure_info.json", auto_unbox = TRUE, pretty = 2, null = "null")
  message("  measure_info.json: declared scale 0-100 on ", length(percent_measures), " measure(s)")
})
