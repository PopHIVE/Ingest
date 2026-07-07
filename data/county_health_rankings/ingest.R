# =============================================================================
# County Health Rankings Data
# Source: https://github.com/PopHIVE/county_health_rankings
# Pulls pre-processed standard files from the county_health_rankings repository.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

base_url <- "https://raw.githubusercontent.com/PopHIVE/county_health_rankings/main"

standard_files <- c(
  "data_county.csv.gz",
  "data_state.csv.gz",
  "datapackage.json"
)

current_hashes <- list()

for (f in standard_files) {
  url <- paste0(base_url, "/standard/", f)
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
# The CHR 2013 release stores chr_infant_mortality at 100x the documented
# scale (numerator/denominator * 100,000 instead of CHR's own "per 1,000
# live births" definition -- see t_measure_years.csv measure_id 129, and
# us-rates/code/verify_chr_infant_mortality_2013.R /
# explore_v129_analytic_data2013.R for the full derivation). Guarded by a
# ratio check against 2014 so this becomes a no-op if CHR ever corrects the
# upstream file.
# -----------------------------------------------------------------------------
correct_2013_infant_mortality <- function(path) {
  if (!file.exists(path)) return(invisible(NULL))
  df <- vroom::vroom(path, show_col_types = FALSE)
  if (!"chr_infant_mortality" %in% names(df)) return(invisible(NULL))

  year <- format(as.Date(df$time), "%Y")
  is_2013 <- year == "2013"
  ratio <- median(df$chr_infant_mortality[is_2013], na.rm = TRUE) /
    median(df$chr_infant_mortality[year == "2014"], na.rm = TRUE)

  if (is.na(ratio) || ratio < 20) {
    message("  ", basename(path), ": 2013 infant mortality scale looks normal (ratio ",
            round(ratio, 1), ") -- skipping correction")
    return(invisible(NULL))
  }

  message("  ", basename(path), ": correcting 2013 chr_infant_mortality (100x scale bug, ratio ",
          round(ratio, 1), ")")
  df$chr_infant_mortality[is_2013] <- df$chr_infant_mortality[is_2013] / 100
  vroom::vroom_write(df, path, delim = ",")
}

correct_2013_infant_mortality(file.path("standard", "data_county.csv.gz"))
correct_2013_infant_mortality(file.path("standard", "data_state.csv.gz"))
