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

# -----------------------------------------------------------------------------
# Percent scale. PopHIVE's standard is 0-100: a percent measure stores 18.44,
# not 0.1844. CHR&R publishes its percent measures as 0-1 proportions, so they
# are converted here, after the fetch, because this project is a pass-through --
# the files above are re-downloaded and overwritten on every run, so a fix made
# in the upstream county_health_rankings repo would be the better home for this
# but a fix made anywhere else in this project would be erased.
#
# Guarded the same way as the infant-mortality correction above: if the values
# already exceed the 0-1 range, they are on 0-100 and this is a no-op. That
# makes the block self-disabling if CHR&R (or the upstream repo) ever adopts the
# standard, and makes re-running this ingest safe -- which matters, because the
# download always replaces the local file with the upstream 0-1 version.
#
# The threshold is 2 rather than 1: chr_high_school_graduation legitimately
# reaches 1.0075 in CHR&R's 2010 release, so "any value above 1" would
# misclassify a 0-1 file as already converted.
# -----------------------------------------------------------------------------
PERCENT_SCALE_CEILING <- 2

percent_measures <- local({
  if (!file.exists("measure_info.json")) return(character())
  mi <- jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)
  mi[["_sources"]] <- NULL
  is_pct <- vapply(mi, function(e) {
    identical(tolower(as.character(if (is.null(e[["measure_type"]])) "" else e[["measure_type"]])), "percent")
  }, logical(1))
  # CHR&R's measure_info.json declares only the measures it still publishes,
  # but its standard files retain columns for retired measures and for the
  # state-specific FL/NY/WI supplements, backfilled from older releases. Those
  # columns are percentages on the same 0-1 scale and must convert with their
  # siblings, or the file ends up half on each convention. Verified against the
  # data: all 26 max out between 0.105 and 1.0.
  retired <- c(
    "chr_access_to_healthy_foods", "chr_adequate_social_emotional_support_fl",
    "chr_adult_smoking_fl", "chr_adult_smoking_ny",
    "chr_adults_engaging_in_moderate_physical_activity_fl",
    "chr_adults_who_have_a_personal_doctor_fl", "chr_binge_drinking",
    "chr_binge_drinking_ny", "chr_college_degrees",
    "chr_could_not_see_doctor_due_to_cost",
    "chr_dental_visit_within_the_past_year_ny", "chr_diabetes_monitoring",
    "chr_excessive_drinking_fl", "chr_excessive_drinking_ny",
    "chr_fair_or_poor_health_ny", "chr_fast_food_restaurants",
    "chr_fruit_and_vegetable_consumption_fl", "chr_high_housing_costs",
    "chr_hospice_use", "chr_illiteracy", "chr_inadequate_social_support",
    "chr_insured_adults_fl", "chr_no_leisure_time_physical_activity_ny",
    "chr_obese_adults_ny", "chr_overweight_or_obese_adults_fl",
    "chr_single_parent_households"
  )
  union(names(mi)[is_pct], retired)
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

to_percent_scale_file(file.path("standard", "data_county.csv.gz"))
to_percent_scale_file(file.path("standard", "data_state.csv.gz"))

# The fetched measure_info.json is overwritten on every run too, so the scale
# declaration has to be re-applied here rather than edited into the file.
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
