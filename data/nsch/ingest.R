# =============================================================================
# NSCH (National Survey of Children's Health)
# Source: https://github.com/PopHIVE/nsch
# Pulls the pre-standardized files from the nsch repository, where the
# download from the Census Bureau, the Title V coding and the survey
# estimation happen. That repository publishes them wide (one column per
# measure, plus _lcl/_ucl/_n/_suppressed/_unreliable/_not_asked companions);
# they are kept in raw/ and reshaped here to long: one row per stratum x
# measure, as for medicaid_quality.
# =============================================================================

suppressPackageStartupMessages(library(dplyr))
if (!exists("%||%")) `%||%` <- function(x, y) if (is.null(x)) y else x

process <- dcf::dcf_process_record()

# GitHub raw base URL
base_url <- "https://raw.githubusercontent.com/PopHIVE/nsch/main"

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
  "data_age.csv.gz",
  "data_age_sex.csv.gz",
  "data_age_race_ethnicity.csv.gz"
)

dir.create("raw", showWarnings = FALSE)
current_hashes <- c(
  lapply(setNames(standard_files, standard_files),
         function(f) fetch(file.path("standard", f), file.path("raw", f))),
  list(measure_info.json = fetch("measure_info.json", "raw/measure_info_upstream.json"))
)

# Companion columns that travel with each measure in the wide upstream files.
COMPANIONS <- c("_lcl", "_ucl", "_n", "_suppressed", "_unreliable", "_not_asked")
companion_re <- paste0("(", paste(COMPANIONS, collapse = "|"), ")$")

# -----------------------------------------------------------------------------
# Wide -> long. Each measure has three states in the upstream file: an
# estimate; suppressed (value NA, _suppressed = 1); or not asked / outside the
# measure's universe (value NA, _not_asked = 1). Not-asked cells get no row
# here; suppressed cells keep a row with value NA and suppressed = 1.
# -----------------------------------------------------------------------------
to_long <- function(file) {
  # Measure columns numeric, strata (geography, time, age, sex, ...) character.
  header <- names(vroom::vroom(file, n_max = 0, show_col_types = FALSE))
  wide <- vroom::vroom(file, show_col_types = FALSE, altrep = FALSE,
                       col_types = paste(if_else(startsWith(header, "nsch_"), "d", "c"),
                                         collapse = ""))
  measures <- grep("^nsch_", names(wide), value = TRUE)
  measures <- measures[!grepl(companion_re, measures)]
  strata <- names(wide)[!startsWith(names(wide), "nsch_")]
  missing <- setdiff(as.vector(outer(measures, COMPANIONS, paste0)), names(wide))
  if (length(missing)) stop("companion columns missing from ", file, ": ",
                            paste(head(missing), collapse = ", "))

  long <- bind_rows(lapply(measures, function(m) {
    tibble(
      wide[strata],
      measure     = m,
      value       = wide[[m]],
      value_lcl   = wide[[paste0(m, "_lcl")]],
      value_ucl   = wide[[paste0(m, "_ucl")]],
      sample_size = wide[[paste0(m, "_n")]],
      suppressed  = wide[[paste0(m, "_suppressed")]],
      unreliable  = wide[[paste0(m, "_unreliable")]],
      not_asked   = wide[[paste0(m, "_not_asked")]]
    )
  }))
  # Every cell must be exactly one of: estimate, suppressed, not asked.
  state <- (!is.na(long$value)) + (long$suppressed %in% 1) + (long$not_asked %in% 1)
  if (any(state != 1)) stop(sum(state != 1), " cells in ", file,
                            " are not exactly one of estimate / suppressed / not asked")
  long %>%
    filter(!(not_asked %in% 1)) %>%
    select(-not_asked) %>%
    relabel_age(strata) %>%
    arrange(across(all_of(strata)), measure)
}

# -----------------------------------------------------------------------------
# Age labels. Upstream reports bands 0-5 / 6-11 / 12-17 plus 'Overall' for
# every measure, but a measure only covers its own age range, named by the
# _<lo>_<hi> suffix (_6mo_5 = 6 months-5 years; no suffix = 0-17). So
# 'Overall' is really that range and a band is really its overlap with it
# (e.g. nsch_pct_adhd_3_17: 'Overall' -> '3-17', '0-5' -> '3-5'). Where the
# range sits inside one band (_12_17, _6_11, _6mo_5), 'Overall' and the band
# become the same label; they must then be identical rows, and one is kept.
# -----------------------------------------------------------------------------
relabel_age <- function(long, strata) {
  re <- "^.*_(\\d+|6mo)_(\\d+)$"
  has <- grepl(re, long$measure)
  u_lo_raw <- if_else(has, sub(re, "\\1", long$measure), "0")
  # if_else() evaluates both branches, so non-matching names coerce to NA there
  u_lo <- suppressWarnings(if_else(u_lo_raw == "6mo", 0.5, as.numeric(u_lo_raw)))
  u_hi <- suppressWarnings(if_else(has, as.numeric(sub(re, "\\2", long$measure)), 17))
  src <- if_else(long$age == "Overall", "0-17", long$age)
  if (!all(grepl("^\\d+-\\d+$", src))) stop("unexpected age levels: ",
                                            paste(unique(src[!grepl("^\\d+-\\d+$", src)]), collapse = ", "))
  lo <- pmax(as.numeric(sub("-.*", "", src)), u_lo)
  hi <- pmin(as.numeric(sub(".*-", "", src)), u_hi)
  if (any(lo > hi)) stop("an age band lies outside its measure's age range")
  out <- long %>%
    mutate(age = paste0(if_else(lo == 0.5, "6mo", as.character(lo)), "-", hi)) %>%
    distinct()
  if (anyDuplicated(out[c(strata, "measure")])) {
    stop("'Overall' differs from the single band it should equal")
  }
  out
}

# -----------------------------------------------------------------------------
# measure_info.json, regenerated from the upstream copy on every run so a
# refresh upstream cannot undo the long-format entries. The per-measure
# definitions are kept (they are what `measure` levels point to); the
# companion entries are replaced by one entry per long-format column.
# -----------------------------------------------------------------------------
long_entry <- function(id, short_name, short_description, measure_type, unit) {
  list(id = id, short_name = short_name, long_name = paste0("NSCH: ", short_name),
       category = "behavioral_health", short_description = short_description,
       measure_type = measure_type, unit = unit, time_resolution = "Year",
       sources = list(list(id = "nsch")))
}

build_measure_info <- function(upstream) {
  mi <- jsonlite::read_json(upstream)
  keys <- names(mi)
  measure_keys <- keys[startsWith(keys, "nsch_") & !grepl(companion_re, keys)]
  types <- unique(vapply(mi[measure_keys], function(x) x$unit %||% "", ""))
  if (!identical(types, "Percent")) stop("NSCH measures are not all Percent: ", paste(types, collapse = ", "))

  # Upstream says "Rows with age = 'Overall' cover the full measure universe
  # (...)". relabel_age() replaces 'Overall' with the range itself.
  overall_re <- "Rows with age = 'Overall' cover the full measure universe \\((.*)\\)\\."
  for (k in measure_keys) {
    d <- mi[[k]]$long_description
    if (!grepl(overall_re, d)) stop("unexpected long_description wording for ", k)
    mi[[k]]$long_description <- sub(
      overall_re, "The row whose age is the full range covers all \\1; age bands are cut to that range.", d)
  }

  long_cols <- list(
    age = long_entry(
      "age", "Age group",
      paste("Child's age in years, limited to the measure's own age range (named in the measure, e.g. _3_17).",
            "Each measure has a row for its full range (e.g. '3-17'; '0-17' for measures with no age suffix)",
            "plus the bands 0-5, 6-11 and 12-17 cut to that range (e.g. '3-5'). '6mo-5' means 6 months to 5 years.",
            "A measure that fits inside one band (e.g. _12_17) has only that band."),
      "Category", "Age group"),
    measure = c(long_entry(
      "measure", "NSCH measure",
      "Which NSCH measure the row reports. Each value is defined in the table of values below.",
      "Category", "Measure identifier"),
      list(levels = setNames(lapply(measure_keys, function(k) setNames(list(), character())),
                             measure_keys))),
    value = long_entry(
      "value", "Estimate",
      "Weighted percent of children in the measure's age range, for this state, year and group. NA when suppressed.",
      "Percent", "Percent"),
    value_lcl = long_entry(
      "value_lcl", "Lower 95% CI",
      "Lower bound of the 95% confidence interval for value, from the survey design, on the logit scale. NA when value is NA, 0 or 100.",
      "Percent", "Percent"),
    value_ucl = long_entry(
      "value_ucl", "Upper 95% CI",
      "Upper bound of the 95% confidence interval for value, from the survey design, on the logit scale. NA when value is NA, 0 or 100.",
      "Percent", "Percent"),
    sample_size = long_entry(
      "sample_size", "Sample size",
      "Unweighted number of surveyed children in the denominator of value.",
      "Count", "Count"),
    suppressed = long_entry(
      "suppressed", "Suppressed flag",
      "1 if fewer than 30 children were surveyed, so value and its interval are NA (not imputed); 0 otherwise. Ages and years a measure does not cover have no row.",
      "Indicator", "0/1 flag"),
    unreliable = long_entry(
      "unreliable", "Unreliable flag",
      "1 if the 95% confidence interval is wider than 20 percentage points or 1.2 times the estimate, or the estimate is 0 or 100; NA when suppressed; 0 otherwise.",
      "Indicator", "0/1 flag")
  )
  other <- keys[!startsWith(keys, "nsch_")]
  c(long_cols, mi[measure_keys], mi[other])
}

# Reshape only when every upstream file arrived and either the upstream data or
# the transformation here changed. Bump FORMAT_VERSION whenever to_long(),
# relabel_age() or build_measure_info() changes, so existing output is redone.
FORMAT_VERSION <- "long-2"
fetched_all <- !any(vapply(current_hashes, is.null, logical(1)))

if (fetched_all && (!identical(process$format_version, FORMAT_VERSION) ||
                    !identical(process$raw_state, current_hashes))) {
  for (f in standard_files) {
    vroom::vroom_write(to_long(file.path("raw", f)), file.path("standard", f), delim = ",")
  }
  jsonlite::write_json(build_measure_info("raw/measure_info_upstream.json"), "measure_info.json",
                       auto_unbox = TRUE, pretty = TRUE, digits = NA)
  process$raw_state <- current_hashes
  process$format_version <- FORMAT_VERSION
  dcf::dcf_process_record(updated = process)
}
