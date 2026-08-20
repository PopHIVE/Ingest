# =============================================================================
# Build Data Table
# Generates docs/data-table.html: an auto-updating overview table with one row
# per data source (Datasets tab) and one row per bundle (Bundles tab).
#
# Columns are derived automatically from the repository:
#   - docs/data_sources_index.json (the curated per-dataset "summary", reused
#     verbatim as the Brief Description column)
#   - measure_info.json "_sources" (title, organization, url, restrictions,
#     time_resolution, and a fallback description)
#   - standard/*.csv.gz data files (spatial / age / sex / other resolutions,
#     earliest & latest observation dates)
#   - git commit history of standard/*.csv.gz (Last Refreshed: when our
#     pipeline last processed this source -- can move even without new
#     upstream data, e.g. a script fix)
#   - raw/<id>.json metadata sidecars (Latest Issue: the publisher's own
#     `rowsUpdatedAt` timestamp, i.e. when the SOURCE says it last updated the
#     data. Sources with no such timestamp fall back to the newest commit that
#     changed a raw data file, marked "est." in the table.)
#
# Because those two dates mean different things -- one is the publisher's claim,
# the other is our processing cadence -- the page carries a "How to read this
# table" note defining them, and any dataset whose Latest Issue is more than
# STALE_THRESHOLD_DAYS after its Last Refreshed is flagged (in the table and in
# the build log): the source has published data that our standardized output has
# not caught up with.
#
# Run from the repository root (same as scripts/build_docs.R):
#   Rscript scripts/build_data_table.R
# =============================================================================

suppressMessages({
  library(jsonlite)
  library(vroom)
  library(htmltools)
})

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) return(y)
  if (is.character(x) && (is.na(x[1]) || !nzchar(x[1]))) return(y)
  x
}

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Columns (as they appear in the standard files) that count toward each
# stratification. Detection is presence-based, per the data-table spec:
# a source is "Stratified" on a dimension if ANY of its standard files carries
# the corresponding column.
AGE_COLS <- c("age", "age_group", "agec")
SEX_COLS <- c("sex")

# "Other" stratifying demographics -> display label. Extend as new sources add
# demographic breakdowns. (Measure dimensions such as virus/serotype/vaccine are
# intentionally excluded -- this column is for demographic strata only.)
OTHER_STRAT <- c(
  race_ethnicity   = "Race/Ethnicity",
  race             = "Race/Ethnicity",
  ethnicity        = "Race/Ethnicity",
  education        = "Education",
  education_level  = "Education",
  educ             = "Education",
  urban            = "Urbanicity",
  urbanicity       = "Urbanicity",
  rural_urban      = "Urbanicity",
  metro            = "Urbanicity",
  insurance        = "Insurance",
  insurance_status = "Insurance",
  payer            = "Payer",
  income           = "Income",
  income_level     = "Income",
  poverty          = "Poverty",
  disability       = "Disability",
  disability_status= "Disability",
  grade            = "School Grade",
  wapo_school_grade= "School Grade",
  birth_year       = "Birth Cohort"
)

# Candidate time columns, in preference order.
TIME_COLS <- c("time", "date", "week_end", "year")

# Base URL for the source folders on GitHub (for the "Data URL" column).
GITHUB_DATA_BASE <- "https://github.com/PopHIVE/Ingest/tree/main/data"

# Flag a dataset when its Latest Issue (newest raw/ commit) is more than this
# many days after its Last Refreshed (newest standard/ commit) -- i.e. upstream
# published new data that our standardized output has not picked up.
STALE_THRESHOLD_DAYS <- 7

# Section anchor id for a folder on the data-dictionary page (docs/index.html).
# Must match build_docs.R: gsub("[^a-zA-Z0-9]", "-", name).
doc_section_id <- function(name) gsub("[^a-zA-Z0-9]", "-", name)

# -----------------------------------------------------------------------------
# Display-name helpers (kept consistent with scripts/build_docs.R)
# -----------------------------------------------------------------------------
format_source_name <- function(name) {
  name <- gsub("_", " ", name)
  name <- tools::toTitleCase(name)
  repl <- c("Cdc"="CDC","Jhu"="JHU","Mmr"="MMR","Cms"="CMS","Nssp"="NSSP",
            "Nis"="NIS","Nrevss"="NREVSS","Nchs"="NCHS","Brfss"="BRFSS",
            "Vaers"="VAERS","Amr"="AMR","Ili"="ILI","Nhsn"="NHSN","Nnds"="NNDS",
            "Yrbss"="YRBSS","Ahrf"="AHRF","Acs"="ACS","Narms"="NARMS",
            "Nhtsa"="NHTSA","Wisqars"="WISQARS","Nccr"="NCCR","Chr"="CHR")
  for (k in names(repl)) name <- gsub(paste0("\\b", k, "\\b"), repl[[k]], name)
  name
}

format_bundle_name <- function(name) {
  display <- sub("^bundle_", "", name)
  display <- gsub("_", " ", display)
  tools::toTitleCase(display)
}

# Short one-sentence summary of a (possibly long) description.
short_summary <- function(x, max_chars = 300) {
  x <- x %||% ""
  x <- trimws(gsub("\\s+", " ", x))
  if (!nzchar(x)) return("")
  first <- strsplit(x, "(?<=[.!?])\\s+", perl = TRUE)[[1]][1]
  if (is.na(first)) first <- x
  if (nchar(first) > max_chars) first <- paste0(substr(first, 1, max_chars - 1), "…")
  first
}

# -----------------------------------------------------------------------------
# Curated metadata (docs/data_sources_index.json)
# -----------------------------------------------------------------------------
# The Brief Description and Search Terms columns reuse the hand-written
# `summary` and `search_terms` each dataset carries in
# docs/data_sources_index.json, so this table and the website data page never
# drift apart. build_docs.R writes that file and runs before this script in
# both CI workflows, so the values read here are always current.
load_index_entries <- function(path = "docs/data_sources_index.json") {
  if (!file.exists(path)) {
    cat(sprintf("NOTE: %s not found -- deriving brief descriptions, no search terms\n", path))
    return(list())
  }
  idx <- tryCatch(fromJSON(path, simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(idx) || is.null(idx$datasets)) {
    cat(sprintf("NOTE: %s has no `datasets` array -- deriving brief descriptions instead\n", path))
    return(list())
  }
  out <- list()
  for (d in idx$datasets) {
    if (is.null(d$dataset)) next
    terms <- unlist(d$search_terms)
    out[[d$dataset]] <- list(
      summary      = trimws(d$summary %||% ""),
      search_terms = if (is.null(terms)) character(0) else trimws(as.character(terms))
    )
  }
  out
}

INDEX_ENTRIES <- load_index_entries()

# -----------------------------------------------------------------------------
# Data-file inspection helpers
# -----------------------------------------------------------------------------

get_standard_files <- function(source_dir) {
  standard_dir <- file.path(source_dir, "standard")
  if (!dir.exists(standard_dir)) return(character(0))
  list.files(standard_dir, pattern = "\\.csv\\.gz$", full.names = TRUE)
}

get_columns <- function(filepath) {
  tryCatch(names(vroom::vroom(filepath, n_max = 0, show_col_types = FALSE)),
           error = function(e) character(0))
}

# Classify the geography levels present in one file. A file with no geography
# column represents a national aggregate (e.g. CDC national counts by age), so
# it is treated as "National".
GEO_COLS <- c("geography", "fips")
geo_levels <- function(filepath, cols) {
  gcol <- GEO_COLS[GEO_COLS %in% cols][1]
  if (is.na(gcol)) return("National")
  vals <- tryCatch(
    vroom::vroom(filepath, col_select = tidyselect::all_of(gcol),
                 col_types = cols(.default = col_character()),
                 show_col_types = FALSE)[[1]],
    error = function(e) NULL)
  if (is.null(vals)) return(character(0))
  vals <- trimws(vals[!is.na(vals) & nzchar(trimws(vals))])
  if (!length(vals)) return(character(0))
  vals <- unique(vals)
  levs <- character(0)
  is_nat <- vals %in% c("00", "0", "US", "USA", "United States")
  if (any(is_nat)) levs <- c(levs, "National")
  nc <- nchar(vals)
  if (any(!is_nat & nc <= 2)) levs <- c(levs, "State")
  if (any(nc >= 4 & nc <= 5)) levs <- c(levs, "County")
  levs
}

# Safe, format-aware date parser: never errors, returns a Date vector.
# Handles ISO (YYYY-MM-DD), US month-first (MM-DD-YYYY or MM/DD/YYYY) and
# year-only (YYYY) values, then drops implausible years (parse artifacts).
parse_dates <- function(vals) {
  vals <- trimws(vals[!is.na(vals)])
  vals <- vals[nzchar(vals)]
  if (!length(vals)) return(as.Date(character(0)))
  # Year-only values (e.g. "2024") -> Dec 31 of that year.
  if (all(grepl("^[0-9]{4}$", vals))) return(as.Date(paste0(vals, "-12-31")))

  d <- rep(as.Date(NA), length(vals))
  iso <- grepl("^[0-9]{4}[-/]", vals)                        # 2024-01-31
  mdy <- grepl("^[0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{4}$", vals)  # 01-31-2024
  if (any(iso)) d[iso] <- as.Date(gsub("/", "-", vals[iso]), format = "%Y-%m-%d")
  if (any(mdy)) d[mdy] <- as.Date(gsub("/", "-", vals[mdy]), format = "%m-%d-%Y")
  rest <- is.na(d) & !iso & !mdy
  if (any(rest)) {
    d[rest] <- suppressWarnings(tryCatch(as.Date(vals[rest]),
                                         error = function(e) as.Date(NA)))
  }

  d <- d[!is.na(d)]
  if (!length(d)) return(as.Date(character(0)))
  yr <- as.integer(format(d, "%Y"))
  cur <- as.integer(format(Sys.Date(), "%Y"))
  d[yr >= 1980 & yr <= cur + 1]
}

# TRUE if any of `candidate_cols` is present AND carries a value other than the
# non-stratifying placeholders ("Overall", "All", "Total", ...). This makes a
# dataset count as stratified only when it actually breaks the measure down.
NONSTRAT_VALS <- c("overall", "all", "total", "all ages", "all races",
                   "both sexes", "all sexes", "")

is_stratified <- function(filepath, cols, candidate_cols) {
  length(strat_values(filepath, cols, candidate_cols)) > 0
}

# Distinct non-placeholder values of the first present stratifying column
# (e.g. the actual age groups). Empty if the column is absent or all "Overall".
strat_values <- function(filepath, cols, candidate_cols) {
  col <- candidate_cols[candidate_cols %in% cols][1]
  if (is.na(col)) return(character(0))
  vals <- tryCatch(
    vroom::vroom(filepath, col_select = tidyselect::all_of(col),
                 col_types = cols(.default = col_character()),
                 show_col_types = FALSE)[[1]],
    error = function(e) NULL)
  if (is.null(vals)) return(character(0))
  vals <- trimws(vals[!is.na(vals)])
  vals <- vals[nzchar(vals)]
  unique(vals[!(tolower(vals) %in% NONSTRAT_VALS)])
}

# Order age groups by their leading number ("0-4" < "18-49" < "65+"); values
# with no leading number sort last.
sort_age_groups <- function(x) {
  num <- suppressWarnings(as.integer(sub("^[^0-9]*([0-9]+).*", "\\1", x)))
  x[order(num, x, na.last = TRUE)]
}

read_time_col <- function(filepath, cols) {
  tcol <- TIME_COLS[TIME_COLS %in% cols][1]
  if (is.na(tcol)) return(NULL)
  tryCatch(
    vroom::vroom(filepath, col_select = tidyselect::all_of(tcol),
                 col_types = cols(.default = col_character()),
                 show_col_types = FALSE)[[1]],
    error = function(e) NULL)
}

# Earliest / latest observation dates from one file (returns Date, length 2).
date_range <- function(filepath, cols) {
  vals <- read_time_col(filepath, cols)
  if (is.null(vals)) return(c(NA, NA))
  d <- parse_dates(vals)
  if (!length(d)) return(c(NA, NA))
  c(min(d), max(d))
}

# Time resolution from measure_info (preferred) with a data-derived fallback.
normalize_resolution <- function(x) {
  x <- tolower(trimws(x %||% ""))
  if (!nzchar(x)) return(NA_character_)
  if (grepl("week", x)) return("Weekly")
  if (grepl("month", x)) return("Monthly")
  if (grepl("year|annual", x)) return("Annual")
  if (grepl("day|daily", x)) return("Daily")
  if (grepl("quarter", x)) return("Quarterly")
  tools::toTitleCase(x)
}

resolution_from_dates <- function(filepath, cols) {
  vals <- read_time_col(filepath, cols)
  if (is.null(vals)) return(NA_character_)
  d <- sort(unique(parse_dates(vals)))
  if (length(d) < 2) return(NA_character_)
  g <- as.numeric(stats::median(diff(d)))
  if (g <= 2) "Daily"
  else if (g <= 10) "Weekly"
  else if (g <= 45) "Monthly"
  else if (g <= 100) "Quarterly"
  else "Annual"
}

# Most recent git commit date (YYYY-MM-DD) across a set of files.
git_last_updated <- function(files) {
  if (!length(files)) return(NA_character_)
  dates <- vapply(files, function(f) {
    out <- tryCatch(
      system2("git", c("log", "-1", "--format=%cs", "--", f),
              stdout = TRUE, stderr = FALSE),
      error = function(e) character(0))
    if (length(out) == 0) NA_character_ else out[1]
  }, character(1))
  dates <- dates[!is.na(dates) & nzchar(dates)]
  if (!length(dates)) return(NA_character_)
  max(dates)  # ISO dates sort lexicographically
}

# Latest Issue: the date the PUBLISHER says the data was last updated, read from
# the metadata sidecar `dcf_download_cdc()` saves next to each download
# (`raw/<id>.json`, the payload of https://data.<host>/api/views/<id>). The
# `rowsUpdatedAt` field there is the publisher's own row-update timestamp, epoch
# seconds.
#
# This is a claim by the source rather than anything inferred from our
# repository, which is the whole point: file-based proxies cannot tell a real
# release from a re-export of identical rows in a different order, from a
# metadata-only refresh, or from repo housekeeping -- all three were observed
# producing phantom staleness flags. A publisher timestamp has none of those
# failure modes and is not relative to the checked-out branch.
#
# A source with several datasets has one sidecar each; take the newest. Sources
# not downloaded through the Socrata API have no sidecar, and those fall back to
# raw_last_issued() below. Because the two are different kinds of claim, the
# table marks which one each row used rather than silently blending them.
METADATA_DATE_FIELD <- "rowsUpdatedAt"

latest_issue_from_metadata <- function(source_dir) {
  raw_dir <- file.path(source_dir, "raw")
  if (!dir.exists(raw_dir)) return(NA_character_)
  sidecars <- list.files(raw_dir, pattern = "\\.json$", recursive = TRUE,
                         full.names = TRUE, ignore.case = TRUE)
  if (!length(sidecars)) return(NA_character_)

  dates <- character(0)
  for (s in sidecars) {
    meta <- tryCatch(fromJSON(s, simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(meta)) next
    v <- meta[[METADATA_DATE_FIELD]]
    if (is.null(v) || length(v) == 0) next        # not a Socrata view payload
    v <- v[[1]]
    d <- if (is.numeric(v)) {
      format(as.POSIXct(v, origin = "1970-01-01", tz = "UTC"), "%Y-%m-%d")
    } else if (is.character(v) && grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", v)) {
      substr(v, 1, 10)
    } else NA_character_
    if (!is.na(d)) dates <- c(dates, d)
  }
  if (!length(dates)) return(NA_character_)
  max(dates)  # ISO dates sort lexicographically
}

# Fallback for sources with no publisher timestamp: the most recent commit date
# on which a raw DATA file changed. This is a proxy, so it is narrowed to shed
# the noise that made the directory-level version unusable --
#   * only files that exist at HEAD are considered, so a ghost-file cleanup or a
#     sub-source moved into its own directory cannot pass as a release;
#   * `<id>.json` sidecars are excluded, since Socrata metadata churns on its own
#     schedule (they are the preferred signal above, but a metadata-only commit
#     is not evidence of new data). If raw/ holds nothing else, they are kept.
# One `git log` call with every file as a pathspec gives the newest commit
# touching any of them. Note this is still read from the checked-out branch.
raw_last_issued <- function(source_dir) {
  raw_dir <- file.path(source_dir, "raw")
  if (!dir.exists(raw_dir)) return(NA_character_)
  files <- list.files(raw_dir, recursive = TRUE, full.names = TRUE, all.files = FALSE)
  if (!length(files)) return(NA_character_)
  data_files <- files[!grepl("\\.json$", files, ignore.case = TRUE)]
  if (!length(data_files)) data_files <- files
  out <- tryCatch(
    system2("git", c("log", "-1", "--format=%cs", "--", gsub("\\\\", "/", data_files)),
            stdout = TRUE, stderr = FALSE),
    error = function(e) character(0))
  if (length(out) == 0 || is.na(out[1]) || !nzchar(out[1])) return(NA_character_)
  out[1]
}

# Latest Issue, preferring the publisher's own timestamp and falling back to the
# raw-file proxy. Returns the date plus which basis produced it, so the table can
# show that a value is an inference rather than a reported date.
latest_issue_for <- function(source_dir) {
  d <- latest_issue_from_metadata(source_dir)
  if (!is.na(d)) return(list(date = d, basis = "metadata"))
  d <- raw_last_issued(source_dir)
  if (!is.na(d)) return(list(date = d, basis = "files"))
  list(date = NA_character_, basis = "none")
}

# Name of the default branch, for the behind-check below.
git_default_branch <- function() {
  out <- tryCatch(
    system2("git", c("symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD"),
            stdout = TRUE, stderr = FALSE),
    error = function(e) character(0))
  if (length(out) && !is.na(out[1]) && nzchar(out[1])) return(out[1])
  for (b in c("main", "master")) {
    ok <- tryCatch(
      system2("git", c("rev-parse", "--verify", "--quiet", b), stdout = TRUE, stderr = FALSE),
      error = function(e) character(0))
    if (length(ok) && !is.na(ok[1]) && nzchar(ok[1])) return(b)
  }
  NA_character_
}

# How many commits HEAD is missing relative to `ref`. Every date column here
# comes from `git log` on the CHECKED-OUT branch, so a branch that trails the
# default branch reports stale dates for every dataset -- and the staleness flag
# then fires on datasets that are perfectly healthy on the default branch.
git_commits_behind <- function(ref) {
  if (is.na(ref)) return(NA_integer_)
  out <- tryCatch(
    system2("git", c("rev-list", "--count", paste0("HEAD..", ref)),
            stdout = TRUE, stderr = FALSE),
    error = function(e) character(0))
  if (length(out) == 0 || is.na(out[1]) || !nzchar(out[1])) return(NA_integer_)
  suppressWarnings(as.integer(out[1]))
}

# Days by which Latest Issue leads Last Refreshed. Positive means new upstream
# data landed in raw/ after the last time standardized output was committed.
# NA when either date is unavailable (nothing to compare).
issue_lead_days <- function(last_refreshed, latest_issue) {
  iso <- function(x) {
    if (is.null(x) || length(x) == 0 || is.na(x[1])) return(as.Date(NA))
    if (!grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", x[1])) return(as.Date(NA))  # "—" etc.
    as.Date(x[1])
  }
  d1 <- iso(last_refreshed); d2 <- iso(latest_issue)
  if (is.na(d1) || is.na(d2)) return(NA_integer_)
  as.integer(d2 - d1)
}

# -----------------------------------------------------------------------------
# Per-source summary
# -----------------------------------------------------------------------------
summarize_source <- function(source_name, source_dir) {
  measure_info <- tryCatch(
    fromJSON(file.path(source_dir, "measure_info.json"), simplifyVector = FALSE),
    error = function(e) list())

  sources_meta <- measure_info[["_sources"]]
  first_source <- if (!is.null(sources_meta) && length(sources_meta) > 0) sources_meta[[1]] else list()

  title        <- first_source$name %||% format_source_name(source_name)
  organization <- first_source$organization %||% ""

  index_entry  <- INDEX_ENTRIES[[source_name]]
  search_terms <- index_entry$search_terms %||% character(0)

  # Brief description: the curated summary from the data sources index, so this
  # column matches the website data page verbatim. Datasets absent from the
  # index (or with a blank summary) fall back to a derivation spanning ALL
  # _sources entries, so multi-source datasets aren't misrepresented by only the
  # first (e.g. NCHS covers overdose AND 21 causes of mortality) -- one sentence
  # per distinct source, joined.
  description <- index_entry$summary %||% ""
  if (!nzchar(description)) {
    descs <- character(0)
    if (!is.null(sources_meta)) {
      for (s in sources_meta) {
        ss <- short_summary(s$description %||% "")
        if (nzchar(ss)) descs <- c(descs, ss)
      }
    }
    description <- paste(unique(descs), collapse = " ")
  }

  data_url     <- first_source$url %||% ""
  restrictions <- first_source$restrictions %||% ""

  # time_resolution declared in measure_info (across all measures).
  declared_res <- character(0)
  for (key in names(measure_info)) {
    if (key == "_sources") next
    m <- measure_info[[key]]
    if (is.list(m) && !is.null(m$time_resolution)) {
      r <- normalize_resolution(m$time_resolution)
      if (!is.na(r)) declared_res <- c(declared_res, r)
    }
  }
  declared_res <- unique(declared_res)

  files <- get_standard_files(source_dir)

  geo <- character(0)
  age_groups <- character(0)
  sex_strat <- FALSE
  other <- character(0)
  mins <- as.Date(character(0)); maxs <- as.Date(character(0))
  derived_res <- character(0)

  for (f in files) {
    cols <- get_columns(f)
    if (!length(cols)) next
    geo <- union(geo, geo_levels(f, cols))
    age_groups <- union(age_groups, strat_values(f, cols, AGE_COLS))
    if (!sex_strat && is_stratified(f, cols, SEX_COLS)) sex_strat <- TRUE
    # Value-based (like age/sex): only count an "other" dimension when it
    # actually varies -- a column that is entirely "Total"/"Overall" does not.
    for (oc in intersect(names(OTHER_STRAT), cols)) {
      if (length(strat_values(f, cols, oc))) other <- union(other, OTHER_STRAT[[oc]])
    }
    dr <- date_range(f, cols)
    if (!is.na(dr[1])) mins <- c(mins, as.Date(dr[1], origin = "1970-01-01"))
    if (!is.na(dr[2])) maxs <- c(maxs, as.Date(dr[2], origin = "1970-01-01"))
    if (!length(declared_res)) {
      rr <- resolution_from_dates(f, cols)
      if (!is.na(rr)) derived_res <- c(derived_res, rr)
    }
  }

  geo_order <- c("National", "State", "County")
  spatial <- paste(geo_order[geo_order %in% geo], collapse = ", ")

  time_scale <- if (length(declared_res)) paste(declared_res, collapse = ", ")
                else paste(unique(derived_res), collapse = ", ")

  last_updated <- git_last_updated(files) %||% "—"
  issue <- latest_issue_for(source_dir)
  latest_issue <- issue$date %||% "—"

  list(
    folder       = source_name,
    title        = title,
    description  = description,
    search_terms = search_terms,
    spatial      = if (nzchar(spatial)) spatial else "—",
    age          = if (length(age_groups)) paste(sort_age_groups(age_groups), collapse = ", ")
                   else "Not Stratified",
    sex          = if (sex_strat) "Stratified" else "Not Stratified",
    other        = if (length(other)) paste(sort(other), collapse = ", ") else "—",
    earliest     = if (length(mins)) format(min(mins), "%Y-%m-%d") else "—",
    latest       = if (length(maxs)) format(max(maxs), "%Y-%m-%d") else "—",
    time_scale   = if (nzchar(time_scale)) time_scale else "—",
    last_updated = last_updated,
    latest_issue = latest_issue,
    issue_basis  = issue$basis,
    issue_lead   = issue_lead_days(last_updated, latest_issue),
    restrictions = if (nzchar(restrictions)) restrictions else "—",
    organization = if (nzchar(organization)) organization else "—",
    data_url     = data_url
  )
}

# -----------------------------------------------------------------------------
# Per-bundle summary (datasets involved come from process.json$source_files)
# -----------------------------------------------------------------------------
summarize_bundle <- function(bundle_name, bundle_dir, valid_sources) {
  datasets <- character(0)

  # (a) dcf-tracked source files, when present.
  proc <- tryCatch(
    fromJSON(file.path(bundle_dir, "process.json"), simplifyVector = FALSE),
    error = function(e) list())
  src_files <- proc$source_files
  if (!is.null(src_files) && length(src_files) > 0) {
    datasets <- vapply(names(src_files), function(k) strsplit(k, "/")[[1]][1],
                       character(1))
  }

  # (b) sibling-source references in build.R (../<source>/...). This is the
  # authoritative current wiring and covers bundles with no source_files record.
  build_path <- file.path(bundle_dir, "build.R")
  if (file.exists(build_path)) {
    txt <- readLines(build_path, warn = FALSE)
    hits <- unlist(regmatches(txt, gregexpr("\\.\\./([A-Za-z0-9_]+)/", txt)))
    hits <- sub("\\.\\./([A-Za-z0-9_]+)/.*", "\\1", hits)
    datasets <- c(datasets, hits)
  }

  # Keep only real sibling data sources; drop resources/, data/, self-refs, etc.
  datasets <- unique(datasets)
  datasets <- datasets[datasets %in% valid_sources & datasets != bundle_name]
  datasets <- sort(vapply(datasets, format_source_name, character(1)))

  list(
    folder   = bundle_name,
    title    = format_bundle_name(bundle_name),
    n        = length(datasets),
    datasets = if (length(datasets)) paste(datasets, collapse = ", ") else "—"
  )
}

# -----------------------------------------------------------------------------
# HTML building
# -----------------------------------------------------------------------------
DATASET_HEADERS <- c("Dataset", "Content Title", "Brief Description", "Subject Tags",
                     "Spatial Resolution", "Age Resolution", "Sex Resolution",
                     "Other Resolutions", "Earliest Data", "Latest Data",
                     "Time Scale", "Last Refreshed", "Latest Issue",
                     "Data Restrictions", "Organization", "Source URL", "Data URL")

strat_class <- function(v) if (identical(v, "Not Stratified")) "strat-notstratified" else "strat-stratified"

# TRUE when new upstream data (raw/) landed well after the last standardized
# output commit -- see STALE_THRESHOLD_DAYS.
is_stale <- function(s) {
  !is.null(s$issue_lead) && !is.na(s$issue_lead) && s$issue_lead > STALE_THRESHOLD_DAYS
}

dataset_row <- function(s) {
  source_cell <- if (nzchar(s$data_url)) {
    tags$a(href = s$data_url, target = "_blank", rel = "noopener", "Link")
  } else "—"
  data_cell <- tags$a(href = paste0(GITHUB_DATA_BASE, "/", s$folder),
                      target = "_blank", rel = "noopener", "GitHub")
  dataset_cell <- tags$a(href = paste0("index.html#", doc_section_id(s$folder)),
                         tags$code(s$folder))
  # Rendered as badges, but DataTables still searches/sorts on the plain text.
  terms_cell <- if (length(s$search_terms)) {
    lapply(s$search_terms, function(t) tags$span(class = "term-badge", t))
  } else "—"
  # A date derived from raw-file commits rather than reported by the publisher is
  # marked, so the column is never read as though every value means the same thing.
  basis_marker <- if (identical(s$issue_basis, "files")) {
    tags$span(class = "basis-inferred",
      title = paste0("No publisher timestamp for this source; inferred from the most recent ",
                     "commit that changed a raw data file."),
      "est.")
  } else NULL
  # Latest Issue carries the staleness flag. `data-order` keeps DataTables
  # sorting on the bare date rather than on the date plus badge text.
  issue_cell <- if (is_stale(s)) {
    tagList(
      s$latest_issue, basis_marker,
      tags$span(class = "stale-flag",
        title = sprintf(paste0("The publisher reports updating this data %d days after our last ",
                               "standardized output commit (threshold: %d days), so the ",
                               "standardized files here are probably missing that update."),
                        s$issue_lead, STALE_THRESHOLD_DAYS),
        sprintf("⚠ +%dd", s$issue_lead))
    )
  } else tagList(s$latest_issue, basis_marker)
  tags$tr(
    tags$td(dataset_cell),
    tags$td(class = "title-cell", s$title),
    tags$td(class = "desc-cell", s$description),
    tags$td(class = "terms-cell", terms_cell),
    tags$td(s$spatial),
    tags$td(class = strat_class(s$age), s$age),
    tags$td(class = strat_class(s$sex), s$sex),
    tags$td(s$other),
    tags$td(s$earliest),
    tags$td(s$latest),
    tags$td(s$time_scale),
    tags$td(s$last_updated),
    tags$td(`data-order` = s$latest_issue,
            class = if (is_stale(s)) "issue-stale" else NULL,
            issue_cell),
    tags$td(s$restrictions),
    tags$td(s$organization),
    tags$td(source_cell),
    tags$td(data_cell)
  )
}

bundle_row <- function(b) {
  folder_cell <- tags$a(href = paste0("index.html#", doc_section_id(b$folder)),
                        tags$code(b$folder))
  tags$tr(
    tags$td(folder_cell),
    tags$td(class = "title-cell", b$title),
    tags$td(class = "text-center", b$n),
    tags$td(b$datasets)
  )
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
cat("Building data table...\n")

data_dir <- "data"
all_dirs <- list.dirs(data_dir, recursive = FALSE, full.names = TRUE)

# Every non-bundle data directory is a candidate "source" a bundle may reference.
valid_sources <- basename(all_dirs[!grepl("^bundle_", basename(all_dirs))])

all_dirs <- all_dirs[sapply(all_dirs, function(d) file.exists(file.path(d, "measure_info.json")))]

source_dirs <- all_dirs[!grepl("^bundle_", basename(all_dirs))]
bundle_dirs <- all_dirs[grepl("^bundle_", basename(all_dirs))]

# Only list sources that actually have standardized data files -- every dataset
# column is derived from standard/*.csv.gz, so sources with none (scaffolding
# templates or not-yet-standardized sources) are skipped. They appear
# automatically once standardized data is added.
has_standard <- vapply(source_dirs, function(d) length(get_standard_files(d)) > 0, logical(1))
skipped <- basename(source_dirs[!has_standard])
if (length(skipped)) {
  cat(sprintf("Skipping %d source(s) with no standard data: %s\n",
              length(skipped), paste(skipped, collapse = ", ")))
}
source_dirs <- source_dirs[has_standard]

source_dirs <- source_dirs[order(basename(source_dirs))]
bundle_dirs <- bundle_dirs[order(basename(bundle_dirs))]

cat(sprintf("Found %d data sources and %d bundles\n", length(source_dirs), length(bundle_dirs)))

source_summaries <- lapply(seq_along(source_dirs), function(i) {
  cat(sprintf("  source %s (%d/%d)\n", basename(source_dirs[i]), i, length(source_dirs)))
  summarize_source(basename(source_dirs[i]), source_dirs[i])
})

# Mirrors the tripwire in build_docs.R: make an auto-derived Brief Description
# visible in build/CI logs rather than letting it pass as curated text.
no_summary <- vapply(source_summaries, function(s) {
  is.null(INDEX_ENTRIES[[s$folder]]) || !nzchar(INDEX_ENTRIES[[s$folder]]$summary %||% "")
}, logical(1))
if (any(no_summary)) {
  cat(sprintf(
    "WARNING: %d source(s) have no curated summary in docs/data_sources_index.json -- using an auto-derived fallback: %s\n",
    sum(no_summary), paste(vapply(source_summaries[no_summary], `[[`, character(1), "folder"), collapse = ", ")))
}

bundle_summaries <- lapply(seq_along(bundle_dirs), function(i) {
  cat(sprintf("  bundle %s (%d/%d)\n", basename(bundle_dirs[i]), i, length(bundle_dirs)))
  summarize_bundle(basename(bundle_dirs[i]), bundle_dirs[i], valid_sources)
})

updated_stamp <- format(Sys.Date(), "%B %d, %Y")

# Every date column is read from the checked-out branch's git history, so a
# branch trailing the default branch produces stale dates and phantom staleness
# flags across the board. Say so loudly rather than letting the page look
# authoritative. (In CI this runs on the default branch, so `behind` is 0.)
default_branch <- git_default_branch()
commits_behind <- git_commits_behind(default_branch)
branch_is_behind <- !is.na(commits_behind) && commits_behind > 0
if (branch_is_behind) {
  cat(sprintf(paste0(
    "WARNING: HEAD is %d commit(s) behind %s. Last Refreshed / Latest Issue and the\n",
    "         staleness flag are computed from THIS branch's history, so they will look\n",
    "         stale for datasets that are current on %s. Rebuild after merging.\n"),
    commits_behind, default_branch, default_branch))
}

# Datasets where upstream raw data is materially newer than our standardized
# output. Surfaced both on the page and in the build/CI log.
basis_count <- function(b) sum(vapply(source_summaries,
                                      function(s) identical(s$issue_basis, b), logical(1)))
cat(sprintf("Latest Issue basis: %d publisher timestamp, %d raw-file commit (est.), %d unavailable\n",
            basis_count("metadata"), basis_count("files"), basis_count("none")))

stale_sources <- Filter(is_stale, source_summaries)
if (length(stale_sources)) {
  cat(sprintf(
    "WARNING: %d dataset(s) have a Latest Issue more than %d days after their Last Refreshed (new raw data not yet standardized): %s\n",
    length(stale_sources), STALE_THRESHOLD_DAYS,
    paste(vapply(stale_sources, function(s) sprintf("%s (+%dd)", s$folder, s$issue_lead),
                 character(1)), collapse = ", ")))
} else {
  cat(sprintf("No datasets flagged stale (Latest Issue within %d days of Last Refreshed).\n",
              STALE_THRESHOLD_DAYS))
}

# "How to read this table": the date columns are easy to misread -- two of them
# are git commit dates about our pipeline, one is a property of the data.
notes_block <- tags$details(class = "table-notes", open = NA,
  tags$summary("How to read this table"),
  tags$dl(
    tags$dt("Earliest Data / Latest Data"),
    tags$dd("The first and last observation dates found in the time column of the dataset's ",
            tags$code("standard/*.csv.gz"), " files. These describe the data itself — the period it covers."),

    tags$dt("Last Refreshed"),
    tags$dd("The most recent git commit date of the dataset's ", tags$code("standard/*.csv.gz"),
            " files: when our pipeline last wrote standardized output. It can move without any new ",
            "upstream data — for example after an ", tags$code("ingest.R"),
            " fix — so it reflects our processing cadence, not the publisher's."),

    tags$dt("Latest Issue"),
    tags$dd("When the source last published new or changed data, taken from the publisher wherever ",
            "possible: the ", tags$code("rowsUpdatedAt"), " field of the metadata saved alongside ",
            "each download (", tags$code("raw/<id>.json"), "). That is the source's own claim, so ",
            "unlike a file-based proxy it cannot be moved by a re-export of identical rows, a ",
            "metadata-only refresh, or repository housekeeping. Sources not pulled through the ",
            "Socrata API report no such timestamp; those fall back to the most recent commit that ",
            "changed a raw data file, and are marked ", tags$span(class = "basis-inferred", "est."),
            " to show the date is inferred rather than reported. A dash means neither was available."),

    tags$dt(HTML("&#9888; flag on Latest Issue")),
    tags$dd(sprintf(paste0("Shown when Latest Issue is more than %d days after Last Refreshed: the ",
                           "publisher has updated the data since we last rebuilt the standardized ",
                           "output, so the files here are behind the source. The badge gives the gap ",
                           "in days. "), STALE_THRESHOLD_DAYS),
            tags$strong(sprintf("%d of %d dataset%s flagged", length(stale_sources),
                                length(source_summaries),
                                if (length(stale_sources) == 1) "" else "s")),
            sprintf("; %d use a publisher timestamp, %d fall back to raw-file commits, %d have neither.",
                    sum(vapply(source_summaries, function(s) identical(s$issue_basis, "metadata"), logical(1))),
                    sum(vapply(source_summaries, function(s) identical(s$issue_basis, "files"), logical(1))),
                    sum(vapply(source_summaries, function(s) identical(s$issue_basis, "none"), logical(1)))))
  ),
  tags$p(class = "caveat",
    tags$strong("Caveats. "),
    "Last Refreshed is a repository commit date read from the branch this page was built on, so a ",
    "branch behind ",
    if (is.na(default_branch)) "the default branch" else tags$code(default_branch),
    " reports it as stale for every dataset. Latest Issue is only as current as the last time we ",
    "downloaded the dataset, since it comes from the metadata saved with that download: if a source ",
    "has not been fetched in a while, its publisher timestamp is stale too, and the gap between the ",
    "two columns understates how far behind we are. It also reflects whatever the publisher chooses ",
    "to report — some update the timestamp on a full re-publish that changes no values. The ",
    tags$span(class = "basis-inferred", "est."),
    " fallback dates carry the weaknesses of any file-based proxy: a publisher that re-exports ",
    "identical rows in a different order changes the file's bytes and moves the date with no new ",
    "data behind it, and like Last Refreshed they are read from the current branch.")
)

# Prominent, page-level version of the behind-branch warning above.
branch_banner <- if (branch_is_behind) {
  tags$div(class = "branch-warning",
    tags$strong("Built from a branch that is behind. "),
    sprintf("HEAD is %d commit%s behind %s. Last Refreshed, Latest Issue and the ",
            commits_behind, if (commits_behind == 1) "" else "s", default_branch),
    HTML("&#9888;"), " flags below are computed from this branch's git history and will look ",
    "stale for datasets that are current on ", default_branch, ".")
} else NULL

datasets_table <- tags$table(id = "datasets-table",
  class = "display table table-striped table-bordered table-hover", style = "width:100%",
  tags$thead(tags$tr(lapply(DATASET_HEADERS, tags$th))),
  tags$tbody(lapply(source_summaries, dataset_row))
)

bundles_table <- tags$table(id = "bundles-table",
  class = "display table table-striped table-bordered table-hover", style = "width:100%",
  tags$thead(tags$tr(
    tags$th("Folder"), tags$th("Bundle"), tags$th("# Datasets"),
    tags$th("Datasets Involved")
  )),
  tags$tbody(lapply(bundle_summaries, bundle_row))
)

page <- tags$html(lang = "en",
  tags$head(
    tags$meta(charset = "UTF-8"),
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1"),
    tags$title("PopHIVE Data Table"),
    tags$link(rel = "stylesheet",
      href = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"),
    tags$link(rel = "stylesheet",
      href = "https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css"),
    tags$style(HTML("
      body { padding: 1.5rem; }
      h1 { margin-bottom: .25rem; }
      .subtitle { color: #6c757d; margin-bottom: 1rem; }
      table.dataTable td { font-size: .85rem; vertical-align: top; }
      table.dataTable th { font-size: .8rem; }
      .title-cell { font-weight: 600; }
      /* Curated summaries run long (some >1000 chars); give them room so the
         surrounding one-word columns aren't squeezed into vertical slivers. */
      .desc-cell { min-width: 26rem; }
      .terms-cell { min-width: 9rem; }
      .term-badge {
        display: inline-block; background: #eef2f7; border: 1px solid #d6dee8;
        border-radius: .75rem; padding: .05rem .5rem; margin: 0 .2rem .2rem 0;
        font-size: .75rem; color: #33415c; white-space: nowrap;
      }
      .strat-stratified { color: #146c43; font-weight: 600; }
      .strat-notstratified { color: #6c757d; }
      .nav-tabs { margin-bottom: 1rem; }
      /* \"How to read this table\" note */
      .table-notes {
        background: #f8f9fa; border: 1px solid #dee2e6; border-radius: .5rem;
        padding: .75rem 1rem; margin-bottom: 1.25rem; font-size: .85rem;
      }
      .table-notes > summary {
        cursor: pointer; font-weight: 600; font-size: .9rem;
      }
      .table-notes dl { margin: .75rem 0 0; }
      .table-notes dt { margin-top: .5rem; }
      .table-notes dd { margin: 0 0 0 1.25rem; color: #495057; }
      .table-notes .caveat { margin: .75rem 0 0; color: #495057; }
      .table-notes code, table.dataTable code { font-size: .8rem; }
      /* Staleness flag: Latest Issue well after Last Refreshed. */
      .issue-stale { white-space: nowrap; }
      .stale-flag {
        display: inline-block; margin-left: .35rem; padding: .05rem .4rem;
        background: #fff3cd; border: 1px solid #ffe69c; border-radius: .5rem;
        color: #664d03; font-size: .75rem; font-weight: 600; cursor: help;
      }
      .basis-inferred {
        display: inline-block; margin-left: .3rem; padding: 0 .3rem;
        background: #eef2f7; border: 1px solid #d6dee8; border-radius: .4rem;
        color: #6c757d; font-size: .7rem; font-style: italic; cursor: help;
      }
      .branch-warning {
        background: #f8d7da; border: 1px solid #f1aeb5; border-radius: .5rem;
        padding: .6rem .9rem; margin-bottom: 1rem; font-size: .85rem; color: #58151c;
      }
    "))
  ),
  tags$body(
    tags$h1("PopHIVE Data Table"),
    tags$p(class = "subtitle",
      "Overview of all standardized data sources and combined bundles in the ",
      tags$a(href = "https://github.com/PopHIVE/Ingest", target = "_blank", "PopHIVE/Ingest"),
      " repository. Automatically generated from repository files — last updated ",
      updated_stamp, ". ",
      tags$a(href = "index.html", "View full data documentation →")),

    branch_banner,
    notes_block,

    tags$ul(class = "nav nav-tabs", id = "mainTabs", role = "tablist",
      tags$li(class = "nav-item", role = "presentation",
        tags$button(class = "nav-link active", id = "datasets-tab",
          `data-bs-toggle` = "tab", `data-bs-target` = "#datasets-pane",
          type = "button", role = "tab", "Datasets",
          tags$span(class = "badge bg-secondary ms-2", length(source_summaries)))),
      tags$li(class = "nav-item", role = "presentation",
        tags$button(class = "nav-link", id = "bundles-tab",
          `data-bs-toggle` = "tab", `data-bs-target` = "#bundles-pane",
          type = "button", role = "tab", "Bundles",
          tags$span(class = "badge bg-secondary ms-2", length(bundle_summaries))))
    ),

    tags$div(class = "tab-content",
      tags$div(class = "tab-pane fade show active", id = "datasets-pane", role = "tabpanel",
        tags$div(class = "table-responsive", datasets_table)),
      tags$div(class = "tab-pane fade", id = "bundles-pane", role = "tabpanel",
        tags$div(class = "table-responsive", bundles_table))
    ),

    tags$script(src = "https://code.jquery.com/jquery-3.7.1.min.js"),
    tags$script(src = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"),
    tags$script(src = "https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"),
    tags$script(src = "https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"),
    tags$script(HTML("
      $(function () {
        var dsTable = $('#datasets-table').DataTable({
          pageLength: 25, order: [[1, 'asc']], scrollX: true
        });
        $('#bundles-table').DataTable({
          pageLength: 25, order: [[1, 'asc']]
        });
        // DataTables mis-measures column widths when initialized inside a hidden
        // tab; recalculate when a tab is shown.
        $('button[data-bs-toggle=\"tab\"]').on('shown.bs.tab', function () {
          $.fn.dataTable.tables({visible: true, api: true}).columns.adjust();
        });
      });
    "))
  )
)

if (!dir.exists("docs")) dir.create("docs")
output_path <- "docs/data-table.html"
cat(sprintf("Writing %s...\n", output_path))
save_html(page, output_path)
cat("Done.\n")
