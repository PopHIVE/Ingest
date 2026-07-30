# =============================================================================
# NCCR (National Childhood Cancer Registry) Explorer Data Ingestion
# Source: https://nccrexplorer.ccdi.cancer.gov/
#
# NCCR*Explorer publishes cancer incidence statistics for children, adolescents,
# and young adults (ages 0-39), diagnosed 2001 forward, pooled from ~29 U.S.
# cancer registries covering ~76% of the U.S. population. Data are NATIONAL ONLY
# (no state/registry breakdown is available from this source).
#
# The web application is backed by a JSON endpoint (render_region_5.php). For a
# given cancer site it returns the full cross-product of sex x race/ethnicity x
# age, each with an annual series of [year, rate, rate_se, rate_lower_ci,
# rate_upper_ci, modeled_rate]. We pull every ICCC site/category, reshape to the
# standard wide format (one rate column per ICCC site), and write data.csv.gz.
#
# Output (standard/data.csv.gz), wide by ICCC site:
#   geography, time, age, sex, race_ethnicity,
#   nccr_<site>, nccr_<site>_lcl, nccr_<site>_ucl   (one trio per ICCC site)
# where rate is the age-adjusted incidence rate per 1,000,000.
# =============================================================================

suppressMessages({
  library(dplyr)
  library(tidyr)
  library(jsonlite)
})

API_BASE  <- "https://nccrexplorer.ccdi.cancer.gov/source/content_writers"
VAR_URL   <- file.path(API_BASE, "get_var_formats.php")
DATA_URL  <- file.path(API_BASE, "render_region_5.php")
DATA_TYPE <- 1L   # 1 = Incidence
GRAPH_TYPE <- 2L  # 2 = Trends Over Time (annual series)

# Initialize process record (process.json is created by dcf::dcf_add_source())
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data into memory, write single combined file
# -----------------------------------------------------------------------------
dir.create("raw", showWarnings = FALSE)

fetch_content <- function(url, tries = 3) {
  for (i in seq_len(tries)) {
    result <- tryCatch(
      paste(readLines(url, warn = FALSE), collapse = ""),
      error = function(e) NULL
    )
    if (!is.null(result) && nchar(result) > 0) return(result)
    Sys.sleep(2)
  }
  stop("Failed to download: ", url)
}

# Variable formats: defines site codes/names and the curated list of ICCC sites
vf_parsed <- fromJSON(fetch_content(VAR_URL))
vf <- vf_parsed$VariableFormats
cancer_sites <- vf_parsed$CancerSites
cancer_sites <- cancer_sites[cancer_sites != 9999]  # drop "Compare Cancer Sites" pseudo-option

# Restrict to "All ICCC Sites Combined" plus the top-level ICCC category groups
# (the Roman-numeral level), dropping the lettered subcategories (I.a, I.b, ...).
# Edit this list to change which sites are ingested.
GROUP_SITES <- c(
  1,     # All ICCC Sites Combined
  100,   # I.   Leukemias
  200,   # II.  Lymphomas
  300,   # III. CNS Neoplasms (Malignant)
  2300,  # III. CNS Neoplasms (Non-Malignant)
  400,   # IV.  Neuroblastoma and Peripheral Nervous Cell
  500,   # V.   Retinoblastoma
  600,   # VI.  Renal Tumors
  700,   # VII. Hepatic Tumors
  800,   # VIII.Bone Tumors
  900,   # IX.  Soft Tissue Tumors
  1000,  # X.   Germ Cell Tumors (Malignant)
  3000,  # X.   Germ Cell Tumors (Non-Malignant)
  1100,  # XI.  Epithelial Neoplasms and Melanomas
  1200   # XII. Other and Unspecified Neoplasms
)
cancer_sites <- GROUP_SITES[GROUP_SITES %in% cancer_sites]

# ---------------------------------------------------------------------------
# Code -> label lookups
# ---------------------------------------------------------------------------
# Age ranges relevant to incidence trends (overlapping NCCR groupings retained)
# "0-39" is NCCR's full population total (source label "Ages <40"); "0-19" is
# the childhood-only total (source label "Ages <20").
age_lab <- c(
  "1" = "0-19", "2" = "<1", "3" = "1-4", "4" = "5-9", "5" = "10-14",
  "6" = "15-19", "7" = "0-39", "8" = "15-39", "9" = "20-39",
  "10" = "20-24", "11" = "25-29", "12" = "30-39"
)
sex_lab <- c("1" = "Overall", "2" = "Male", "3" = "Female")
race_lab <- c(
  "1" = "Overall", "2" = "White", "3" = "Black",
  "4" = "Asian/Pacific Islander", "5" = "American Indian/Alaska Native",
  "6" = "Hispanic"
)

# ICCC site code -> short label -> column slug (prefer short_name, fall back to full)
site_full  <- vf$site
site_short <- vf$site_short_name
make_slug <- function(name) {
  s <- tolower(name)
  s <- gsub("&", " and ", s, fixed = TRUE)
  s <- gsub("+", "_p_", s, fixed = TRUE)
  s <- gsub("[^a-z0-9]+", "_", s)
  s <- gsub("_+", "_", s)
  s <- gsub("^_|_$", "", s)
  # drop the leading ICCC roman-numeral group prefix (e.g. "i_leukemias" -> "leukemias")
  s <- sub("^(i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii)_", "", s)
  s
}
site_name <- vapply(as.character(cancer_sites), function(c) {
  if (!is.null(site_short[[c]])) site_short[[c]] else site_full[[c]]
}, character(1))
site_slug <- setNames(make_slug(site_name), as.character(cancer_sites))

# Download all site data into memory and parse to long format
parse_site <- function(raw, code) {
  keys <- names(raw$data)
  if (is.null(keys) || !length(keys)) return(NULL)
  slug <- site_slug[[as.character(code)]]
  do.call(rbind, lapply(keys, function(k) {
    parts <- strsplit(k, "_", fixed = TRUE)[[1]]  # sex_race_age_subtype_site
    m <- raw$data[[k]]$data_series                # matrix: year,rate,se,lci,uci,modeled
    if (is.null(m) || length(m) == 0) return(NULL)
    if (is.null(dim(m))) m <- matrix(m, nrow = 1)
    data.frame(
      sex = sex_lab[parts[1]],
      race_ethnicity = race_lab[parts[2]],
      age = age_lab[parts[3]],
      site = slug,
      year = suppressWarnings(as.integer(m[, 1])),
      rate = suppressWarnings(as.numeric(m[, 2])),
      lcl  = suppressWarnings(as.numeric(m[, 4])),
      ucl  = suppressWarnings(as.numeric(m[, 5])),
      stringsAsFactors = FALSE, row.names = NULL
    )
  }))
}

long <- bind_rows(lapply(cancer_sites, function(code) {
  url <- sprintf("%s?data_type=%d&graph_type=%d&site=%d", DATA_URL, DATA_TYPE, GRAPH_TYPE, code)
  site_raw <- fromJSON(fromJSON(fetch_content(url)))  # response is double-encoded JSON
  Sys.sleep(0.2)
  parse_site(site_raw, code)
})) %>%
  filter(!is.na(year), !is.na(age), !is.na(sex), !is.na(race_ethnicity))

# Write single combined raw file
vroom::vroom_write(long, "raw/nccr_raw.csv.gz", delim = ",")

# -----------------------------------------------------------------------------
# 2. Change detection (hash combined raw file)
# -----------------------------------------------------------------------------
raw_state <- list(hash = tools::md5sum("raw/nccr_raw.csv.gz"))

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 5. Reshape to standard wide format (one rate column per ICCC site)
  # ---------------------------------------------------------------------------
  data_standard <- long %>%
    mutate(
      geography = "00",
      time = sprintf("%d-12-31", year)
    ) %>%
    select(geography, time, age, sex, race_ethnicity, site, rate, lcl, ucl) %>%
    pivot_wider(
      id_cols = c(geography, time, age, sex, race_ethnicity),
      names_from = site,
      values_from = c(rate, lcl, ucl),
      names_glue = "nccr_{site}@@{.value}"
    ) %>%
    arrange(geography, time, age, sex, race_ethnicity)

  # Tidy column names: rate -> nccr_<site>, lcl/ucl -> nccr_<site>_lcl / _ucl
  nm <- names(data_standard)
  nm <- sub("@@rate$", "", nm)
  nm <- sub("@@lcl$", "_lcl", nm)
  nm <- sub("@@ucl$", "_ucl", nm)
  names(data_standard) <- nm

  # Add per-site flags before NA replacement:
  #   suppressed_flag    = 1 when rate == 0 and CI is absent (API-suppressed cell)
  #   not_collected_flag = 1 when rate is NA (not returned by API at all)
  for (s in site_slug) {
    rate_col <- paste0("nccr_", s)
    lcl_col  <- paste0("nccr_", s, "_lcl")
    data_standard[[paste0("nccr_", s, "_suppressed")]] <- as.integer(
      !is.na(data_standard[[rate_col]]) &
        data_standard[[rate_col]] == 0 &
        is.na(data_standard[[lcl_col]])
    )
    data_standard[[paste0("nccr_", s, "_not_collected")]] <- as.integer(
      is.na(data_standard[[rate_col]])
    )
  }

  # Replace all remaining NAs with 0
  data_standard[is.na(data_standard)] <- 0L

  # Order columns: index cols then per-site groups (rate, lcl, ucl, suppressed, not_collected)
  idx_cols <- c("geography", "time", "age", "sex", "race_ethnicity")
  val_cols <- unlist(lapply(site_slug, function(s) {
    c(paste0("nccr_", s), paste0("nccr_", s, "_lcl"), paste0("nccr_", s, "_ucl"),
      paste0("nccr_", s, "_suppressed"), paste0("nccr_", s, "_not_collected"))
  }), use.names = FALSE)
  val_cols <- val_cols[val_cols %in% names(data_standard)]
  data_standard <- data_standard[, c(idx_cols, val_cols)]

  # ---------------------------------------------------------------------------
  # 6. Write standardized output
  # ---------------------------------------------------------------------------
  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 7. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
