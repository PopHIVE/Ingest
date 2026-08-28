# =============================================================================
# Bundle: Census (source-complete)
#
# A complete tall-format mirror of the census source, in one place and one
# shape. Unlike every other bundle in this repo, this one is organised by
# SOURCE rather than by topic: its purpose is to expose the entire Census
# dataset as two parquet files with a uniform (geography, time, measure, value,
# source) schema, rather than the six wide CSVs -- split by geography level and
# by Census program -- that census/standard/ ships.
#
# Measures here are DELIBERATELY DUPLICATED into the topic bundles, where they
# sit alongside comparable measures from other sources:
#
#   bundle_county_access  ACS social determinants, SAHIE, SAIPE, urban/rural,
#                         OQM -- next to CHR access, AHRF providers, HUD
#                         housing, BLS unemployment, USDA food access
#   bundle_maternal_health  acs_BTH, as `birth_rate`
#
# The topic bundles are canonical for analysis: their measures are curated and
# directly comparable across sources. This bundle is a convenience mirror of a
# single source. Do not union it with the topic bundles -- you will double-count.
# The same pattern already exists between bundle_antimicrobial_resistance and
# bundle_enteric_diseases, which ship byte-identical narms outputs.
#
# NO ALLOW-LIST BY DESIGN. Every non-index column of every census standard file
# is included. bundle_county_access curates an explicit list of measures; if
# this bundle did too, the two would silently drift apart whenever the Census
# ingest gained a variable. Being definitionally complete means it cannot.
#
# Sources (all of census/standard/, ZCTA excluded -- it is no longer produced):
#   data_state.csv.gz   ACS 5-year, 2-digit FIPS + national "00"
#   data_county.csv.gz  ACS 5-year + urban/rural allocation, 5-digit FIPS
#   data_pep.csv.gz     Population Estimates Program
#   data_saipe.csv.gz   Small Area Income and Poverty Estimates
#   data_sahie.csv.gz   Small Area Health Insurance Estimates
#   data_oqm.csv.gz     2020 Census Operational Quality Metrics
# The last four carry national, state and county rows in a single file, so the
# build splits them by FIPS length.
#
# Outputs:
#   dist/census_state.parquet   geography(2-digit or "00") x time x measure
#   dist/census_county.parquet  geography(5-digit) x time x measure
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

# -----------------------------------------------------------------------------
# 0. Configuration
# -----------------------------------------------------------------------------

# Which files hold which geography levels. Files listed as "both" carry
# national/state/county together and are split by nchar(geography).
SOURCE_FILES <- c(
  "../census/standard/data_state.csv.gz",
  "../census/standard/data_county.csv.gz",
  "../census/standard/data_pep.csv.gz",
  "../census/standard/data_saipe.csv.gz",
  "../census/standard/data_sahie.csv.gz",
  "../census/standard/data_oqm.csv.gz"
)

missing <- SOURCE_FILES[!file.exists(SOURCE_FILES)]
if (length(missing) > 0) {
  stop(
    "Missing census source files (run the census ingest first):\n",
    paste(" -", missing, collapse = "\n")
  )
}

# Measure prefix -> Census program. Resolved per measure rather than per file
# because data_county.csv.gz carries both acs_* and census_ur_* columns. Keeping
# this a prefix map (not a per-measure list) is what makes the bundle
# self-maintaining: a new acs_* variable needs no change here.
SOURCE_BY_PREFIX <- c(
  acs_       = "Census ACS 5-Year",
  census_ur_ = "Census Urban Areas",
  pep_       = "Census PEP",
  saipe_     = "Census SAIPE",
  sahie_     = "Census SAHIE",
  oqm_       = "Census OQM"
)

# Longest prefix wins, so census_ur_* is not captured by a shorter pattern.
resolve_source <- function(measures) {
  prefixes <- names(SOURCE_BY_PREFIX)
  prefixes <- prefixes[order(nchar(prefixes), decreasing = TRUE)]
  out <- rep(NA_character_, length(measures))
  for (p in prefixes) {
    hit <- is.na(out) & startsWith(measures, p)
    out[hit] <- SOURCE_BY_PREFIX[[p]]
  }
  out
}

# -----------------------------------------------------------------------------
# 1. Read every census standard file, keep one geography level, go tall
# -----------------------------------------------------------------------------

read_all_measures <- function(path, geo_nchar) {
  raw <- vroom(
    path,
    show_col_types = FALSE,
    col_types = cols(geography = col_character(), .default = col_guess())
  )

  measures <- setdiff(colnames(raw), c("geography", "time"))
  if (length(measures) == 0) {
    return(NULL)
  }

  out <- raw %>%
    filter(!is.na(geography), nchar(geography) == geo_nchar) %>%
    select(geography, time, all_of(measures)) %>%
    mutate(time = as.Date(time)) %>%
    pivot_longer(
      cols = all_of(measures),
      names_to = "measure",
      values_to = "value",
      values_transform = as.numeric
    ) %>%
    filter(!is.na(value)) %>%
    mutate(source = resolve_source(measure))

  # A column whose prefix is not in SOURCE_BY_PREFIX would silently get
  # source = NA. Fail loudly instead: it means the census ingest added a
  # variable family and this map needs one new entry.
  unmapped <- sort(unique(out$measure[is.na(out$source)]))
  if (length(unmapped) > 0) {
    stop(
      "No Census program mapped for measure prefix(es) in ", basename(path), ":\n",
      paste(" -", unmapped, collapse = "\n"),
      "\nAdd the prefix to SOURCE_BY_PREFIX in build.R."
    )
  }

  out
}

# No duplicate rows for the given key columns with differing values; drops exact
# duplicates, stops if the same key has conflicting values.
check_dupes <- function(df, label,
                        key_cols = c("geography", "time", "measure", "source")) {
  dupes <- df %>%
    group_by(across(all_of(key_cols))) %>%
    summarize(n = n(), n_distinct_values = n_distinct(value), .groups = "drop") %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    if (any(dupes$n_distinct_values > 1)) {
      stop(
        label, ": ", sum(dupes$n_distinct_values > 1),
        " duplicate ", paste(key_cols, collapse = "-"),
        " rows have differing values — a stratification column may be missing."
      )
    }
    warning(
      label, ": ", nrow(dupes),
      " duplicate rows with identical values; keeping first occurrence."
    )
    df <- df %>%
      group_by(across(all_of(key_cols))) %>%
      slice(1) %>%
      ungroup()
  }
  df
}

# -----------------------------------------------------------------------------
# 2. Assemble. data_state holds state + national ("00", also 2 chars); the
#    remaining files are split by FIPS length.
# -----------------------------------------------------------------------------

census_state <- bind_rows(lapply(
  setdiff(SOURCE_FILES, "../census/standard/data_county.csv.gz"),
  read_all_measures, geo_nchar = 2
)) %>%
  select(geography, time, measure, value, source) %>%
  arrange(measure, geography, time)

census_county <- bind_rows(lapply(
  setdiff(SOURCE_FILES, "../census/standard/data_state.csv.gz"),
  read_all_measures, geo_nchar = 5
)) %>%
  select(geography, time, measure, value, source) %>%
  arrange(measure, geography, time)

census_state <- check_dupes(census_state, "census_state")
census_county <- check_dupes(census_county, "census_county")

# -----------------------------------------------------------------------------
# 3. Write outputs
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)

write_parquet(census_state, "dist/census_state.parquet", compression = "snappy")
write_parquet(census_county, "dist/census_county.parquet", compression = "snappy")

report <- function(df, file) {
  message(sprintf(
    "Wrote %d rows to dist/%s (%d geographies, %d measures, %d programs, %s to %s)",
    nrow(df), file,
    n_distinct(df$geography),
    n_distinct(df$measure),
    n_distinct(df$source),
    format(min(df$time), "%Y"),
    format(max(df$time), "%Y")
  ))
}

report(census_state, "census_state.parquet")
report(census_county, "census_county.parquet")

message(
  "Measures present: ",
  length(union(census_state$measure, census_county$measure)),
  " (source-complete; no allow-list)"
)
