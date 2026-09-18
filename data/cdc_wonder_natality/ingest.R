# =============================================================================
# CDC WONDER Natality Data Ingestion
# Source: CDC WONDER, Natality, 2016-2024 expanded (Single Race), database D149
#         https://wonder.cdc.gov/natality-expanded-current.html
#
# CDC WONDER has no API for sub-national data (its official API is
# national-only by policy), so the raw files under
# raw/natality_expanded_2016_2024/ are produced by the Selenium scraper in the
# sibling repo PopHIVE/wonder-scraper-drowning, which drives the live query
# form. To refresh or extend the pull, from a checkout of that repo:
#
#   caffeinate -i -s scraper/.venv/bin/python scraper/natality_scraper.py \
#       --output-dir ../Ingest/data/cdc_wonder_natality/raw/natality_expanded_2016_2024
#
# and add `--levels national state county` for the county loop (a
# 51-states x 9-years x 4-queries loop with a 20-40s delay per request plus a
# fresh browser session each time -- budget 2-3 days, not hours). Already-
# downloaded files are skipped, so a re-run only fills gaps. This script
# builds standard/data_county.csv.gz only if county files are present, so no
# change here is needed when that pull lands.
#
# Geography: national and state (and county, once that pull exists). National
#            rows are queried from CDC WONDER directly (never summed up from
#            states -- suppressed state cells each hide 1-9 births, so a state
#            sum runs low) and live in the state table under geography "00".
#
# WARNING -- the role names shift on the natality databases:
#   age            is the MOTHER's age (Age of Mother 9: <15, 15-19, ... 50+)
#   sex            is the INFANT's sex
#   race_ethnicity is the MOTHER's single race / Hispanic origin
#
# Eight more one-at-a-time breakdowns (added 2026-09-15, national + state
# only -- see DETAIL_DIMENSIONS below) land in a SEPARATE file,
# standard/data_state_detail.csv.gz, with generic dimension/category columns
# rather than eight more age/sex/race_ethnicity-style columns on
# data_state.csv.gz: with 11 independent breakdowns a wide table would be
# dominated by "Overall" filler on every row. data_state.csv.gz's shape is
# =============================================================================

library(dplyr)
library(purrr)
library(vroom)

RAW_DIR <- "raw/natality_expanded_2016_2024"
YEARS <- 2016:2024

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

# The only missingness marker natality exports carry in a value cell:
# confidentiality suppression, applied to any cell representing 1-9 births.
# The mortality databases' "Not Applicable" / "Not Available" markers cannot
# appear here -- those are denominator problems, and a natality export has no
# Population or rate column at all (Birth Rate and Fertility Rate are separate
# checkboxes on the form that the scrape does not tick).
SUPPRESSED_MARKER <- "Suppressed"

# Geography code for the national rows. WONDER's national exports carry no
# geography columns at all (the query has no location group-by), so the code is
# assigned here rather than read.
NATIONAL_GEOGRAPHY <- "00"

# County natality is only published for counties of 100,000+ people; smaller
# counties in each state are pooled into one "Unidentified Counties, XX" row
# under the pseudo-FIPS code SS999 (CDC's own code). Kept rather than dropped
# -- dropping it would understate rural states -- but SS999 is not a real FIPS
# code, so exclude it when joining to county geography. See the count reported
# at write time and README.md.
UNIDENTIFIED_COUNTY_SUFFIX <- "999"
is_unidentified_county <- function(geography) {
  nchar(geography) == 5 & endsWith(geography, UNIDENTIFIED_COUNTY_SUFFIX)
}

# Suppressed cells are written as NA with natality_suppressed_flag = 1, rather than the
# -999 sentinel the sibling drowning source uses: this source feeds
# bundle_maternal_health, whose pivot coerces with as.numeric() and drops NA,
# so a sentinel would land in the dist parquet as a real value of -999.
to_num <- function(x) suppressWarnings(as.numeric(ifelse(x %in% SUPPRESSED_MARKER, NA, x)))
is_suppressed <- function(x) x %in% SUPPRESSED_MARKER

# Guard against silent data drift. A Births cell that is neither numeric nor
# the known marker would become an unflagged NA; surface it loudly instead.
warn_unknown_values <- function(x, path) {
  unknown <- unique(x[is.na(x) | (is.na(suppressWarnings(as.numeric(x))) & !(x %in% SUPPRESSED_MARKER))])
  if (length(unknown)) {
    warning(
      "Unrecognized value(s) in the Births column of ", path, ": ",
      paste(ifelse(is.na(unknown), "<blank>", unknown), collapse = ", "),
      ". These become unflagged NAs; extend SUPPRESSED_MARKER to classify them.",
      call. = FALSE
    )
  }
}

# "Under 15 years" / "15-19 years" / "50 years and over" -> "<15" / "15-19" / "50+".
# The open-ended top group is matched before the " years" suffix is stripped,
# because it reads "50 years and over" -- the suffix is mid-label, not at the end.
clean_age_label <- function(x) {
  x <- sub("^50 years and over$", "50+", x)
  x <- gsub(" years?$", "", x)
  x <- sub("^Under 15$", "<15", x)
  x
}

clean_race_label <- function(x) sub("^Mother's ", "", x)

clean_ethnicity_label <- function(x) {
  case_when(
    x == "Hispanic or Latino" ~ "Hispanic",
    x == "Not Hispanic or Latino" ~ "Not Hispanic",
    x == UNKNOWN_LABEL ~ "Unknown Hispanic origin",
    TRUE ~ x
  )
}

# WONDER reports its "no usable value" categories as rows alongside the real
# levels of a dimension. They are kept, collapsed into a single "Unknown"
# level, rather than dropped: on the Mother's Hispanic Origin breakdown
# "Unknown or Not Stated" carries real volume (623,865 births over 2016-2024
# -- 35,452 nationally in 2024 alone, 14,348 of them in California), so
# dropping it would silently lose about 1% of births and leave the breakdown
# short of its Overall row. On Mother's Single Race 6 the same labels are
# reported at 0 births in all 468 rows pulled, which is a true reported zero
# and costs nothing to keep.
#
# A dimension can carry more than one of these labels at once (race has all
# three), so collapsing them creates duplicate category keys; read_category
# sums them back into the one "Unknown" row.
PLACEHOLDER_CATEGORIES <- c("Not Stated", "Unknown or Not Stated", "Not Available", "Not Reported")
UNKNOWN_LABEL <- "Unknown"

collapse_placeholders <- function(x) ifelse(x %in% PLACEHOLDER_CATEGORIES, UNKNOWN_LABEL, x)

# Every WONDER export ends with a metadata block -- dataset name, the
# "Query Parameters:" footer the scraper validates against, citation, caveats
# -- introduced by a line containing only "---". Those lines have one field
# where the data has six, so handing the whole file to vroom emits a parsing
# warning per file (~50 per run) that would bury a real one from
# warn_unknown_values(). Cutting the file at the separator keeps the reader
# strict instead: a ragged row above the footer is then a genuine problem.
read_wonder_csv <- function(path) {
  lines <- readLines(path, warn = FALSE)
  sep <- which(trimws(lines) == '"---"')
  if (!length(sep)) {
    stop(path, " has no '---' separator line, so its data section cannot be ",
         "delimited from its footer -- a truncated download, or not a WONDER export.")
  }
  vroom::vroom(
    I(lines[seq_len(sep[1] - 1)]),
    delim = ",", col_types = vroom::cols(.default = "c"), show_col_types = FALSE
  )
}

# Reads one raw natality export (national or state level, one demographic
# breakdown) and standardizes it into long format for a single dimension (age,
# sex, or race_ethnicity), with the other two dimensions set to "Overall".
#
# Column layout is fixed across every state file in this dataset: Notes, geo
# name, geo FIPS code, category label, category code, Births. The national
# files (has_geography = FALSE) are the same minus the two geography columns,
# filled in here as "United States" / NATIONAL_GEOGRAPHY so the rest of the
# function sees one layout.
read_category <- function(path, dim, map_fn, time_val, has_geography = TRUE) {
  df <- read_wonder_csv(path)
  if (!has_geography) {
    df <- df %>%
      mutate(geo_name = "United States", geography = NATIONAL_GEOGRAPHY) %>%
      relocate(geo_name, geography, .after = 1)
  }
  if (ncol(df) != 6) {
    stop(
      path, " has ", ncol(df), " columns, expected 6 ",
      "(Notes, geography, geography code, category, category code, Births). ",
      "The export layout changed, or this is not the query the file name claims."
    )
  }
  names(df) <- c("notes", "geo_name", "geography", "raw_category", "raw_category_code", "births_raw")

  df <- df %>% filter(!is.na(geography), !is.na(raw_category))

  warn_unknown_values(df$births_raw, path)

  out <- df %>%
    transmute(
      geography = geography,
      time = time_val,
      natality_suppressed_flag = as.integer(is_suppressed(births_raw)),
      natality_births = to_num(births_raw),
      category_value = map_fn(collapse_placeholders(trimws(raw_category)))
    ) %>%
    # Collapsing the placeholder labels can leave several rows sharing the
    # "Unknown" level (Mother's Single Race 6 reports three of them), so sum
    # them back to one row per geography/time/level. Real levels are already
    # unique and pass through this untouched. na.rm = FALSE keeps a suppressed
    # component from being silently treated as a zero.
    group_by(geography, time, category_value) %>%
    summarize(
      natality_births = sum(natality_births, na.rm = FALSE),
      natality_suppressed_flag = as.integer(any(natality_suppressed_flag == 1)),
      .groups = "drop"
    ) %>%
    rename(!!dim := category_value)

  for (other_dim in setdiff(c("age", "sex", "race_ethnicity"), dim)) {
    out[[other_dim]] <- "Overall"
  }
  out
}

# Derives an all-demographics "Overall" row per geography/time by summing the
# infant-sex rows -- the only exhaustive split in this data. If any sex cell is
# suppressed the total is left NA rather than fabricated from partial
# information, and the flag is carried up so the reason is not lost. The
# national row is the exception: CDC reports it directly (national_total.csv),
# so build_level does not route it through here.
build_overall_from_sex <- function(sex_df) {
  sex_df %>%
    group_by(geography, time) %>%
    summarize(
      natality_births = sum(natality_births, na.rm = FALSE),
      natality_suppressed_flag = as.integer(any(natality_suppressed_flag == 1)),
      .groups = "drop"
    ) %>%
    mutate(age = "Overall", sex = "Overall", race_ethnicity = "Overall")
}

# Builds the full standardized long table for one geography level ("state" --
# which also carries the national rows -- or "county") across all years and
# all four demographic breakdowns. Returns NULL if the level has no files at
# all, which is how the county table stays absent until that pull is run.
build_level <- function(level) {
  is_state <- level == "state"

  parts <- list()
  national_total_parts <- list()
  found_any <- FALSE

  for (yr in YEARS) {
    time_val <- sprintf("%d-12-31", yr)
    year_dir <- file.path(RAW_DIR, yr)
    if (!dir.exists(year_dir)) next

    if (is_state) {
      files <- list(
        age       = file.path(year_dir, "state_age.csv"),
        sex       = file.path(year_dir, "state_gender.csv"),
        race      = file.path(year_dir, "state_race.csv"),
        ethnicity = file.path(year_dir, "state_ethnicity.csv")
      )
      files <- lapply(files, function(f) f[file.exists(f)])
    } else {
      files <- lapply(
        c(age = "county_age", sex = "county_gender",
          race = "county_race", ethnicity = "county_ethnicity"),
        function(prefix) list.files(year_dir, pattern = sprintf("^%s_.*\\.csv$", prefix), full.names = TRUE)
      )
    }
    if (!any(lengths(files) > 0)) next
    found_any <- TRUE

    read_all <- function(paths, ...) {
      map_dfr(paths, read_category, ..., time_val = time_val, has_geography = TRUE)
    }

    year_rows <- list(
      read_all(files$age, dim = "age", map_fn = clean_age_label),
      read_all(files$sex, dim = "sex", map_fn = identity),
      read_all(files$race, dim = "race_ethnicity", map_fn = clean_race_label),
      read_all(files$ethnicity, dim = "race_ethnicity", map_fn = clean_ethnicity_label)
    )

    if (is_state) {
      # The national breakdowns ride along in the state table. Same readers;
      # only the missing geography columns differ.
      national_read <- function(name, ...) {
        path <- file.path(year_dir, name)
        if (!file.exists(path)) return(NULL)
        read_category(path, ..., time_val = time_val, has_geography = FALSE)
      }
      year_rows <- c(year_rows, list(
        national_read("national_age.csv", dim = "age", map_fn = clean_age_label),
        national_read("national_gender.csv", dim = "sex", map_fn = identity),
        national_read("national_race.csv", dim = "race_ethnicity", map_fn = clean_race_label),
        national_read("national_ethnicity.csv", dim = "race_ethnicity",
                      map_fn = clean_ethnicity_label)
      ))
      # national_total.csv is grouped by Year (WONDER needs at least one
      # group-by), so its single row is the all-demographics total; mapping its
      # category to "Overall" makes it the national Overall row.
      national_total_parts[[as.character(yr)]] <- national_read(
        "national_total.csv", dim = "age", map_fn = function(x) "Overall"
      )
    }

    parts[[as.character(yr)]] <- bind_rows(year_rows)
  }

  if (!found_any) return(NULL)

  all_rows <- bind_rows(parts)
  # Overall rows: summed from the infant sexes for states and counties; taken
  # from CDC's own national query for the national row.
  overall_rows <- bind_rows(
    build_overall_from_sex(
      all_rows %>%
        filter(sex != "Overall", geography != NATIONAL_GEOGRAPHY) %>%
        select(geography, time, natality_births, natality_suppressed_flag)
    ),
    bind_rows(national_total_parts)
  )

  bind_rows(all_rows, overall_rows) %>%
    select(geography, time, age, sex, race_ethnicity, natality_births, natality_suppressed_flag) %>%
    arrange(geography, time, age, sex, race_ethnicity)
}

# -----------------------------------------------------------------------------
# Detail dimensions: eight more one-at-a-time breakdowns, national + state
# only. output dimension name -> raw filename token (matches
# scraper/natality_scraper.py's _DETAIL_VARS and the --extra-query names it
# passes, e.g. state_birthweight.csv / national_birthweight.csv).
# -----------------------------------------------------------------------------
DETAIL_DIMENSIONS <- c(
  birth_weight            = "birthweight",
  gestational_age         = "gestage",
  delivery_method         = "delivery",
  plurality               = "plurality",
  prenatal_care_trimester = "prenatal",
  tobacco_use             = "tobacco",
  mothers_education       = "education",
  marital_status          = "marital"
)

# Mother's Education additionally reports "Excluded" for states whose
# birth-certificate revision does not collect the item at all -- a
# jurisdiction-level gap, not a single record's missingness. It is collapsed
# into the same "Unknown" level as the others here for consistency with every
# other placeholder in this file; see README.md for the distinction. Every
# other detail dimension's real categories are free of this string.
# "No prenatal care" (prenatal_care_trimester) is a real, substantive answer,
# not a placeholder -- it does not match any string here and is kept as-is.
DETAIL_PLACEHOLDER_CATEGORIES <- c(PLACEHOLDER_CATEGORIES, "Excluded")

# Reads one detail-dimension export (national or state) into
# geography/time/dimension/category/natality_births/natality_suppressed_flag. Column
# layout is the same fixed 6-column shape as read_category() above; kept as
# its own function because the output has generic dimension/category columns
# instead of read_category()'s three fixed demographic columns.
read_detail_category <- function(path, dimension, time_val, has_geography = TRUE) {
  df <- read_wonder_csv(path)
  if (!has_geography) {
    df <- df %>%
      mutate(geo_name = "United States", geography = NATIONAL_GEOGRAPHY) %>%
      relocate(geo_name, geography, .after = 1)
  }
  if (ncol(df) != 6) {
    stop(
      path, " has ", ncol(df), " columns, expected 6 ",
      "(Notes, geography, geography code, category, category code, Births). ",
      "The export layout changed, or this is not the query the file name claims."
    )
  }
  names(df) <- c("notes", "geo_name", "geography", "raw_category", "raw_category_code", "births_raw")
  df <- df %>% filter(!is.na(geography), !is.na(raw_category))
  warn_unknown_values(df$births_raw, path)

  df %>%
    transmute(
      geography = geography,
      time = time_val,
      dimension = dimension,
      category = ifelse(trimws(raw_category) %in% DETAIL_PLACEHOLDER_CATEGORIES,
                        UNKNOWN_LABEL, trimws(raw_category)),
      natality_suppressed_flag = as.integer(is_suppressed(births_raw)),
      natality_births = to_num(births_raw)
    ) %>%
    # Same reason as read_category(): collapsing placeholders can leave
    # several rows sharing "Unknown" for one geography/time/dimension.
    group_by(geography, time, dimension, category) %>%
    summarize(
      natality_births = sum(natality_births, na.rm = FALSE),
      natality_suppressed_flag = as.integer(any(natality_suppressed_flag == 1)),
      .groups = "drop"
    )
}

# Builds the full standardized detail table across all years and all eight
# dimensions, national + state. Returns NULL if none of the detail files
# exist yet, matching build_level()'s NULL-if-absent convention for county.
build_detail_level <- function() {
  parts <- list()
  found_any <- FALSE

  for (yr in YEARS) {
    time_val <- sprintf("%d-12-31", yr)
    year_dir <- file.path(RAW_DIR, yr)
    if (!dir.exists(year_dir)) next

    year_rows <- list()
    for (dim_name in names(DETAIL_DIMENSIONS)) {
      token <- DETAIL_DIMENSIONS[[dim_name]]
      state_path <- file.path(year_dir, paste0("state_", token, ".csv"))
      national_path <- file.path(year_dir, paste0("national_", token, ".csv"))
      if (file.exists(state_path)) {
        found_any <- TRUE
        year_rows[[length(year_rows) + 1]] <-
          read_detail_category(state_path, dim_name, time_val, has_geography = TRUE)
      }
      if (file.exists(national_path)) {
        found_any <- TRUE
        year_rows[[length(year_rows) + 1]] <-
          read_detail_category(national_path, dim_name, time_val, has_geography = FALSE)
      }
    }
    parts[[as.character(yr)]] <- bind_rows(year_rows)
  }

  if (!found_any) return(NULL)

  bind_rows(parts) %>%
    select(geography, time, dimension, category, natality_births, natality_suppressed_flag) %>%
    arrange(dimension, geography, time, category)
}

# -----------------------------------------------------------------------------
# 1. Initialize process record and check for changes
# -----------------------------------------------------------------------------
process <- if (!file.exists("process.json")) list(raw_state = NULL) else dcf::dcf_process_record()

raw_files <- sort(list.files(RAW_DIR, recursive = TRUE, full.names = TRUE, pattern = "\\.csv$"))
if (!length(raw_files)) {
  stop(
    "no raw CSV files under ", RAW_DIR, ". Run the scraper first -- see the ",
    "header of this file for the command."
  )
}
raw_state <- list(hash = tools::md5sum(raw_files))

if (!identical(process$raw_state, raw_state)) {
  # ---------------------------------------------------------------------------
  # 2. Build standardized tables
  # ---------------------------------------------------------------------------
  data_state <- build_level("state")
  data_county <- build_level("county")

  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_state, "standard/data_state.csv.gz", delim = ",")
  message("cdc_wonder_natality: data_state.csv.gz — ", nrow(data_state), " rows, ",
          n_distinct(data_state$geography), " geographies, ",
          min(data_state$time), " to ", max(data_state$time))

  if (!is.null(data_county)) {
    vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")
    # Report the pooled small-county rows rather than letting them pass as
    # ordinary counties: SS999 is not a joinable FIPS code, and the share of
    # births sitting in it is the single most important caveat on this file.
    unid <- data_county %>% filter(is_unidentified_county(geography))
    overall <- data_county %>%
      filter(age == "Overall", sex == "Overall", race_ethnicity == "Overall")
    unid_share <- 100 * sum(overall$natality_births[is_unidentified_county(overall$geography)],
                            na.rm = TRUE) / sum(overall$natality_births, na.rm = TRUE)
    message("cdc_wonder_natality: data_county.csv.gz — ", nrow(data_county), " rows, ",
            n_distinct(data_county$geography), " geographies (of which ",
            n_distinct(unid$geography), " are pooled \"Unidentified Counties\" SS999 codes, ",
            sprintf("%.1f%%", unid_share), " of all county births — not real FIPS, ",
            "exclude when joining to county geography)")
  } else {
    message("cdc_wonder_natality: no county files under ", RAW_DIR,
            "; skipping data_county.csv.gz (see the header for the county pull command)")
  }

  data_state_detail <- build_detail_level()
  if (!is.null(data_state_detail)) {
    vroom::vroom_write(data_state_detail, "standard/data_state_detail.csv.gz", delim = ",")
    message("cdc_wonder_natality: data_state_detail.csv.gz — ", nrow(data_state_detail), " rows, ",
            n_distinct(data_state_detail$dimension), " dimensions (",
            paste(sort(unique(data_state_detail$dimension)), collapse = ", "), "), ",
            n_distinct(data_state_detail$geography), " geographies")
  } else {
    message("cdc_wonder_natality: no detail-dimension files under ", RAW_DIR,
            "; skipping data_state_detail.csv.gz")
  }

  # ---------------------------------------------------------------------------
  # 4. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
