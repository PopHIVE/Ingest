# =============================================================================
# Epic Cosmos Mental Health ED Length-of-Stay Ingestion
# Source: Epic Cosmos SlicerDicer exports
#         (raw/staging_median_pct/*.xlsx, raw/staging_iqr/*.xlsx)
#
# Two ED-diagnosis buckets, each exported as a pair of crosstab sessions
# (same row-dimension grid, different measure columns):
#   Data model      : ED Encounters
#   Population base : All ED Encounters
#   Criteria        : Country of Residence = United States of America
#   ED Diagnoses    : "Suicidal behavior" (sessions 2852625, 2852630;
#                                            7/1/2022-6/30/2026)
#                      "Mood"              (sessions 2852830, 2852828;
#                                            1/1/2022-6/30/2026 - the third
#                                            re-export of this diagnosis; the
#                                            first two, 2852663/2852656 and
#                                            2852768/2852738, had a corrupted
#                                            age-bucket label and/or a
#                                            shorter date range)
#   raw/staging_median_pct/ : Median ED Length of Stay + Percentage of Sliced
#                              Population, by Year/Month/State of
#                              Residence/Age at Time of Visit Range
#   raw/staging_iqr/        : Q1 and Q3 ED length of stay, same row dimensions
#
# The "Behavioral" ED-diagnosis bucket (session 2852635, no age breakdown) was
# intentionally excluded from this ingest per user direction - if it is added
# later, it needs its own median/pct and Q1/Q3 exports with an age breakdown
# to match the other two buckets.
#
# Output (PopHIVE wide format, standard/data.csv.gz):
#   index   : geography (FIPS string, "00" = national), time, age
#   measures, per diagnosis bucket (suicidal_behavior, mood):
#     epic_median_ed_los_<dx>          (minutes)
#     epic_pct_sliced_population_<dx>  (%, Epic's own "Percentage of Sliced
#                                        Population" measure - see the caveat
#                                        below; this is NOT a prevalence rate)
#     epic_q1_ed_los_<dx>              (minutes)
#     epic_q3_ed_los_<dx>              (minutes)
#   each immediately followed by its own <measure>_suppressed_flag
#
# Conventions applied here:
#   - time is the LAST day of the month, formatted YYYY-mm-dd
#   - age uses Epic's own "Age at Time of Visit" buckets, standardized to
#     inclusive ranges (see standardize_age()), plus "Overall" from the Total
#     bucket. The "No value" (unclassified) age bucket is dropped.
#   - the "Total" ED Diagnoses bucket (all diagnoses combined) is dropped: it
#     is redundant across the two diagnosis-pair exports and, since the two
#     pairs cover different session date ranges and age-bucket boundaries,
#     keeping it would create two inconsistent "Overall" series rather than
#     one canonical baseline
#   - a defensive guard drops any row with the age label "Years or more and
#     less than N Years" (missing its leading bound number) rather than
#     guessing at the intended bound. The original Mood exports (sessions
#     2852663/2852656) had this corruption; the replacement exports above
#     fixed it, so this guard is not expected to trigger, but is kept in case
#     a future re-export regresses
#   - suppression is handled per measure (blank -> flag 1). Unlike a simple
#     count, none of these four measures (a median/Q1/Q3 duration statistic,
#     or a percentage with no denominator count exported alongside it) can be
#     imputed from a denominator, so a suppressed cell is left NA rather than
#     filled with 5 - the same treatment as a mean-valued measure. The one
#     exception is a bounded percentage ("<0.01%"), which is a reported (if
#     imprecise) value: it is imputed at half the bound and still flagged 1,
#     since it reflects the same underlying small-cell suppression
#
# IMPORTANT - what "Percentage of Sliced Population" actually means here:
#   Verified empirically: for a fixed month, epic_pct_sliced_population_<dx>
#   summed across every age bucket at geography "00" is ~100%, and summed
#   across every state at age "Overall" is also ~100%. That is only possible
#   if the value is (this cell's encounter count) / (that diagnosis's total
#   US encounter count that month, across all states and ages) x 100 - i.e.
#   a compositional share of the diagnosis's own population (which age
#   groups / which states it shows up in), NOT the share of all ED
#   encounters that carry this diagnosis (unlike epic_pct_concussion in
#   cosmos_concussions, which IS a prevalence rate). A populous state will
#   have a large share here simply from volume, independent of its rate of
#   this diagnosis. Documented in measure_info.json and README - do not
#   treat this measure as a rate.
# =============================================================================

library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(openxlsx2)
library(zoo)

# Check that msoffcrypto-tool is available (required for decrypting password-protected xlsx files)
msoffcrypto_check <- system(
  paste(Sys.which("python"), "-c \"import msoffcrypto\""),
  ignore.stdout = TRUE, ignore.stderr = TRUE
)
if (msoffcrypto_check != 0) {
  stop(
    "Python package 'msoffcrypto-tool' is required but not installed.\n",
    "Install it with: python -m pip install msoffcrypto-tool"
  )
}

# Load FIPS lookup
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# Password for xlsx files (set in .Renviron via usethis::edit_r_environ())
xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# =============================================================================
# 1. Locate staging files & detect change
# =============================================================================

staging_files <- c(
  list.files("raw/staging_median_pct", pattern = "\\.(csv|xlsx)$", full.names = TRUE),
  list.files("raw/staging_iqr", pattern = "\\.(csv|xlsx)$", full.names = TRUE)
)
if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging_median_pct/ or raw/staging_iqr/.\n",
    "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
  )
}

current_state <- list(
  files = staging_files,
  hashes = unname(tools::md5sum(staging_files))
)

if (!identical(process$raw_state, current_state)) {

  # ---------------------------------------------------------------------------
  # 2. Decrypt and read the raw grid
  # ---------------------------------------------------------------------------
  read_epic_grid <- function(file, password) {
    if (!grepl("\\.xlsx$", file, ignore.case = TRUE)) {
      stop("Expected a password-protected .xlsx SlicerDicer export, got: ", file)
    }
    if (is.null(password) || !nzchar(password)) {
      stop(
        "EPIC_XLSX_PASSWORD is not set, but ", basename(file), " is a ",
        "password-protected SlicerDicer export.\n",
        "Set it in .Renviron via usethis::edit_r_environ()."
      )
    }
    decrypted <- tempfile(fileext = ".xlsx")
    cmd <- sprintf(
      'python -m msoffcrypto -p "%s" "%s" "%s"',
      password, normalizePath(file, winslash = "/"), decrypted
    )
    if (system(cmd) != 0) stop("Decryption failed: ", file)
    on.exit(unlink(decrypted))

    wb <- wb_load(decrypted)
    grid <- wb_to_df(
      wb, sheet = 1, col_names = FALSE,
      skip_empty_rows = FALSE, skip_empty_cols = FALSE
    )
    as.data.frame(lapply(grid, as.character), stringsAsFactors = FALSE)
  }

  # ---------------------------------------------------------------------------
  # 3. Locate the row-dimension header row (Year/Month/State of
  #    Residence/Age at Time of Visit - order varies between exports, so
  #    columns are located by name, not by fixed position)
  # ---------------------------------------------------------------------------
  find_dim_row <- function(grid) {
    expected <- c("Year", "Month", "State of Residence", "Age at Time of Visit")
    for (r in seq_len(min(20, nrow(grid)))) {
      if (setequal(trimws(as.character(grid[r, 1:4])), expected)) return(r)
    }
    stop("Could not find the Year/Month/State of Residence/Age at Time of Visit header row; export layout changed.")
  }

  # ---------------------------------------------------------------------------
  # 4. Parse one crosstab export into long format:
  #    state_name, year, month, age_raw, diagnosis, measure, raw_value
  # ---------------------------------------------------------------------------
  process_los_crosstab <- function(file, password) {
    grid <- read_epic_grid(file, password)
    dim_row <- find_dim_row(grid)
    dim_labels <- trimws(as.character(grid[dim_row, 1:4]))
    col_of <- function(name) which(dim_labels == name)
    year_col <- col_of("Year")
    month_col <- col_of("Month")
    state_col <- col_of("State of Residence")
    age_col <- col_of("Age at Time of Visit")
    if (any(lengths(list(year_col, month_col, state_col, age_col)) != 1)) {
      stop("Could not uniquely locate row-dimension columns in ", basename(file))
    }

    measure_row <- dim_row - 1L
    diag_row <- dim_row - 2L
    data_start <- dim_row + 1L
    value_col_idx <- 5:ncol(grid)

    # --- ED Diagnoses column-group header: filled only at the first column
    #     of each span, so fill right across the gaps ---
    diag_raw <- trimws(as.character(grid[diag_row, value_col_idx]))
    diag_raw[diag_raw == ""] <- NA
    diag_raw <- zoo::na.locf(diag_raw, na.rm = FALSE)
    if (any(is.na(diag_raw))) {
      stop("ED Diagnoses column-group header has unfillable gaps in ", basename(file))
    }
    diag_std <- ifelse(grepl("^Total", diag_raw), "Total", diag_raw)
    unrecognized_dx <- setdiff(unique(diag_std), c("Suicidal behavior", "Mood", "Total"))
    if (length(unrecognized_dx) > 0) {
      stop(
        "Unrecognized ED Diagnoses label(s) in ", basename(file), ": ",
        paste(unrecognized_dx, collapse = ", "),
        "\nUpdate the allowed diagnosis list in ingest.R if this bucket is expected."
      )
    }

    # --- Measure header: each value column carries its own explicit label ---
    measure_raw <- trimws(as.character(grid[measure_row, value_col_idx]))
    measure_std <- case_when(
      grepl("^Median.*Length of Stay", measure_raw, ignore.case = TRUE) ~ "epic_median_ed_los",
      grepl("^Percentage of Sliced Population", measure_raw, ignore.case = TRUE) ~ "epic_pct_sliced_population",
      grepl("^Q3.*Length of Stay", measure_raw, ignore.case = TRUE) ~ "epic_q3_ed_los",
      grepl("^Q1.*Length of Stay", measure_raw, ignore.case = TRUE) ~ "epic_q1_ed_los",
      TRUE ~ NA_character_
    )
    if (any(is.na(measure_std))) {
      stop(
        "Unrecognized measure label(s) in ", basename(file), ": ",
        paste(unique(measure_raw[is.na(measure_std)]), collapse = ", "),
        "\nUpdate the measure regex map in ingest.R for the new label wording."
      )
    }

    col_meta <- data.frame(
      col_idx = value_col_idx, diagnosis = diag_std, measure = measure_std,
      stringsAsFactors = FALSE
    )

    # --- Data rows: fill down merged state/year/month cells (age is present
    #     on every row, so it is not filled) ---
    data_raw <- grid[data_start:nrow(grid), , drop = FALSE]
    colnames(data_raw)[year_col] <- "year"
    colnames(data_raw)[month_col] <- "month"
    colnames(data_raw)[state_col] <- "state_name"
    colnames(data_raw)[age_col] <- "age_raw"
    colnames(data_raw)[value_col_idx] <- as.character(value_col_idx)

    data_raw <- data_raw %>%
      mutate(
        state_name = na_if(trimws(iconv(state_name, to = "UTF-8", sub = "")), ""),
        year = na_if(trimws(year), ""),
        month = na_if(trimws(iconv(month, to = "UTF-8", sub = "")), ""),
        age_raw = trimws(iconv(age_raw, to = "UTF-8", sub = ""))
      ) %>%
      fill(state_name, year, month, .direction = "down")

    data_raw %>%
      select(state_name, year, month, age_raw, all_of(as.character(value_col_idx))) %>%
      pivot_longer(cols = all_of(as.character(value_col_idx)), names_to = "col_idx", values_to = "raw_value") %>%
      mutate(col_idx = as.integer(col_idx)) %>%
      left_join(col_meta, by = "col_idx") %>%
      select(-col_idx) %>%
      filter(diagnosis != "Total")  # drop the all-diagnoses baseline (see header note)
  }

  process_staging_folder <- function(dir, password) {
    files <- list.files(dir, pattern = "\\.(csv|xlsx)$", full.names = TRUE)
    if (length(files) == 0) stop("No staging files found in ", dir)
    bind_rows(lapply(files, process_los_crosstab, password = password))
  }

  all_long <- bind_rows(
    process_staging_folder("raw/staging_median_pct", xlsx_password),
    process_staging_folder("raw/staging_iqr", xlsx_password)
  )

  # ---------------------------------------------------------------------------
  # 5. Age: drop the corrupted / unclassified labels, then standardize
  # ---------------------------------------------------------------------------
  is_malformed_age <- grepl("^Years or more and less than \\d+ Years$", all_long$age_raw)
  n_malformed <- sum(is_malformed_age)
  if (n_malformed > 0) {
    message(
      "Dropping ", n_malformed, " row(s) with a malformed 'Age at Time of Visit' label ",
      "(missing leading bound number): '",
      paste(unique(all_long$age_raw[is_malformed_age]), collapse = "; "), "'"
    )
  }
  all_long <- all_long[!is_malformed_age, ]

  is_no_value_age <- all_long$age_raw == "No value"
  n_no_value <- sum(is_no_value_age)
  if (n_no_value > 0) {
    message("Dropping ", n_no_value, " row(s) with age 'No value' (unclassified age at time of visit)")
  }
  all_long <- all_long[!is_no_value_age, ]

  standardize_age <- function(age) {
    age <- trimws(age)
    age <- str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years")
    age <- str_replace(age, "^(\\d+) Years or more$", "\\1+ Years")
    # This session mixes two phrasings for the same bound-pair concept:
    # "≥ X and < Y Years" and "X Years or more and less than Y Years".
    age <- str_replace(age, "^(\\d+)\\s+Years or more and less than\\s+(\\d+)\\s+Years$",
                        "≥ \\1 and < \\2 Years")
    m <- str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
    lower <- m[, 2]
    upper <- as.character(as.integer(m[, 3]) - 1L)
    age <- ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
    ifelse(grepl("^Total", age), "Overall", age)
  }
  age_std <- standardize_age(all_long$age_raw)
  if (any(is.na(age_std) | age_std == "")) {
    bad <- unique(all_long$age_raw[is.na(age_std) | age_std == ""])
    stop("Unrecognized 'Age at Time of Visit' label(s): ", paste(bad, collapse = ", "),
         "\nUpdate standardize_age() for the new label format.")
  }
  all_long$age <- age_std

  # ---------------------------------------------------------------------------
  # 6. Time: last day of the month
  # ---------------------------------------------------------------------------
  all_long$time <- format(
    ceiling_date(as.Date(paste(all_long$year, all_long$month, "01"), format = "%Y %b %d"), "month") - days(1),
    "%Y-%m-%d"
  )
  if (any(is.na(all_long$time))) {
    stop("Failed to parse year/month into a date for some row(s); check the Year/Month label format.")
  }

  # ---------------------------------------------------------------------------
  # 7. Geography: state name -> FIPS
  # ---------------------------------------------------------------------------
  valid_states <- c(state.name, "District of Columbia")
  n_before_geo <- nrow(all_long)

  all_long <- all_long %>%
    mutate(
      geography_name = case_when(
        grepl("^Total", state_name) ~ "United States",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      )
    )
  dropped_states <- setdiff(unique(all_long$state_name[is.na(all_long$geography_name)]), NA_character_)

  all_long <- all_long %>%
    filter(!is.na(geography_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography))

  if (length(dropped_states) > 0) {
    message(
      "Dropped ", n_before_geo - nrow(all_long), " non-US / catch-all row(s) (state of residence: ",
      paste(sort(dropped_states), collapse = ", "), ")"
    )
  }

  # ---------------------------------------------------------------------------
  # 8. Fold diagnosis into the measure column name (suicidal_behavior / mood)
  # ---------------------------------------------------------------------------
  dx_suffix <- case_when(
    all_long$diagnosis == "Suicidal behavior" ~ "suicidal_behavior",
    all_long$diagnosis == "Mood" ~ "mood"
  )
  all_long$measure_col <- paste0(all_long$measure, "_", dx_suffix)

  dupes <- all_long %>% count(geography, time, age, measure_col) %>% filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate rows per geography/time/age/measure (", nrow(dupes), " combinations). ",
         "Check for overlapping staging files.")
  }

  # ---------------------------------------------------------------------------
  # 9. Suppression + numeric parsing
  #    Median/Q1/Q3 length of stay have no denominator to impute a suppressed
  #    cell from, so they are treated like a mean-valued measure: NA, flag 1.
  #    Percentage of Sliced Population is a percent, but this crosstab does
  #    not export a paired patient-count column either, so a blank cell is
  #    likewise left NA; a bounded cell ("<0.01%") is a reported (if
  #    imprecise) value and is imputed at half the bound, still flagged 1.
  # ---------------------------------------------------------------------------
  is_pct <- grepl("^epic_pct_sliced_population", all_long$measure)
  raw_trim <- trimws(all_long$raw_value)
  is_blank <- is.na(raw_trim) | raw_trim == "" | raw_trim == "-"
  is_bounded <- is_pct & grepl("^<", raw_trim)

  all_long <- all_long %>%
    mutate(
      suppressed = as.integer(is_blank | is_bounded),
      value = case_when(
        is_blank ~ NA_real_,
        is_bounded ~ suppressWarnings(as.numeric(gsub("[<%]", "", raw_trim))) / 2,
        is_pct ~ suppressWarnings(as.numeric(gsub("%", "", raw_trim))),
        TRUE ~ suppressWarnings(as.numeric(gsub(",", "", raw_trim)))
      )
    )

  # ---------------------------------------------------------------------------
  # 10. Pivot wide: one column per measure x diagnosis, flag immediately after
  # ---------------------------------------------------------------------------
  value_wide <- all_long %>%
    select(geography, time, age, measure_col, value) %>%
    pivot_wider(names_from = measure_col, values_from = value)

  flag_wide <- all_long %>%
    select(geography, time, age, measure_col, suppressed) %>%
    mutate(measure_col = paste0(measure_col, "_suppressed_flag")) %>%
    pivot_wider(names_from = measure_col, values_from = suppressed)

  measure_cols <- c(
    "epic_median_ed_los_suicidal_behavior", "epic_pct_sliced_population_suicidal_behavior",
    "epic_q1_ed_los_suicidal_behavior", "epic_q3_ed_los_suicidal_behavior",
    "epic_median_ed_los_mood", "epic_pct_sliced_population_mood",
    "epic_q1_ed_los_mood", "epic_q3_ed_los_mood"
  )
  ordered_cols <- as.vector(rbind(measure_cols, paste0(measure_cols, "_suppressed_flag")))

  data_clean <- value_wide %>%
    full_join(flag_wide, by = c("geography", "time", "age")) %>%
    select(geography, time, age, all_of(ordered_cols)) %>%
    arrange(geography, age, time)

  # ---------------------------------------------------------------------------
  # 11. Validate
  # ---------------------------------------------------------------------------
  dupes2 <- data_clean %>% count(geography, time, age) %>% filter(n > 1)
  if (nrow(dupes2) > 0) {
    stop("Duplicate rows per geography/time/age (", nrow(dupes2), " combinations) after pivoting.")
  }

  stopifnot(
    all(nchar(data_clean$geography) == 2),
    "00" %in% data_clean$geography,
    all(grepl("^\\d{4}-\\d{2}-\\d{2}$", data_clean$time)),
    all(as.Date(data_clean$time) == ceiling_date(as.Date(data_clean$time), "month") - days(1))
  )

  for (mc in measure_cols) {
    fl <- paste0(mc, "_suppressed_flag")
    val <- data_clean[[mc]]
    flag <- data_clean[[fl]]
    stopifnot(
      all(flag %in% c(0L, 1L, NA)),
      # a flag of 1 never sits on a source-reported value: every flagged cell
      # is either NA (blank, unimputable) or the bounded-percentage half value
      all(is.na(val[flag == 1L]) | (grepl("pct_sliced_population", mc) & val[flag == 1L] >= 0 & val[flag == 1L] < 0.01)),
      all(!is.na(val[flag == 0L & !is.na(flag)]))
    )
    if (grepl("pct_sliced_population", mc)) {
      stopifnot(all(val >= 0 & val <= 100, na.rm = TRUE))
    } else {
      stopifnot(all(val >= 0, na.rm = TRUE))
    }
  }

  message(
    "Standardized ", nrow(data_clean), " rows | ",
    length(unique(data_clean$geography)), " geographies | ",
    min(data_clean$time), " to ", max(data_clean$time)
  )
  for (mc in measure_cols) {
    fl <- paste0(mc, "_suppressed_flag")
    message(
      "  ", fl, ": ", sum(data_clean[[fl]], na.rm = TRUE), " suppressed/imputed, ",
      sum(is.na(data_clean[[fl]])), " cell(s) absent from the source export"
    )
  }

  # ---------------------------------------------------------------------------
  # 12. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")
  vroom::vroom_write(data_clean, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 13. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
