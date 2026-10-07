# =============================================================================
# Epic Cosmos Concussion Data Ingestion
# Source: Epic Cosmos SlicerDicer export (raw/staging/*.xlsx)
#         Monthly ED encounter counts with a concussion diagnosis, by state,
#         age at time of visit, and evaluated sex
#
# SlicerDicer session 2851311, "Number of ED Encounters by Age at Time of
# Visit Range and State of Residence and ED Diagnoses and Evaluated Sex":
#   Data model      : ED Encounters
#   Population base : All ED Encounters
#   Criteria        : Country of Residence = United States of America
#   Measure         : Number of ED Encounters
#   ED Diagnoses    : "Concussion( ICD-10-CA: S06.0 )" (numerator) and
#                      "Total" (denominator - all ED encounters, any diagnosis)
#
# Raw export layout (rows, 1-indexed as in the spreadsheet):
#   1-9   : session metadata
#   12    : "Age at Time of Visit" column-group header (cols E:end, filled right)
#   13    : "Evaluated Sex" sub-header (Female / Male / Ambiguous / Total per age group)
#   14    : row-dimension labels (A: State of Residence, B: ED Diagnoses,
#           C: Year, D: Month)
#   15+   : data rows, stacked as 52 state blocks of 108 rows each:
#             - rows 1-54 of a block: ED Diagnoses = Concussion, one row per
#               month (Jan 2022 - Jun 2026)
#             - rows 55-108: ED Diagnoses = Total, same 54 months
#           State, diagnosis, and year are merged cells - blank until the next
#           value, so they must be filled down.
#
# Output (PopHIVE wide format, standard/data.csv.gz):
#   index   : geography (FIPS string, "00" = national), time, age, sex
#   measures: epic_n_concussion, epic_pct_concussion
#               -> epic_n_concussion_suppressed_flag (covers both; the percent
#                  is derived from the same numerator cell)
#             epic_n_ed_encounters (denominator: all ED encounters)
#               -> epic_n_ed_encounters_suppressed_flag
#
# Conventions applied here:
#   - time is the LAST day of the month, formatted YYYY-mm-dd; the trailing
#     partial period at the end of the exported range (e.g. "Jun 1 - Jun 22")
#     is dropped
#   - age uses Epic's own "Age at Time of Visit" buckets, standardized to
#     inclusive ranges, plus "Overall" from the age-dimension's Total bucket
#   - sex is Female / Male / "Overall" (from the sex-dimension's Total
#     bucket). The "Ambiguous" evaluated-sex bucket is dropped: it is not one
#     of the standard sex categories, and its counts are already folded into
#     "Overall" via Epic's own Total column, so no data is lost in aggregate
#   - suppression is handled per measure (blank / "10 or fewer" -> 5, flag 1)
#   - the concussion measure is a PERCENT of all ED encounters, not a rate
#     per 100,000
# =============================================================================

library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(openxlsx2)

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

staging_files <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)
if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
  )
}
if (length(staging_files) > 1) {
  stop(
    "Multiple staging files found (", paste(basename(staging_files), collapse = ", "), ").\n",
    "This ingest expects a single ED Encounters crosstab export. Remove extras ",
    "from raw/staging/, or extend ingest.R to combine multiple exports."
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

  grid <- read_epic_grid(staging_files[[1]], xlsx_password)

  # ---------------------------------------------------------------------------
  # 3. Locate and validate header rows (fails loudly if the session drifts)
  # ---------------------------------------------------------------------------
  dim_label_row <- which(grid[[1]] == "State of Residence")
  if (length(dim_label_row) != 1) {
    stop("Could not find exactly one 'State of Residence' row-label row; export layout changed.")
  }
  if (!identical(grid[[2]][[dim_label_row]], "ED Diagnoses")) {
    stop("Expected column B row-dimension label 'ED Diagnoses' on row ", dim_label_row,
         "; export layout changed.")
  }
  if (!identical(grid[[3]][[dim_label_row]], "Year") || !identical(grid[[4]][[dim_label_row]], "Month")) {
    stop("Expected columns C/D row-dimension labels 'Year'/'Month' on row ", dim_label_row,
         "; export layout changed.")
  }

  age_row <- dim_label_row - 2L
  sex_row <- dim_label_row - 1L
  data_start <- dim_label_row + 1L

  n_cols <- ncol(grid)
  if (n_cols < 5) stop("Unexpected export width (", n_cols, " columns); expected state/diagnosis/year/month plus value columns.")

  value_col_idx <- 5:n_cols

  # --- Age group header (row `age_row`): filled only at the first column of
  #     each 4-column span, so fill right across the gaps ---
  age_group_raw <- as.character(grid[age_row, value_col_idx])
  age_group_raw[age_group_raw == ""] <- NA
  age_group_raw <- zoo::na.locf(age_group_raw, na.rm = FALSE)
  if (any(is.na(age_group_raw))) {
    stop("Age group header ('Age at Time of Visit') has unfillable gaps; export layout changed.")
  }

  standardize_age <- function(age) {
    age <- trimws(age)
    age <- str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years")
    age <- str_replace(age, "^(\\d+) Years or more$", "\\1+ Years")
    # This session mixes two phrasings for the same bound-pair concept:
    # "≥ X and < Y Years" and "X Years or more and less than Y Years".
    # Normalize the worded form to the symbolic one before extracting bounds.
    age <- str_replace(age, "^(\\d+)\\s+Years or more and less than\\s+(\\d+)\\s+Years$",
                        "≥ \\1 and < \\2 Years")
    m <- str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
    lower <- m[, 2]
    upper <- as.character(as.integer(m[, 3]) - 1L)
    age <- ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
    ifelse(grepl("^Total", age), "Overall", age)
  }
  age_std <- standardize_age(age_group_raw)
  if (any(is.na(age_std) | age_std == "")) {
    bad <- unique(age_group_raw[is.na(age_std) | age_std == ""])
    stop("Unrecognized 'Age at Time of Visit' column label(s): ", paste(bad, collapse = ", "),
         "\nUpdate standardize_age() for the new label format.")
  }

  # --- Sex sub-header (row `sex_row`): Female / Male / Ambiguous / Total, repeats per age group ---
  sex_raw <- trimws(as.character(grid[sex_row, value_col_idx]))
  sex_std <- case_when(
    sex_raw == "Female" ~ "Female",
    sex_raw == "Male" ~ "Male",
    sex_raw == "Ambiguous" ~ "Ambiguous",
    grepl("^Total", sex_raw) ~ "Overall",
    TRUE ~ NA_character_
  )
  if (any(is.na(sex_std))) {
    stop("Unrecognized 'Evaluated Sex' column label(s): ",
         paste(unique(sex_raw[is.na(sex_std)]), collapse = ", "))
  }

  col_meta <- data.frame(
    col_idx = value_col_idx,
    age = age_std,
    sex = sex_std,
    stringsAsFactors = FALSE
  )

  # ---------------------------------------------------------------------------
  # 4. Data rows: fill down merged state/diagnosis/year cells
  # ---------------------------------------------------------------------------
  data_raw <- grid[data_start:nrow(grid), , drop = FALSE]
  colnames(data_raw)[1:4] <- c("state_name", "diagnosis", "year", "month")
  colnames(data_raw)[value_col_idx] <- as.character(value_col_idx)

  data_raw <- data_raw %>%
    mutate(
      state_name = iconv(state_name, to = "UTF-8", sub = ""),
      diagnosis  = iconv(diagnosis,  to = "UTF-8", sub = ""),
      month      = iconv(month,      to = "UTF-8", sub = ""),
      state_name = na_if(trimws(state_name), ""),
      diagnosis  = na_if(trimws(diagnosis), ""),
      year       = na_if(trimws(year), ""),
      month      = na_if(trimws(month), "")
    ) %>%
    fill(state_name, diagnosis, year, .direction = "down") %>%
    mutate(
      diagnosis = case_when(
        grepl("^Total", diagnosis) ~ "Total",
        grepl("^Concussion", diagnosis) ~ "Concussion",
        TRUE ~ diagnosis
      )
    )

  unrecognized_dx <- setdiff(unique(data_raw$diagnosis), c("Concussion", "Total"))
  if (length(unrecognized_dx) > 0) {
    stop("Unrecognized ED Diagnoses value(s): ", paste(unrecognized_dx, collapse = ", "))
  }

  # --- Drop the trailing partial period (e.g. "Jun 1 - Jun 22") ---
  is_full_month <- grepl("^[A-Za-z]{3}$", trimws(data_raw$month))
  n_partial <- sum(!is_full_month)
  if (n_partial > 0) {
    message(
      "Dropping ", n_partial, " row(s) from partial period(s): ",
      paste(unique(data_raw$month[!is_full_month]), collapse = ", ")
    )
    if (n_partial == nrow(data_raw)) {
      stop("Every row was classified as a partial period - the month label format probably changed.")
    }
  }
  data_raw <- data_raw[is_full_month, ]

  data_raw$time <- format(
    ceiling_date(as.Date(paste(data_raw$year, data_raw$month, "01"), format = "%Y %b %d"), "month") - days(1),
    "%Y-%m-%d"
  )

  # ---------------------------------------------------------------------------
  # 5. Geography: state name -> FIPS
  # ---------------------------------------------------------------------------
  valid_states <- c(state.name, "District of Columbia")
  n_before_geo <- nrow(data_raw)

  data_raw <- data_raw %>%
    mutate(
      geography_name = case_when(
        grepl("^Total", state_name) ~ "United States",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      )
    )
  dropped_states <- setdiff(unique(data_raw$state_name[is.na(data_raw$geography_name)]), NA_character_)

  data_raw <- data_raw %>%
    filter(!is.na(geography_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography))

  if (length(dropped_states) > 0) {
    message(
      "Dropped ", n_before_geo - nrow(data_raw), " non-US / catch-all row(s) (state of residence: ",
      paste(sort(dropped_states), collapse = ", "), ")"
    )
  }

  # ---------------------------------------------------------------------------
  # 6. Pivot value columns to long, tag with age/sex, drop Ambiguous sex
  # ---------------------------------------------------------------------------
  data_long <- data_raw %>%
    select(geography, time, diagnosis, all_of(as.character(value_col_idx))) %>%
    pivot_longer(cols = all_of(as.character(value_col_idx)), names_to = "col_idx", values_to = "raw_value") %>%
    mutate(col_idx = as.integer(col_idx)) %>%
    left_join(col_meta, by = "col_idx") %>%
    filter(sex != "Ambiguous") %>%
    select(-col_idx)

  # --- Suppression + numeric parsing ---
  is_suppressed_count <- function(x) {
    x <- trimws(x)
    is.na(x) | x == "" | x == "-" | x == "10 or fewer"
  }
  unsuppress_count <- function(x) {
    x <- trimws(x)
    suppressWarnings(as.numeric(ifelse(is_suppressed_count(x), "5", gsub(",", "", x))))
  }

  data_long <- data_long %>%
    mutate(
      suppressed = as.integer(is_suppressed_count(raw_value)),
      value = unsuppress_count(raw_value)
    )

  # ---------------------------------------------------------------------------
  # 7. Pivot ED Diagnoses wide: Concussion (numerator) vs Total (denominator)
  # ---------------------------------------------------------------------------
  wide_n <- data_long %>%
    select(geography, time, age, sex, diagnosis, value) %>%
    pivot_wider(names_from = diagnosis, values_from = value, values_fn = sum) %>%
    rename(epic_n_concussion = Concussion, epic_n_ed_encounters = Total)

  wide_flag <- data_long %>%
    select(geography, time, age, sex, diagnosis, suppressed) %>%
    pivot_wider(names_from = diagnosis, values_from = suppressed, values_fn = max) %>%
    rename(epic_n_concussion_suppressed_flag = Concussion, epic_n_ed_encounters_suppressed_flag = Total)

  # Flags are computed BEFORE imputation, so they record what Epic withheld.
  # When the denominator itself was suppressed it has already been imputed to
  # 5, so 5/5*100 would assert a meaningless 100% - leave those cells NA, as
  # in cosmos_gas; epic_n_ed_encounters_suppressed_flag marks them.
  data_clean <- wide_n %>%
    left_join(wide_flag, by = c("geography", "time", "age", "sex")) %>%
    mutate(
      epic_pct_concussion = if_else(
        epic_n_ed_encounters_suppressed_flag == 1L | epic_n_ed_encounters == 0,
        NA_real_,
        epic_n_concussion / epic_n_ed_encounters * 100
      )
    ) %>%
    select(
      geography, time, age, sex,
      epic_n_concussion, epic_n_concussion_suppressed_flag,
      epic_pct_concussion,
      epic_n_ed_encounters, epic_n_ed_encounters_suppressed_flag
    ) %>%
    arrange(geography, age, sex, time)

  # ---------------------------------------------------------------------------
  # 8. Validate
  # ---------------------------------------------------------------------------
  dupes <- data_clean %>%
    count(geography, time, age, sex) %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate rows per geography/time/age/sex (", nrow(dupes), " combinations). ",
         "Check for overlapping staging files in raw/staging/.")
  }

  stopifnot(
    # Geography: FIPS strings, national is "00"
    all(nchar(data_clean$geography) == 2),
    "00" %in% data_clean$geography,
    # Time: YYYY-mm-dd, always the last day of a month
    all(grepl("^\\d{4}-\\d{2}-\\d{2}$", data_clean$time)),
    all(as.Date(data_clean$time) == ceiling_date(as.Date(data_clean$time), "month") - days(1)),
    # Counts are present and non-negative
    !any(is.na(data_clean$epic_n_concussion)),
    !any(is.na(data_clean$epic_n_ed_encounters)),
    all(data_clean$epic_n_concussion >= 0),
    all(data_clean$epic_n_ed_encounters >= 0),
    # Percent is a percent, in [0, 100]
    all(data_clean$epic_pct_concussion >= 0 & data_clean$epic_pct_concussion <= 100, na.rm = TRUE),
    # Flags are 0/1, and every imputed value carries a flag of 1
    all(data_clean$epic_n_concussion_suppressed_flag %in% c(0L, 1L)),
    all(data_clean$epic_n_ed_encounters_suppressed_flag %in% c(0L, 1L)),
    all(data_clean$epic_n_concussion[data_clean$epic_n_concussion_suppressed_flag == 1L] == 5),
    all(data_clean$epic_n_ed_encounters[data_clean$epic_n_ed_encounters_suppressed_flag == 1L] == 5),
    # The percent is missing exactly where the denominator was suppressed
    identical(
      is.na(data_clean$epic_pct_concussion),
      data_clean$epic_n_ed_encounters_suppressed_flag == 1L
    )
  )

  message(
    "Standardized ", nrow(data_clean), " rows | ",
    length(unique(data_clean$geography)), " geographies | ",
    min(data_clean$time), " to ", max(data_clean$time)
  )
  message(
    "  epic_n_concussion_suppressed_flag: ", sum(data_clean$epic_n_concussion_suppressed_flag),
    " suppressed/imputed (covers epic_n_concussion and epic_pct_concussion)"
  )
  message(
    "  epic_n_ed_encounters_suppressed_flag: ", sum(data_clean$epic_n_ed_encounters_suppressed_flag),
    " suppressed/imputed, ", sum(is.na(data_clean$epic_pct_concussion)), " percent cells left NA"
  )

  # ---------------------------------------------------------------------------
  # 9. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")
  vroom::vroom_write(data_clean, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 10. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
