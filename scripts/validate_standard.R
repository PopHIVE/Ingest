# =============================================================================
# PopHIVE/Ingest Standard Format Validation Script
# 
# This script validates that standardized data files conform to the PopHIVE
# data format specifications. Run on individual files or entire directories.
#
# Usage:
#   source("scripts/validate_standard.R")
#   validate_standard_file("data/source_name/standard/data.csv.gz")
#   validate_all_sources()
#
# =============================================================================

library(dplyr)
library(vroom)
library(jsonlite)
library(cli)

# =============================================================================
# Configuration
# =============================================================================

# Valid FIPS codes for US states and territories
VALID_STATE_FIPS <- c(

"00",  # National
  "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
  "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
  "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
  "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
  "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
  "56", "60", "66", "69", "72", "78"  # Territories
)

# Required columns
REQUIRED_COLUMNS <- c("geography", "time")

# Standard optional columns (for suggestions)
STANDARD_COLUMNS <- c(
  "geography", "time", "age", "race_ethnicity", "sex", "virus",
  "value", "value_smooth", "value_smooth_scale", "suppressed_flag"
)

# =============================================================================
# Validation Result Class
# =============================================================================

#' Create a validation result object
#' @param check_name Name of the validation check
#' @param passed Logical, whether check passed
#' @param message Descriptive message
#' @param details Optional list of details
validation_result <- function(check_name, passed, message, details = NULL) {
  structure(
    list(
      check = check_name,
      passed = passed,
      message = message,
      details = details
    ),
    class = "validation_result"
  )
}

#' Print validation result
print.validation_result <- function(x, ...) {
  status <- if (x$passed) cli::col_green("✓ PASS") else cli::col_red("✗ FAIL")
  cat(sprintf("%s [%s] %s\n", status, x$check, x$message))
  if (!is.null(x$details) && !x$passed) {
    for (detail in x$details) {
      cat(sprintf("        → %s\n", detail))
    }
  }
}

# =============================================================================
# Individual Validation Functions
# =============================================================================

#' Check that file exists and is readable
check_file_exists <- function(filepath) {
  exists <- file.exists(filepath)
  validation_result(
    "file_exists",
    exists,
    if (exists) "File exists and is accessible" else paste("File not found:", filepath)
  )
}

#' Check file compression
check_compression <- function(filepath) {
  is_compressed <- grepl("\\.(gz|xz|bz2)$", filepath, ignore.case = TRUE)
  validation_result(
    "compression",
    is_compressed,
    if (is_compressed) {
      "File is compressed"
    } else {
      "File should be compressed (.gz recommended)"
    }
  )
}

#' Check required columns exist
check_required_columns <- function(df) {
  missing <- setdiff(REQUIRED_COLUMNS, names(df))
  passed <- length(missing) == 0
  validation_result(
    "required_columns",
    passed,
    if (passed) {
      "All required columns present"
    } else {
      paste("Missing required columns:", paste(missing, collapse = ", "))
    }
  )
}

#' Check geography column format
check_geography_format <- function(df) {
  if (!"geography" %in% names(df)) {
    return(validation_result("geography_format", FALSE, "Geography column missing"))
  }
  
  geo_values <- unique(df$geography)
  
  # Check for NA values
  has_na <- any(is.na(geo_values))
  
  # Check format: should be string FIPS codes
  # State: 2 digits, County: 5 digits, National: "00"
  valid_pattern <- "^(00|[0-9]{2}|[0-9]{5})$"
  invalid_values <- geo_values[!grepl(valid_pattern, geo_values) & !is.na(geo_values)]
  
  # Check if state FIPS codes are valid
  state_codes <- geo_values[nchar(geo_values) == 2 & !is.na(geo_values)]
  invalid_states <- setdiff(state_codes, VALID_STATE_FIPS)
  
  issues <- c()
  if (has_na) issues <- c(issues, "Contains NA values")
  if (length(invalid_values) > 0) {
    issues <- c(issues, paste("Invalid format:", 
                               paste(head(invalid_values, 5), collapse = ", "),
                               if (length(invalid_values) > 5) "..." else ""))
  }
  if (length(invalid_states) > 0) {
    issues <- c(issues, paste("Invalid state FIPS:", paste(invalid_states, collapse = ", ")))
  }
  
  passed <- length(issues) == 0
  validation_result(
    "geography_format",
    passed,
    if (passed) {
      sprintf("Geography format valid (%d unique values)", length(geo_values))
    } else {
      "Geography format issues found"
    },
    details = issues
  )
}

#' Check time column format (MM-DD-YYYY)
check_time_format <- function(df) {
  if (!"time" %in% names(df)) {
    return(validation_result("time_format", FALSE, "Time column missing"))
  }
  
  time_values <- unique(df$time)
  
  # Check for NA values
  has_na <- any(is.na(time_values))
  
  # Expected format: MM-DD-YYYY
  expected_pattern <- "^[0-9]{2}-[0-9]{2}-[0-9]{4}$"
  format_matches <- grepl(expected_pattern, time_values) | is.na(time_values)
  invalid_format <- time_values[!format_matches]
  
  # Try to parse valid-looking dates
  valid_looking <- time_values[format_matches & !is.na(time_values)]
  parsed <- suppressWarnings(as.Date(valid_looking, format = "%m-%d-%Y"))
  unparseable <- valid_looking[is.na(parsed)]
  
  issues <- c()
  if (has_na) issues <- c(issues, sprintf("Contains %d NA values", sum(is.na(df$time))))
  if (length(invalid_format) > 0) {
    issues <- c(issues, paste("Wrong format (expected MM-DD-YYYY):", 
                               paste(head(invalid_format, 3), collapse = ", "),
                               if (length(invalid_format) > 3) "..." else ""))
  }
  if (length(unparseable) > 0) {
    issues <- c(issues, paste("Invalid dates:", paste(head(unparseable, 3), collapse = ", ")))
  }
  
  passed <- length(issues) == 0
  validation_result(
    "time_format",
    passed,
    if (passed) {
      date_range <- range(parsed, na.rm = TRUE)
      sprintf("Time format valid (range: %s to %s)", date_range[1], date_range[2])
    } else {
      "Time format issues found"
    },
    details = issues
  )
}

#' Check if weekly data falls on Saturday
check_weekly_saturday <- function(df) {
  if (!"time" %in% names(df)) {
    return(validation_result("weekly_saturday", NA, "Time column missing"))
  }
  
  time_values <- unique(df$time)
  valid_looking <- time_values[grepl("^[0-9]{2}-[0-9]{2}-[0-9]{4}$", time_values)]
  parsed <- as.Date(valid_looking, format = "%m-%d-%Y")
  parsed <- parsed[!is.na(parsed)]
  
  if (length(parsed) < 2) {
    return(validation_result("weekly_saturday", NA, "Insufficient dates to determine frequency"))
  }
  
  # Check if data appears to be weekly (most common interval is 7 days)
  sorted_dates <- sort(parsed)
  intervals <- diff(sorted_dates)
  median_interval <- median(as.numeric(intervals))
  
  is_weekly <- abs(median_interval - 7) < 2  # Allow some tolerance
  
  if (!is_weekly) {
    return(validation_result(
      "weekly_saturday",
      NA,
      sprintf("Data does not appear to be weekly (median interval: %.1f days)", median_interval)
    ))
  }
  
  # Check if dates fall on Saturday (weekday 6 in R's convention, or 7 if using lubridate)
  weekdays <- as.POSIXlt(parsed)$wday  # 0 = Sunday, 6 = Saturday
  saturdays <- sum(weekdays == 6)
  pct_saturday <- saturdays / length(parsed) * 100
  
  passed <- pct_saturday >= 90  # Allow some tolerance
  
  non_saturday_dates <- parsed[weekdays != 6]
  
  validation_result(
    "weekly_saturday",
    passed,
    if (passed) {
      sprintf("Weekly data correctly uses Saturday (%.1f%% on Saturday)", pct_saturday)
    } else {
      sprintf("Weekly data should use Saturday (only %.1f%% on Saturday)", pct_saturday)
    },
    details = if (!passed && length(non_saturday_dates) > 0) {
      c(paste("Non-Saturday dates:", paste(head(format(non_saturday_dates), 5), collapse = ", ")))
    }
  )
}

#' Check for value columns
check_value_columns <- function(df) {
  value_cols <- grep("^value", names(df), value = TRUE)
  
  if (length(value_cols) == 0) {
    return(validation_result(
      "value_columns",
      FALSE,
      "No value columns found (expected 'value', 'value_smooth', etc.)"
    ))
  }
  
  # Check for numeric types
  non_numeric <- value_cols[!sapply(df[value_cols], is.numeric)]
  
  issues <- c()
  if (length(non_numeric) > 0) {
    issues <- c(issues, paste("Non-numeric value columns:", paste(non_numeric, collapse = ", ")))
  }
  
  # Check for all-NA columns
  all_na <- value_cols[sapply(df[value_cols], function(x) all(is.na(x)))]
  if (length(all_na) > 0) {
    issues <- c(issues, paste("All-NA columns:", paste(all_na, collapse = ", ")))
  }
  
  passed <- length(issues) == 0
  validation_result(
    "value_columns",
    passed,
    if (passed) {
      sprintf("Value columns valid: %s", paste(value_cols, collapse = ", "))
    } else {
      "Value column issues found"
    },
    details = issues
  )
}

#' Check suppression flag consistency
check_suppression_flag <- function(df) {
  if (!"suppressed_flag" %in% names(df)) {
    return(validation_result(
      "suppression_flag",
      NA,
      "No suppressed_flag column (OK if no suppression)"
    ))
  }
  
  flag_values <- unique(df$suppressed_flag)
  valid_values <- c(0, 1, NA)
  invalid <- setdiff(flag_values[!is.na(flag_values)], c(0, 1))
  
  issues <- c()
  if (length(invalid) > 0) {
    issues <- c(issues, paste("Invalid flag values (should be 0 or 1):", paste(invalid, collapse = ", ")))
  }
  
  # Check if suppressed rows have values
  if ("value" %in% names(df)) {
    suppressed_with_value <- sum(df$suppressed_flag == 1 & !is.na(df$value), na.rm = TRUE)
    suppressed_without_value <- sum(df$suppressed_flag == 1 & is.na(df$value), na.rm = TRUE)
    
    if (suppressed_without_value > 0) {
      issues <- c(issues, sprintf("%d suppressed rows have NA values (should be imputed)", 
                                   suppressed_without_value))
    }
  }
  
  passed <- length(issues) == 0
  n_suppressed <- sum(df$suppressed_flag == 1, na.rm = TRUE)
  
  validation_result(
    "suppression_flag",
    passed,
    if (passed) {
      sprintf("Suppression flag valid (%d rows flagged)", n_suppressed)
    } else {
      "Suppression flag issues found"
    },
    details = issues
  )
}

#' Check for duplicate rows
check_duplicates <- function(df) {
  # Define key columns (those that should uniquely identify a row)
  key_cols <- intersect(names(df), c("geography", "time", "age", "race_ethnicity", "sex", "virus"))
  
  if (length(key_cols) < 2) {
    return(validation_result("duplicates", NA, "Insufficient key columns to check duplicates"))
  }
  
  dup_check <- df %>%
    group_by(across(all_of(key_cols))) %>%
    filter(n() > 1) %>%
    nrow()
  
  passed <- dup_check == 0
  
  validation_result(
    "duplicates",
    passed,
    if (passed) {
      sprintf("No duplicates found (keys: %s)", paste(key_cols, collapse = ", "))
    } else {
      sprintf("%d duplicate key combinations found", dup_check)
    }
  )
}

#' Check column naming conventions
check_column_names <- function(df) {
  col_names <- names(df)
  
  # Check for lowercase and underscores
  valid_pattern <- "^[a-z][a-z0-9_]*$"
  invalid_names <- col_names[!grepl(valid_pattern, col_names)]
  
  # Check for known standard columns
  unknown_cols <- setdiff(col_names, STANDARD_COLUMNS)
  
  issues <- c()
  if (length(invalid_names) > 0) {
    issues <- c(issues, paste("Non-standard naming (use lowercase_underscore):", 
                               paste(invalid_names, collapse = ", ")))
  }
  
  # This is just a warning, not a failure
  warnings <- c()
  if (length(unknown_cols) > 0) {
    warnings <- c(warnings, paste("Non-standard columns (OK if documented):", 
                                   paste(unknown_cols, collapse = ", ")))
  }
  
  passed <- length(issues) == 0
  
  validation_result(
    "column_names",
    passed,
    if (passed) {
      sprintf("Column names follow conventions (%d columns)", length(col_names))
    } else {
      "Column naming issues found"
    },
    details = c(issues, if (length(warnings) > 0) paste("Note:", warnings))
  )
}

#' Check data completeness
check_completeness <- function(df) {
  total_cells <- nrow(df) * ncol(df)
  na_cells <- sum(is.na(df))
  pct_complete <- (1 - na_cells / total_cells) * 100
  
  # Check completeness of key columns
  key_completeness <- sapply(df[intersect(names(df), c("geography", "time"))], 
                              function(x) sum(!is.na(x)) / length(x) * 100)
  
  issues <- c()
  if (any(key_completeness < 100)) {
    issues <- c(issues, sprintf("Key columns have missing values: %s",
                                 paste(names(key_completeness[key_completeness < 100]), 
                                       collapse = ", ")))
  }
  
  passed <- all(key_completeness == 100)
  
  validation_result(
    "completeness",
    passed,
    sprintf("Data %.1f%% complete (%s rows × %d columns)", pct_complete, 
            format(nrow(df), big.mark = ","), ncol(df)),
    details = issues
  )
}

# =============================================================================
# Main Validation Functions
# =============================================================================

#' Validate a single standard format file
#' 
#' @param filepath Path to the standardized CSV file
#' @param verbose Print results as they run
#' @return List of validation results
#' @export
validate_standard_file <- function(filepath, verbose = TRUE) {
  
  if (verbose) {
    cli::cli_h1("Validating: {basename(filepath)}")
    cli::cli_text("Path: {filepath}")
    cli::cli_text("")
  }
  
  results <- list()
  
  # Check file exists
  results$file_exists <- check_file_exists(filepath)
  if (verbose) print(results$file_exists)
  
  if (!results$file_exists$passed) {
    return(invisible(results))
  }
  
  # Check compression
  results$compression <- check_compression(filepath)
  if (verbose) print(results$compression)
  
  # Read data
  if (verbose) cli::cli_text("Reading data...")
  df <- tryCatch(
    vroom::vroom(filepath, show_col_types = FALSE),
    error = function(e) {
      results$read_error <<- validation_result("read_file", FALSE, paste("Error reading file:", e$message))
      if (verbose) print(results$read_error)
      return(NULL)
    }
  )
  
  if (is.null(df)) {
    return(invisible(results))
  }
  
  if (verbose) cli::cli_text("Read {format(nrow(df), big.mark=',')} rows, {ncol(df)} columns\n")
  
  # Run all checks
  results$required_columns <- check_required_columns(df)
  if (verbose) print(results$required_columns)
  
  results$column_names <- check_column_names(df)
  if (verbose) print(results$column_names)
  
  results$geography_format <- check_geography_format(df)
  if (verbose) print(results$geography_format)
  
  results$time_format <- check_time_format(df)
  if (verbose) print(results$time_format)
  
  results$weekly_saturday <- check_weekly_saturday(df)
  if (verbose) print(results$weekly_saturday)
  
  results$value_columns <- check_value_columns(df)
  if (verbose) print(results$value_columns)
  
  results$suppression_flag <- check_suppression_flag(df)
  if (verbose) print(results$suppression_flag)
  
  results$duplicates <- check_duplicates(df)
  if (verbose) print(results$duplicates)
  
  results$completeness <- check_completeness(df)
  if (verbose) print(results$completeness)
  
  # Summary
  if (verbose) {
    passed <- sum(sapply(results, function(r) isTRUE(r$passed)))
    failed <- sum(sapply(results, function(r) isFALSE(r$passed)))
    skipped <- sum(sapply(results, function(r) is.na(r$passed)))
    
    cli::cli_text("")
    cli::cli_rule()
    if (failed == 0) {
      cli::cli_alert_success("All checks passed! ({passed} passed, {skipped} skipped)")
    } else {
      cli::cli_alert_danger("{failed} checks failed, {passed} passed, {skipped} skipped")
    }
  }
  
  invisible(results)
}

#' Validate measure_info.json for a source
#' 
#' @param source_dir Path to the source directory
#' @param verbose Print results
#' @return List of validation results
#' @export
validate_measure_info <- function(source_dir, verbose = TRUE) {
  
  measure_info_path <- file.path(source_dir, "measure_info.json")
  
  if (verbose) {
    cli::cli_h2("Validating measure_info.json")
  }
  
  results <- list()
  
  # Check file exists
  if (!file.exists(measure_info_path)) {
    results$exists <- validation_result("measure_info_exists", FALSE, "measure_info.json not found")
    if (verbose) print(results$exists)
    return(invisible(results))
  }
  
  # Read JSON
  measure_info <- tryCatch(
    jsonlite::fromJSON(measure_info_path),
    error = function(e) {
      results$parse <<- validation_result("measure_info_parse", FALSE, paste("JSON parse error:", e$message))
      if (verbose) print(results$parse)
      return(NULL)
    }
  )
  
  if (is.null(measure_info)) {
    return(invisible(results))
  }
  
  results$exists <- validation_result("measure_info_exists", TRUE, 
                                       sprintf("Found %d variable definitions", length(measure_info)))
  if (verbose) print(results$exists)
  
  # Check each variable has required fields
  required_fields <- c("id", "short_name", "measure_type", "unit", "time_resolution")
  
  missing_fields <- list()
  for (var_name in names(measure_info)) {
    var_def <- measure_info[[var_name]]
    missing <- setdiff(required_fields, names(var_def))
    if (length(missing) > 0) {
      missing_fields[[var_name]] <- missing
    }
  }
  
  if (length(missing_fields) > 0) {
    details <- sapply(names(missing_fields), function(v) {
      sprintf("%s: missing %s", v, paste(missing_fields[[v]], collapse = ", "))
    })
    results$fields <- validation_result("measure_info_fields", FALSE, 
                                         "Some variables missing required fields", 
                                         details = details)
  } else {
    results$fields <- validation_result("measure_info_fields", TRUE, 
                                         "All variables have required fields")
  }
  if (verbose) print(results$fields)
  
  # Check if variables in data match measure_info
  standard_files <- list.files(file.path(source_dir, "standard"), 
                                pattern = "\\.csv", full.names = TRUE)
  
  if (length(standard_files) > 0) {
    # Read first standard file to check variables
    df <- vroom::vroom(standard_files[1], n_max = 1, show_col_types = FALSE)
    data_vars <- setdiff(names(df), c("geography", "time", "age", "race_ethnicity", 
                                       "sex", "virus", "suppressed_flag"))
    
    undocumented <- setdiff(data_vars, names(measure_info))
    
    if (length(undocumented) > 0) {
      results$coverage <- validation_result("measure_info_coverage", FALSE,
                                             "Some data variables not documented",
                                             details = paste("Undocumented:", paste(undocumented, collapse = ", ")))
    } else {
      results$coverage <- validation_result("measure_info_coverage", TRUE,
                                             "All data variables documented")
    }
    if (verbose) print(results$coverage)
  }
  
  invisible(results)
}

#' Validate all sources in the data directory
#' 
#' @param data_dir Path to data directory (default: "data")
#' @param stop_on_error Stop validation if any source fails
#' @return Data frame summarizing results
#' @export
validate_all_sources <- function(data_dir = "data", stop_on_error = FALSE) {
  
  cli::cli_h1("Validating All Data Sources")
  cli::cli_text("Data directory: {data_dir}\n")
  
  # Find all source directories (those with standard/ subdirectory)
  source_dirs <- list.dirs(data_dir, recursive = FALSE)
  source_dirs <- source_dirs[sapply(source_dirs, function(d) {
    dir.exists(file.path(d, "standard")) && !grepl("^bundle_", basename(d))
  })]
  
  cli::cli_text("Found {length(source_dirs)} data sources\n")
  
  results_summary <- data.frame(
    source = character(),
    files_checked = integer(),
    checks_passed = integer(),
    checks_failed = integer(),
    status = character(),
    stringsAsFactors = FALSE
  )
  
  for (source_dir in source_dirs) {
    source_name <- basename(source_dir)
    cli::cli_h2("Source: {source_name}")
    
    # Find standard files
    standard_files <- list.files(file.path(source_dir, "standard"),
                                  pattern = "\\.(csv|csv\\.gz|csv\\.xz)$",
                                  full.names = TRUE)
    
    if (length(standard_files) == 0) {
      cli::cli_alert_warning("No standard files found")
      results_summary <- rbind(results_summary, data.frame(
        source = source_name,
        files_checked = 0,
        checks_passed = 0,
        checks_failed = 0,
        status = "NO_FILES"
      ))
      next
    }
    
    source_passed <- 0
    source_failed <- 0
    
    for (filepath in standard_files) {
      results <- validate_standard_file(filepath, verbose = TRUE)
      
      passed <- sum(sapply(results, function(r) isTRUE(r$passed)))
      failed <- sum(sapply(results, function(r) isFALSE(r$passed)))
      
      source_passed <- source_passed + passed
      source_failed <- source_failed + failed
    }
    
    # Also validate measure_info
    validate_measure_info(source_dir, verbose = TRUE)
    
    status <- if (source_failed == 0) "PASS" else "FAIL"
    
    results_summary <- rbind(results_summary, data.frame(
      source = source_name,
      files_checked = length(standard_files),
      checks_passed = source_passed,
      checks_failed = source_failed,
      status = status
    ))
    
    if (stop_on_error && source_failed > 0) {
      cli::cli_alert_danger("Stopping due to failures in {source_name}")
      break
    }
    
    cli::cli_text("")
  }
  
  # Final summary
  cli::cli_h1("Validation Summary")
  
  total_passed <- sum(results_summary$checks_passed)
  total_failed <- sum(results_summary$checks_failed)
  sources_passed <- sum(results_summary$status == "PASS")
  sources_failed <- sum(results_summary$status == "FAIL")
  
  print(results_summary)
  
  cli::cli_text("")
  if (sources_failed == 0) {
    cli::cli_alert_success("All {sources_passed} sources passed validation!")
  } else {
    cli::cli_alert_danger("{sources_failed} sources failed, {sources_passed} passed")
  }
  
  invisible(results_summary)
}

#' Quick validation check - returns TRUE/FALSE
#' 
#' @param filepath Path to file
#' @return Logical indicating if file passes all critical checks
#' @export
is_valid_standard <- function(filepath) {
  results <- validate_standard_file(filepath, verbose = FALSE)
  
  critical_checks <- c("file_exists", "required_columns", "geography_format", "time_format")
  all(sapply(results[critical_checks], function(r) isTRUE(r$passed)))
}

# =============================================================================
# CLI Entry Point
# =============================================================================

# If run as script, validate files passed as arguments
if (!interactive() && length(commandArgs(trailingOnly = TRUE)) > 0) {
  args <- commandArgs(trailingOnly = TRUE)
  
  if (args[1] == "--all") {
    validate_all_sources()
  } else {
    for (filepath in args) {
      validate_standard_file(filepath)
    }
  }
}