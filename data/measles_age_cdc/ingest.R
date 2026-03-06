# =============================================================================
# CDC Measles Cases by Age Group Data Ingestion
# Source: https://www.cdc.gov/measles/data-research/index.html
# National-level cumulative measles case counts by age group, manually recorded
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Read raw data
# -----------------------------------------------------------------------------
raw_file <- "raw/measles_age_by_week.csv"

# Calculate hash for change detection
raw_state <- list(hash = tools::md5sum(raw_file))

# Check if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read and transform data
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom(raw_file, show_col_types = FALSE)

  # Transform to standard format
  # The data has cumulative case counts by age group
  data_standard <- data_raw %>%
    # Parse date and convert to standard format
    mutate(
      date = as.Date(Update_Date, format = "%m/%d/%y"),
      time = format(date, "%m-%d-%Y"),
      geography = "00"  # National-level data
    ) %>%
    # Select and rename age columns, pivoting to long format
    select(
      geography,
      time,
      `0-4` = Under_5,
      `5-19` = Age_5_19,
      `20+` = Over_20,
      Unknown = Unknown,
      Overall = Cum_Cases
    ) %>%
    # Pivot to long format with age column
    pivot_longer(
      cols = c(`0-4`, `5-19`, `20+`, Unknown, Overall),
      names_to = "age",
      values_to = "cum_cases_measles_age"
    ) %>%
    # Remove rows with NA values
    filter(!is.na(cum_cases_measles_age)) %>%
    # Sort by time and age
    arrange(time, age)

  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 4. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
