# =============================================================================
# CDC Measles Weekly Cases Data Ingestion
# Source: https://www.cdc.gov/wcms/vizdata/measles/MeaslesCasesWeekly.json
# National-level weekly measles case counts
# =============================================================================

library(dplyr)
library(jsonlite)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Download raw data from CDC
# -----------------------------------------------------------------------------
url <- "https://www.cdc.gov/wcms/vizdata/measles/MeaslesCasesWeekly.json"
raw_file <- "raw/MeaslesCasesWeekly.json"

# Download the JSON file
download.file(url, raw_file, mode = "wb", quiet = TRUE)

# Calculate hash for change detection
raw_state <- list(hash = tools::md5sum(raw_file))

# Check if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read and transform data
  # ---------------------------------------------------------------------------
  measles_data <- fromJSON(raw_file)

  # Transform to standard format
  data_standard <- measles_data %>%
    as_tibble() %>%
    mutate(
      # National-level data uses FIPS code "00"
      geography = "00",
      # Use week_end date and convert to MM-DD-YYYY format
      week_end_date = as.Date(week_end),
      time = format(week_end_date, "%m-%d-%Y"),
      # Ensure cases is numeric
      value = as.numeric(cases)
    ) %>%
    # Select standard columns
    select(geography, time, value) %>%
    # Sort by time
    arrange(time)

  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    delim = ","
  )

  # Compress raw file
  unlink(paste0(raw_file, ".xz"))
  system2("xz", c("-f", raw_file))

  # ---------------------------------------------------------------------------
  # 4. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
