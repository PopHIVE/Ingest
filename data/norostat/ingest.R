# =============================================================================
# NoroSTAT Data Ingestion
# Source: https://www.cdc.gov/norovirus/php/reporting/norostat-data-table.html
# NoroSTAT tracks suspected and confirmed norovirus outbreaks reported to CDC
# by 14 participating state health departments.
# =============================================================================

library(dplyr)
library(rvest)
library(vroom)
library(lubridate)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download HTML, parse table, save raw CSV
# -----------------------------------------------------------------------------
url <- "https://www.cdc.gov/norovirus/php/reporting/norostat-data-table.html"
raw_file <- "raw/norostat_raw.csv"

if (!dir.exists("raw")) dir.create("raw")

page <- rvest::read_html(url)
tbl_raw <- page %>% html_table(fill = TRUE) %>% .[[1]]
write.csv(tbl_raw, raw_file, row.names = FALSE)

current_state <- list(hash = tools::md5sum(raw_file)[[raw_file]])

if (!identical(process$raw_state, current_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read raw CSV
  # ---------------------------------------------------------------------------
  tbl <- vroom::vroom(raw_file, show_col_types = FALSE)

  raw_cols <- colnames(tbl)

  # The current season column looks like "YYYY-YYYY"
  season_col <- raw_cols[grepl("^\\d{4}-\\d{4}$", raw_cols)]
  if (length(season_col) == 0) {
    stop("Could not identify the current season column in the NoroSTAT table.")
  }
  season_col <- season_col[1]

  # Extract start year of the season (e.g., 2025 from "2025-2026")
  season_start_year <- as.integer(sub("-(\\d{4})$", "", season_col))

  colnames(tbl) <- c("week_label", "hist_min", "hist_max", "hist_q25", "hist_q75", "outbreaks")

  # ---------------------------------------------------------------------------
  # 3. Convert week labels to actual dates
  #    Labels are like "1-Aug", "8-Aug", "28-Nov", "6-Feb"
  #    Aug-Dec = season_start_year; Jan-Jul = season_start_year + 1
  # ---------------------------------------------------------------------------
  tbl <- tbl %>%
    filter(!is.na(week_label), week_label != "") %>%
    mutate(
      week_date_str = paste0(
        week_label, "-",
        ifelse(grepl("Aug|Sep|Oct|Nov|Dec", week_label),
               season_start_year,
               season_start_year + 1)
      ),
      week_start = as.Date(week_date_str, format = "%d-%b-%Y"),
      # Use Saturday at end of week (week_start + 6 days)
      time = format(week_start + days(6), "%Y-%m-%d")
    )

  # ---------------------------------------------------------------------------
  # 4. Coerce value columns to numeric
  # ---------------------------------------------------------------------------
  tbl <- tbl %>%
    mutate(across(c(hist_min, hist_max, hist_q25, hist_q75, outbreaks), as.numeric))

  # ---------------------------------------------------------------------------
  # 5. Build standard wide-format output (national level only)
  # ---------------------------------------------------------------------------
  data_standard <- tbl %>%
    filter(!is.na(time)) %>%
    mutate(geography = "00") %>%
    select(
      geography,
      time,
      norostat_outbreaks = outbreaks,
      norostat_hist_min  = hist_min,
      norostat_hist_max  = hist_max,
      norostat_hist_q25  = hist_q25,
      norostat_hist_q75  = hist_q75
    ) %>%
    arrange(time)

  # ---------------------------------------------------------------------------
  # 6. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 7. Update process record
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
