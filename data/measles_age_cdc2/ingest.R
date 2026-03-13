# =============================================================================
# CDC Measles Cases by Age Group and Vaccination Status
# Source: https://www.cdc.gov/measles/data-research/index.html
# Files:
#   raw/cdc_measles_new_cases_age.csv     - New weekly cases (hospitalization counts)
#   raw/cdc_measles_cumulative_age.csv    - Cumulative cases (hospitalization percentages)
# Output:
#   standard/data.csv.gz - Combined data with type = "new_cases" | "cumulative"
# =============================================================================

library(dplyr)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}


library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL for standard files
base_url <- "https://github.com/PopHIVE/measles_age_cdc_scraper/raw/refs/heads/main/"

# Files to download from the remote standard folder
raw_files <- c(
  "measles_structured.csv"
)

# Download each file and track hashes for change detection
current_hashes <- list()
any_changed <- FALSE

for (f in raw_files) {
  url <- paste0(base_url, "/", f)
  dest <- file.path("raw", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", f, ": ", e$message)
  })
}


cum_file  <- "raw/measles_structured.csv"

# Change detection based on file hashes
raw_state <- list(
  new_hash = tools::md5sum(new_file),
  cum_hash = tools::md5sum(cum_file)
)

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # New weekly cases
  # Columns: update_date, cases_under_5, cases_5_19, cases_over_20,
  #          cases_age_unknown, cases_unvaccinated_unknown, cases_one_dose,
  #          cases_two_doses, hospitalizations_total, hospitalizations_under_5,
  #          hospitalizations_5_19, hospitalizations_over_20,
  #          hospitalizations_age_unknown
  # ---------------------------------------------------------------------------
  new_cases <- vroom::vroom(
    new_file,
    show_col_types = FALSE,
    na = c("", "NA", "N/A")
  ) %>%
    mutate(
      geography   = "00",
      type        = "new_cases",
      time        = format(as.Date(update_date), "%Y-%m-%d"),
      cases_total = cases_under_5 + cases_5_19 + cases_over_20 + cases_age_unknown
    ) %>%
    rename(
      hosp_total             = hospitalizations_total,
      hosp_count_under_5     = hospitalizations_under_5,
      hosp_count_5_19        = hospitalizations_5_19,
      hosp_count_over_20     = hospitalizations_over_20,
      hosp_count_age_unknown = hospitalizations_age_unknown
    ) %>%
    select(
      geography, time, type,
      cases_total, cases_under_5, cases_5_19, cases_over_20, cases_age_unknown,
      cases_unvaccinated_unknown, cases_one_dose, cases_two_doses,
      hosp_total,
      hosp_count_under_5, hosp_count_5_19, hosp_count_over_20, hosp_count_age_unknown
    )

  # ---------------------------------------------------------------------------
  # Cumulative cases
  # Columns: Update Date, Total Cases, Cases <5, Cases 5-19, Cases 20+,
  #          Cases Age Unknown, Cases Unvacc/Unknown, Cases 1 Dose, Cases 2+ Doses,
  #          Hosp Total, Hosp <5 %, Hosp 5-19 %, Hosp 20+ %, Hosp Age Unknown %
  # Note: hospitalization columns are percentages (not counts)
  # Note: negative values are CDC retroactive corrections, not errors
  # ---------------------------------------------------------------------------
  cum_cases <- vroom::vroom(
    cum_file,
    show_col_types = FALSE,
    na = c("", "NA", "N/A")
  ) %>%
    mutate(
      geography = "00",
      type      = "cumulative",
      time      = format(as.Date(`Update Date`, format = "%B %d, %Y"), "%Y-%m-%d")
    ) %>%
    rename(
      cases_total                = `Total Cases`,
      cases_under_5              = `Cases <5`,
      cases_5_19                 = `Cases 5-19`,
      cases_over_20              = `Cases 20+`,
      cases_age_unknown          = `Cases Age Unknown`,
      cases_unvaccinated_unknown = `Cases Unvacc/Unknown`,
      cases_one_dose             = `Cases 1 Dose`,
      cases_two_doses            = `Cases 2+ Doses`,
      hosp_total                 = `Hosp Total`,
      hosp_pct_under_5           = `Hosp <5 %`,
      hosp_pct_5_19              = `Hosp 5-19 %`,
      hosp_pct_over_20           = `Hosp 20+ %`,
      hosp_pct_age_unknown       = `Hosp Age Unknown %`
    ) %>%
    select(
      geography, time, type,
      cases_total, cases_under_5, cases_5_19, cases_over_20, cases_age_unknown,
      cases_unvaccinated_unknown, cases_one_dose, cases_two_doses,
      hosp_total,
      hosp_pct_under_5, hosp_pct_5_19, hosp_pct_over_20, hosp_pct_age_unknown
    )

  # ---------------------------------------------------------------------------
  # Combine: hosp_count_* columns are NA for cumulative rows;
  #          hosp_pct_* columns are NA for new_cases rows
  # ---------------------------------------------------------------------------
  combined <- bind_rows(new_cases, cum_cases) %>%
    arrange(type, time)

  vroom::vroom_write(combined, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
