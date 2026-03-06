# =============================================================================
# CDC Measles Cases by Age Group Data Ingestion
# Source: https://www.cdc.gov/measles/data-research/index.html
# Ingests two files: new weekly cases and cumulative cases by age/vaccination
# =============================================================================

library(dplyr)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

new_file <- "raw/cdc_measles_new_cases_age.csv"
cum_file  <- "raw/cdc_measles_cumulative_age.csv"

# Calculate hash for change detection
raw_state <- list(
  new_hash = tools::md5sum(new_file),
  cum_hash = tools::md5sum(cum_file)
)

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read and process new cases file
  # ---------------------------------------------------------------------------
  new_cases <- vroom::vroom(new_file, show_col_types = FALSE, na = c("", "NA", "N/A")) %>%
    mutate(
      geography   = "00",
      type        = "new",
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
      hosp_total, hosp_count_under_5, hosp_count_5_19, hosp_count_over_20, hosp_count_age_unknown
    )

  # ---------------------------------------------------------------------------
  # 3. Read and process cumulative cases file
  # ---------------------------------------------------------------------------
  cum_cases <- vroom::vroom(cum_file, show_col_types = FALSE, na = c("", "NA", "N/A")) %>%
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
      hosp_total, hosp_pct_under_5, hosp_pct_5_19, hosp_pct_over_20, hosp_pct_age_unknown
    )

  # ---------------------------------------------------------------------------
  # 4. Combine and write output
  # ---------------------------------------------------------------------------
  combined <- bind_rows(new_cases, cum_cases) %>%
    arrange(type, time)

  vroom::vroom_write(combined, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
