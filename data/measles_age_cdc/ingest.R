# =============================================================================
# CDC Measles Cases by Age Group - New & Cumulative Weekly Data
# Source: https://www.cdc.gov/measles/data-research/index.html
# Two manually-downloaded files are combined into one standard output:
#   - cdc_measles_new_cases_age.csv    (weekly new cases; hosp columns = counts)
#   - cdc_measles_cumulative_age.csv   (cumulative cases; hosp columns = percentages)
# Place both files in raw/ before running.
# =============================================================================

library(dplyr)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Change detection (both raw files)
# -----------------------------------------------------------------------------
new_file <- "raw/cdc_measles_new_cases_age.csv"
cum_file <- "raw/cdc_measles_cumulative_age.csv"

raw_state <- list(
  new_hash = as.character(tools::md5sum(new_file)),
  cum_hash = as.character(tools::md5sum(cum_file))
)

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read and standardize new cases file
  #    Hospitalization columns here are COUNTS.
  # ---------------------------------------------------------------------------
  new_cases <- vroom::vroom(
    new_file,
    show_col_types = FALSE,
    na = c("", "NA", "N/A")
  ) %>%
    rename(
      time                   = update_date,
      cases_under_5          = cases_under_5,
      cases_5_19             = cases_5_19,
      cases_over_20          = cases_over_20,
      cases_age_unknown      = cases_age_unknown,
      cases_unvac_unknown    = cases_unvaccinated_unknown,
      cases_one_dose         = cases_one_dose,
      cases_two_doses        = cases_two_doses,
      hosp_total             = hospitalizations_total,
      hosp_count_under_5     = hospitalizations_under_5,
      hosp_count_5_19        = hospitalizations_5_19,
      hosp_count_over_20     = hospitalizations_over_20,
      hosp_count_age_unknown = hospitalizations_age_unknown
    ) %>%
    mutate(
      geography = "00",
      type      = "new",
      time      = format(as.Date(time), "%Y-%m-%d")
    )

  # ---------------------------------------------------------------------------
  # 3. Read and standardize cumulative cases file
  #    Hospitalization columns here are PERCENTAGES (Hosp <5 %, etc.).
  # ---------------------------------------------------------------------------
  cum_cases <- vroom::vroom(
    cum_file,
    show_col_types = FALSE,
    na = c("", "NA", "N/A")
  ) %>%
    rename(
      time                  = `Update Date`,
      cases_total           = `Total Cases`,
      cases_under_5         = `Cases <5`,
      cases_5_19            = `Cases 5-19`,
      cases_over_20         = `Cases 20+`,
      cases_age_unknown     = `Cases Age Unknown`,
      cases_unvac_unknown   = `Cases Unvacc/Unknown`,
      cases_one_dose        = `Cases 1 Dose`,
      cases_two_doses       = `Cases 2+ Doses`,
      hosp_total            = `Hosp Total`,
      hosp_pct_under_5      = `Hosp <5 %`,
      hosp_pct_5_19         = `Hosp 5-19 %`,
      hosp_pct_over_20      = `Hosp 20+ %`,
      hosp_pct_age_unknown  = `Hosp Age Unknown %`
    ) %>%
    mutate(
      geography = "00",
      type      = "cumulative",
      time      = format(as.Date(time, format = "%B %d, %Y"), "%Y-%m-%d")
    )

  # ---------------------------------------------------------------------------
  # 4. Combine into single file
  #    Columns present in only one type will be NA in the other:
  #      - cases_total       : cumulative only
  #      - hosp_count_*      : new cases only
  #      - hosp_pct_*        : cumulative only
  # ---------------------------------------------------------------------------
  combined <- bind_rows(new_cases, cum_cases) %>%
    select(
      geography, time, type,
      cases_total, cases_under_5, cases_5_19, cases_over_20, cases_age_unknown,
      cases_unvac_unknown, cases_one_dose, cases_two_doses,
      hosp_total,
      hosp_count_under_5, hosp_count_5_19, hosp_count_over_20, hosp_count_age_unknown,
      hosp_pct_under_5, hosp_pct_5_19, hosp_pct_over_20, hosp_pct_age_unknown
    ) %>%
    arrange(type, time)

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(
    combined,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
