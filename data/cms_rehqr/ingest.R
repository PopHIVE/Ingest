# =============================================================================
# CMS Rural Emergency Hospital Quality Reporting (REHQR) Data Ingestion
# Source: https://data.cms.gov/provider-data/dataset/uk3n-au7a
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

dir.create("raw", showWarnings = FALSE)

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
# The CSV download URL is versioned and changes whenever CMS refreshes the
# dataset, so look up the current URL from the metastore API rather than
# hardcoding it.
dataset_meta <- jsonlite::fromJSON(
  "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/uk3n-au7a?show-reference-ids=false"
)
download_url <- dataset_meta$distribution$data$downloadURL[[1]]

download.file(download_url, "raw/REH_Unplanned_Hospital_Visits-National.csv", mode = "wb", quiet = TRUE)

raw_state <- list(hash = unname(tools::md5sum("raw/REH_Unplanned_Hospital_Visits-National.csv")))

# -----------------------------------------------------------------------------
# 2. Check for changes
# -----------------------------------------------------------------------------
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 3. Read raw data
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom(
    "raw/REH_Unplanned_Hospital_Visits-National.csv",
    show_col_types = FALSE
  )

  # ---------------------------------------------------------------------------
  # 4. Transform to standard wide format
  # ---------------------------------------------------------------------------
  # National-level only (this REHQR dataset does not report by state/county).
  # Each `Measure ID` becomes its own `cms_rehqr_<measure>` column so that a
  # future REHQR measure added by CMS is picked up automatically.
  data_long <- data_raw %>%
    transmute(
      geography = "00",
      time = format(as.Date(`End Date`, "%m/%d/%Y"), "%Y-%m-%d"),
      measure = paste0("cms_rehqr_", tolower(gsub("^REH_", "", `Measure ID`))),
      value = suppressWarnings(as.numeric(`National Rate`)),
      # "Not Available" national rates (e.g. too few hospitals with sufficient
      # volume, flagged via `Footnote`) have no other national value in the
      # same period to impute from, so they are left NA and flagged.
      suppressed = if_else(is.na(value), 1L, 0L)
    )

  data_standard <- data_long %>%
    select(geography, time, measure, value) %>%
    tidyr::pivot_wider(names_from = measure, values_from = value) %>%
    left_join(
      data_long %>%
        select(geography, time, measure, suppressed) %>%
        tidyr::pivot_wider(
          names_from = measure,
          values_from = suppressed,
          names_glue = "{measure}_suppressed"
        ),
      by = c("geography", "time")
    ) %>%
    # An all-NA pivoted column (e.g. a brand-new measure suppressed in every
    # period so far) is otherwise typed logical instead of numeric.
    mutate(across(starts_with("cms_rehqr_"), as.numeric))

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # 6. Update process record
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
