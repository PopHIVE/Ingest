library(tidyverse)
library(dcf)
library(cdlTools)

#
# Download and add files to the raw directory
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "jbhn-e8xn",
  "raw",
  process$raw_state
)

#
# Reformat (only if raw data has changed)
#

if (!identical(process$raw_state, raw_state)) {
  
  data_raw <- vroom::vroom("./raw/jbhn-e8xn.csv.xz", show_col_types = FALSE)
  
  data_standard <- data_raw %>%
    mutate(
      # Convert state abbreviations to FIPS codes
      geography = sprintf("%02d", fips(State, to = "FIPS")),
      # Create proper date (end of month)
      time = lubridate::ceiling_date(
        lubridate::make_date(Year, Month, 1), 
        "month"
      ) - 1
    ) %>%
    rename(
      pathogen = Pathogen,
      serotype = `Serotype/Species`,
      source_type = `Source Type`,
      source_site = `Source Site`,
      narms_isolates = `Number of isolates`,
      narms_outbreak_isolates = `Outbreak associated isolates`,
      narms_new_outbreaks = `New multistate outbreaks`,
      narms_new_outbreaks_us = `New multistate outbreaks - US`,
      narms_pct_amr = `% Isolates with clinically important antimicrobial resistance`,
      narms_sequenced = `Number of sequenced isolates analyzed by NARMS`
    ) %>%
    select(
      time, geography, pathogen, serotype, source_type, source_site,
      starts_with("narms_")
    )
  
  # Write standard data
  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    ","
  )
  
  # Record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}