# read data from census ingest and write to dist directory
library(tidyverse)
library(arrow)

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz",
                         show_col_types = FALSE)

## State
sdoh_state <- vroom::vroom("../census/standard/data_state.csv.gz",
                          show_col_types = FALSE) %>%
  rename(fips = geography) %>%
  left_join(all_fips, by = c("fips" = "geography")) %>%
  rename(geography = geography_name) %>%
  mutate(
    geography = if_else(fips == "00", "United States", geography),
    year      = lubridate::year(time),
    source    = "U.S. Census Bureau ACS"
  ) %>%
  filter(geography %in% c(state.name, "District of Columbia",
                          "United States")) %>%
  dplyr::select(geography, fips, year, source, starts_with("acs_")) %>%
  pivot_longer(
    cols         = starts_with("acs_"),
    names_to     = "outcome_name",
    names_prefix = "acs_",
    values_to    = "value"
  ) %>%

  filter(!is.na(value)) %>%
  dplyr::select(geography, fips, year, source, outcome_name, value)

arrow::write_parquet(sdoh_state, "dist/sdoh_state.parquet",
                     compression = "snappy")

## County
sdoh_county <- vroom::vroom("../census/standard/data_county.csv.gz",
                           show_col_types = FALSE) %>%
  mutate(
    year   = lubridate::year(time),
    source = "U.S. Census Bureau ACS"
  ) %>%
  dplyr::select(geography, year, source, starts_with("acs_")) %>%
  pivot_longer(
    cols         = starts_with("acs_"),
    names_to     = "outcome_name",
    names_prefix = "acs_",
    values_to    = "value"
  ) %>%

  filter(!is.na(value)) %>%
  dplyr::select(geography, year, source, outcome_name, value)

arrow::write_parquet(sdoh_county, "dist/sdoh_county.parquet",
                     compression = "snappy")
