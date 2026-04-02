
library(dplyr)
library(tidyr)
library(arrow)
library(lubridate)
library(MMWRweek)

process <- dcf::dcf_process_record()
standard_files <- paste0("../", names(process$source_files))

# -----------------------------------------------------------------------------
# 1. Load FIPS lookup for state name conversion
# -----------------------------------------------------------------------------
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(fips = geography, state_name = geography_name)

gas_state_nnds <- vroom::vroom('../nnds/standard/data.csv.gz', show_col_types = FALSE) %>%
   dplyr::select(time, mmwr_year, mmwr_week, geography,
   streptococcal_toxic_shock_syndrome) %>%
       rename(year = mmwr_year,
          week = mmwr_week,
          value = streptococcal_toxic_shock_syndrome) %>%
     arrange(geography, year, week) %>%
     group_by(geography, year) %>%
     tidyr::fill(value, .direction = "down") %>%
     mutate(new_value = value - lag(value, default = 0),
      date = as.Date(time, format = "%m-%d-%Y")
     )%>%
  ungroup() %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(geography = if_else(geography == "00", "United States", state_name)) %>%
  select(-state_name) %>%
  filter(geography != "United States") %>%
  mutate(source = "cdc_stss_cases_nnds_cum") %>%
  dplyr::select(geography, date, year, week, value , source, new_value)

# -----------------------------------------------------------------------------
# 2. Load ABCs GAS standard files
# -----------------------------------------------------------------------------
abcs_gas <- vroom::vroom('../abcs_gas/standard/data.csv.gz', show_col_types = FALSE)
abcs_gas_syndromes <- vroom::vroom('../abcs_gas/standard/data_syndromes.csv.gz', show_col_types = FALSE)
abcs_gas_resistance <- vroom::vroom('../abcs_gas/standard/data_resistance.csv.gz', show_col_types = FALSE)
abcs_gas_emm <- vroom::vroom('../abcs_gas/standard/data_emm.csv.gz', show_col_types = FALSE)

# -----------------------------------------------------------------------------
# 3. Write outputs
# -----------------------------------------------------------------------------
dir.create("dist", showWarnings = FALSE)

arrow::write_parquet(abcs_gas, "dist/abcs_gas.parquet")
arrow::write_parquet(abcs_gas_syndromes, "dist/abcs_gas_syndromes.parquet")
arrow::write_parquet(abcs_gas_resistance, "dist/abcs_gas_resistance.parquet")
arrow::write_parquet(abcs_gas_emm, "dist/abcs_gas_emm.parquet")