# =============================================================================
# Bundle: Enteric Diseases
# Combines: nnds (NNDSS enteric/gastrointestinal disease case counts)
# Output: enteric_diseases.parquet - state/national case counts in long format
# =============================================================================

library(dplyr)
library(arrow)
library(reshape2)

process <- dcf::dcf_process_record()
standard_files <- paste0("../", names(process$source_files))

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
state_name_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)

nnds_vars <- c(
  'campylobacteriosis',
  'cholera',
  'giardiasis',
  'salmonellosis_excluding_salmonella_typhi_infection_and_salmonella_paratyphi_infection',
  'cyclosporiasis',
  'salmonella_typhi_infection',
  'salmonella_paratyphi_infection',
  'shiga_toxin_producing_escherichia_coli_stec',
  'shigellosis',
  'salmonella_paratyphi_infection_2'
)

enteric_diseases <- vroom::vroom('../nnds/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  dplyr::select(geography, date = time, year = mmwr_year, week = mmwr_week, all_of(nnds_vars)) %>%
  reshape2::melt(id.vars = c('geography', 'date', 'year', 'week'), variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC NNDSS') %>%
  filter(!is.na(value)) %>%
  arrange(measure, geography, date)

arrow::write_parquet(enteric_diseases, "dist/enteric_diseases.parquet")

