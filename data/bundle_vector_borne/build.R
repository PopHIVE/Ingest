# =============================================================================
# Bundle: Vector-Borne Diseases
# Combines: arbonet (CDC ArboNET arboviral disease surveillance, state + county),
#           nnds (NNDSS vector-borne/tick-borne/travel-associated disease
#           case counts)
# Output:
#   vector_borne.parquet - ArboNET state- and county-level annual measures +
#                           NNDSS weekly case counts (de-cumulated from the
#                           source's cumulative year-to-date counts), long
#                           format, distinguished by `source` and
#                           `geography_level`
# =============================================================================

library(dplyr)
library(arrow)
library(reshape2)

process <- dcf::dcf_process_record()
standard_files <- paste0("../", names(process$source_files))

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

# Territories are state-level rows in all_fips.csv.gz but have no
# geography_name (only a postal abbreviation) -- fill those in by hand so
# ArboNET's Puerto Rico rows and NNDSS's territory rows both get readable names
state_name_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  mutate(
    geography_name = case_when(
      !is.na(geography_name) ~ geography_name,
      state == "AS" ~ "American Samoa",
      state == "GU" ~ "Guam",
      state == "MP" ~ "Northern Mariana Islands",
      state == "PR" ~ "Puerto Rico",
      state == "VI" ~ "U.S. Virgin Islands",
      TRUE ~ geography_name
    )
  ) %>%
  select(geography, geography_name)

# -----------------------------------------------------------------------------
# 1. ArboNET (state) + NNDSS: long-format annual/weekly case counts
# -----------------------------------------------------------------------------
arbonet_state_long <- vroom::vroom('../arbonet/standard/data_state.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(
    geography_level = if_else(fips == '00', 'national', 'state'),
    geography_name = if_else(fips == '00', 'United States', geography_name)
  ) %>%
  filter(!is.na(geography_name)) %>%
  dplyr::select(fips, geography_level, date = time, starts_with('arbonet_')) %>%
  reshape2::melt(id.vars = c('fips', 'geography_level', 'date'), variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC ArboNET') %>%
  filter(!is.na(value))

nnds_vars <- c(
  'arboviral_diseases_chikungunya_virus_disease',
  'arboviral_diseases_eastern_equine_encephalitis_virus_disease',
  'arboviral_diseases_jamestown_canyon_virus_disease',
  'arboviral_diseases_la_crosse_virus_disease',
  'arboviral_diseases_powassan_virus_disease',
  'arboviral_diseases_st_louis_encephalitis_virus_disease',
  'arboviral_diseases_west_nile_virus_disease',
  'arboviral_diseases_western_equine_encephalitis_virus_disease',
  'babesiosis',
  'dengue_virus_infections_dengue',
  'dengue_virus_infections_dengue_like_illness',
  'dengue_virus_infections_severe_dengue',
  'ehrlichiosis_and_anaplasmosis_anaplasma_phagocytophilum_infection',
  'ehrlichiosis_and_anaplasmosis_ehrlichia_chaffeensis_infection',
  'ehrlichiosis_and_anaplasmosis_ehrlichia_ewingii_infection',
  'ehrlichiosis_and_anaplasmosis_undetermined_ehrlichiosis_anaplasmosis',
  'malaria',
  'zika_virus_disease_non_congenital'
)

nnds_long <- vroom::vroom('../nnds/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(
    geography_level = if_else(fips == '00', 'national', 'state'),
    geography_name = if_else(fips == '00', 'United States', geography_name)
  ) %>%
  filter(!is.na(geography_name)) %>%
  dplyr::select(fips, geography_level, date = time, mmwr_year, mmwr_week, all_of(nnds_vars)) %>%
  reshape2::melt(id.vars = c('fips', 'geography_level', 'date', 'mmwr_year', 'mmwr_week'), variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value))) %>%
  filter(!is.na(value)) %>%
  # nnds/standard/data.csv.gz stores cumulative year-to-date counts; de-cumulate
  # into weekly incident counts by differencing within each fips/measure/
  # mmwr_year series (the series resets to the raw YTD value at the first week
  # of each MMWR year)
  arrange(fips, measure, mmwr_year, mmwr_week) %>%
  group_by(fips, measure, mmwr_year) %>%
  mutate(value = value - lag(value, default = 0)) %>%
  ungroup() %>%
  dplyr::select(-mmwr_year, -mmwr_week) %>%
  mutate(source = 'CDC NNDSS')

# -----------------------------------------------------------------------------
# 2. ArboNET (county): long-format annual measures. NNDSS is not reported at
#    the county level. The national ("00") row present in the county file is
#    dropped since it duplicates the "United States" rows above.
# -----------------------------------------------------------------------------
arbonet_county_long <- vroom::vroom('../arbonet/standard/data_county.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography), geography != '00') %>%
  rename(fips = geography) %>%
  mutate(geography_level = 'county') %>%
  dplyr::select(fips, geography_level, date = time, starts_with('arbonet_')) %>%
  reshape2::melt(id.vars = c('fips', 'geography_level', 'date'), variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC ArboNET') %>%
  filter(!is.na(value))

# -----------------------------------------------------------------------------
# 3. Combine all three into a single long-format dist file. `geography_level`
#    distinguishes granularity: `fips` holds "00" for national rows, a
#    2-digit state/territory FIPS code for state rows, and a 5-digit county
#    FIPS code for county rows.
# -----------------------------------------------------------------------------
vector_borne <- bind_rows(arbonet_state_long, nnds_long, arbonet_county_long) %>%
  arrange(source, measure, geography_level, fips, date)

arrow::write_parquet(vector_borne, "dist/vector_borne.parquet", compression = "snappy")
message(sprintf("Wrote %d rows to dist/vector_borne.parquet", nrow(vector_borne)))
