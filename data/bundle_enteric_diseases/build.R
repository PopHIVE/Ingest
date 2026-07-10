# =============================================================================
# Bundle: Enteric Diseases
# Combines: nnds (NNDSS enteric/gastrointestinal disease case counts),
#           beam (CDC BEAM Dashboard enteric pathogen isolate counts/rates),
#           narms (antimicrobial resistance surveillance for enteric pathogens)
# Output:
#   1. enteric_diseases.parquet      - NNDSS case counts + BEAM isolate counts,
#                                       long format, distinguished by `source`
#   2. resistance_by_agent.parquet   - NARMS resistance by antimicrobial agent,
#                                       across all NARMS programs
#   3. resistance_by_pattern.parquet - NARMS multi-drug resistance patterns,
#                                       human clinical isolates only
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

# -----------------------------------------------------------------------------
# 1. NNDSS + BEAM: long-format case/isolate counts
# -----------------------------------------------------------------------------
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

nnds_long <- vroom::vroom('../nnds/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  dplyr::select(geography, date = time, all_of(nnds_vars)) %>%
  reshape2::melt(id.vars = c('geography', 'date'), variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC NNDSS') %>%
  filter(!is.na(value))

beam_long <- vroom::vroom('../beam/standard/data.csv.gz', show_col_types = FALSE) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  dplyr::select(geography, date = time, starts_with('beam_')) %>%
  reshape2::melt(id.vars = c('geography', 'date'), variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value)),
         source = 'CDC BEAM Dashboard') %>%
  filter(!is.na(value))

enteric_diseases <- bind_rows(nnds_long, beam_long) %>%
  arrange(source, measure, geography, date)

arrow::write_parquet(enteric_diseases, "dist/enteric_diseases.parquet")
message(sprintf("Wrote %d rows to dist/enteric_diseases.parquet", nrow(enteric_diseases)))

# -----------------------------------------------------------------------------
# 2. NARMS: antimicrobial resistance surveillance for enteric pathogens
#    (same harmonization used in bundle_antimicrobial_resistance)
# -----------------------------------------------------------------------------
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(fips = geography, geography_name)

human_agent   <- vroom::vroom("../narms/standard/data_resistance_agent.csv.gz", show_col_types = FALSE)
human_pattern <- vroom::vroom("../narms/standard/data_resistance_pattern.csv.gz", show_col_types = FALSE)
retail_meats  <- vroom::vroom("../narms/standard/data_retail_meats.csv.gz", show_col_types = FALSE)
animal_path   <- vroom::vroom("../narms/standard/data_animal_pathogen.csv.gz", show_col_types = FALSE)
food_animals  <- vroom::vroom("../narms/standard/data_food_animals.csv.gz", show_col_types = FALSE)

human_long <- human_agent %>%
  mutate(
    source = "NARMS Now (Human Clinical)",
    antimicrobial = antimicrobial_agent
  ) %>%
  select(
    geography, time, source, genus,
    species_serotype, antimicrobial_class, antimicrobial,
    test_method, pct_resistant, n_resistant, n_tested
  )

retail_long <- retail_meats %>%
  mutate(
    source = "FDA Retail Meats",
    species_serotype = if_else(
      is.na(serotype),
      species,
      paste0(species, " ", serotype)
    )
  ) %>%
  select(
    geography, time, source, genus,
    species_serotype, antimicrobial, meat_source,
    pct_resistant, n_resistant, n_tested, mic50, mic90
  )

animal_long <- animal_path %>%
  mutate(source = "FDA Animal Pathogen") %>%
  select(
    geography, time, source, genus,
    antimicrobial, host_species, collection_source,
    pct_resistant, n_resistant, n_tested, mic50, mic90
  )

food_long <- food_animals %>%
  mutate(
    source = paste0("FDA Food Animals (", source_program, ")"),
    species_serotype = if_else(
      is.na(serotype),
      species,
      paste0(species, " ", serotype)
    )
  ) %>%
  select(
    geography, time, source, genus,
    species_serotype, antimicrobial, host_species,
    source_type,
    pct_resistant, n_resistant, n_tested, mic50, mic90
  )

resistance_by_agent <- bind_rows(
  human_long,
  retail_long,
  animal_long,
  food_long
) %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(
    geography = case_when(
      geography == "00" ~ "United States",
      !is.na(geography_name) ~ geography_name,
      TRUE ~ geography
    )
  ) %>%
  select(-geography_name) %>%
  arrange(source, geography, time, genus, antimicrobial)

arrow::write_parquet(
  resistance_by_agent,
  "dist/resistance_by_agent.parquet",
  compression = "snappy"
)
message(sprintf("Wrote %d rows to dist/resistance_by_agent.parquet", nrow(resistance_by_agent)))

resistance_by_pattern <- human_pattern %>%
  mutate(source = "NARMS Now (Human Clinical)") %>%
  left_join(state_fips_lookup, by = c("geography" = "fips")) %>%
  mutate(
    geography = case_when(
      geography == "00" ~ "United States",
      !is.na(geography_name) ~ geography_name,
      TRUE ~ geography
    )
  ) %>%
  select(-geography_name) %>%
  select(
    geography, time, source, genus, species_serotype,
    pattern, test_method, pct_resistant, n_resistant, n_tested
  ) %>%
  arrange(geography, time, genus, pattern)

arrow::write_parquet(
  resistance_by_pattern,
  "dist/resistance_by_pattern.parquet",
  compression = "snappy"
)
message(sprintf("Wrote %d rows to dist/resistance_by_pattern.parquet", nrow(resistance_by_pattern)))

