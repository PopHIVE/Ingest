# =============================================================================
# Bundle: Enteric Diseases
# Combines: nnds (NNDSS enteric/gastrointestinal disease case counts),
#           beam (CDC BEAM Dashboard enteric pathogen isolate counts/rates),
#           narms (antimicrobial resistance surveillance for enteric pathogens),
#           epic_diarrhea (Epic Cosmos all-cause diarrhea encounters)
# Output:
#   1. enteric_diseases.parquet      - NNDSS case counts + BEAM isolate counts,
#                                       long format, distinguished by `source`
#   2. resistance_by_agent.parquet   - NARMS resistance by antimicrobial agent,
#                                       across all NARMS programs
#   3. resistance_by_pattern.parquet - NARMS multi-drug resistance patterns,
#                                       human clinical isolates only
#   4. epic_diarrhea.parquet         - Epic Cosmos weekly all-cause diarrhea
#                                       encounters, long format, by age
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

# -----------------------------------------------------------------------------
# 3. Epic Cosmos: weekly all-cause diarrhea encounters, by state and age
#    Kept in a separate dist file from enteric_diseases.parquet because it is
#    age-stratified. Only data_weekly.csv.gz is bundled; the cyclospora lab
#    testing file (weekly_tests.csv.gz) is not included.
# -----------------------------------------------------------------------------
# Each value measure is paired with the suppression flag that applies to it.
# Percentages are flagged as suppressed if either the numerator or the
# denominator count was suppressed.
epic_flag_map <- c(
  epic_n_ed_diarrhea             = "epic_suppressed_flag_ed_diarrhea",
  epic_n_ed_encounters_weekly    = "epic_suppressed_flag_ed_encounters_weekly",
  epic_pct_ed_diarrhea           = "epic_suppressed_flag_pct_ed_diarrhea",
  epic_n_all_diarrhea            = "epic_suppressed_flag_all_diarrhea",
  epic_n_encounters_total_weekly = "epic_suppressed_flag_encounters_total_weekly",
  epic_pct_all_diarrhea          = "epic_suppressed_flag_pct_all_diarrhea"
)

epic_wide <- vroom::vroom(
  '../epic_diarrhea/standard/data_weekly.csv.gz',
  show_col_types = FALSE
) %>%
  filter(!is.na(geography)) %>%
  rename(fips = geography) %>%
  left_join(state_name_lookup, by = c("fips" = "geography")) %>%
  mutate(geography = if_else(fips == '00', 'United States', geography_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States')) %>%
  mutate(
    epic_suppressed_flag_pct_ed_diarrhea = pmax(
      epic_suppressed_flag_ed_diarrhea,
      epic_suppressed_flag_ed_encounters_weekly
    ),
    epic_suppressed_flag_pct_all_diarrhea = pmax(
      epic_suppressed_flag_all_diarrhea,
      epic_suppressed_flag_encounters_total_weekly
    )
  ) %>%
  dplyr::select(geography, date = time, age, all_of(names(epic_flag_map)),
                all_of(unique(unname(epic_flag_map))))

epic_values <- epic_wide %>%
  dplyr::select(geography, date, age, all_of(names(epic_flag_map))) %>%
  reshape2::melt(id.vars = c('geography', 'date', 'age'),
                 variable.name = 'measure', value.name = 'value') %>%
  mutate(measure = as.character(measure),
         value = suppressWarnings(as.numeric(value)))

epic_flags <- epic_wide %>%
  dplyr::select(geography, date, age, all_of(unique(unname(epic_flag_map)))) %>%
  reshape2::melt(id.vars = c('geography', 'date', 'age'),
                 variable.name = 'flag_column', value.name = 'suppressed_flag') %>%
  mutate(flag_column = as.character(flag_column),
         suppressed_flag = suppressWarnings(as.numeric(suppressed_flag)))

epic_diarrhea <- epic_values %>%
  mutate(flag_column = unname(epic_flag_map[measure])) %>%
  left_join(epic_flags, by = c('geography', 'date', 'age', 'flag_column')) %>%
  dplyr::select(geography, date, age, measure, value, suppressed_flag) %>%
  mutate(source = 'Epic Cosmos') %>%
  filter(!is.na(value)) %>%
  arrange(measure, geography, age, date)

arrow::write_parquet(
  epic_diarrhea,
  "dist/epic_diarrhea.parquet",
  compression = "snappy"
)
message(sprintf("Wrote %d rows to dist/epic_diarrhea.parquet", nrow(epic_diarrhea)))

