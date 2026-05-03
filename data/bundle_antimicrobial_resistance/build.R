# =============================================================================
# Bundle: Antimicrobial Resistance
# Combines: NARMS Human Clinical, Retail Meats, Animal Pathogen, Food Animals
# Output:
#   1. resistance_by_agent.parquet  - All sources, per-antimicrobial resistance
#   2. resistance_by_pattern.parquet - Human clinical multi-drug resistance patterns
# =============================================================================

library(dplyr)
library(arrow)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Load FIPS lookup for geography name conversion
# -----------------------------------------------------------------------------
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(fips = geography, geography_name)

# -----------------------------------------------------------------------------
# 2. Load standardized source files
# -----------------------------------------------------------------------------
human_agent   <- vroom::vroom("../narms/standard/data_resistance_agent.csv.gz", show_col_types = FALSE)
human_pattern <- vroom::vroom("../narms/standard/data_resistance_pattern.csv.gz", show_col_types = FALSE)
retail_meats  <- vroom::vroom("../narms/standard/data_retail_meats.csv.gz", show_col_types = FALSE)
animal_path   <- vroom::vroom("../narms/standard/data_animal_pathogen.csv.gz", show_col_types = FALSE)
food_animals  <- vroom::vroom("../narms/standard/data_food_animals.csv.gz", show_col_types = FALSE)

# =============================================================================
# RESISTANCE BY AGENT (all 4 sources combined)
# =============================================================================

# -----------------------------------------------------------------------------
# 3a. Human clinical — by agent
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 3b. Retail meats
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 3c. Animal pathogen
# -----------------------------------------------------------------------------
animal_long <- animal_path %>%
  mutate(
    source = "FDA Animal Pathogen"
  ) %>%
  select(
    geography, time, source, genus,
    antimicrobial, host_species, collection_source,
    pct_resistant, n_resistant, n_tested, mic50, mic90
  )

# -----------------------------------------------------------------------------
# 3d. Food-producing animals
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 4. Combine all sources (bind_rows fills missing columns with NA)
# -----------------------------------------------------------------------------
resistance_agent <- bind_rows(
  human_long,
  retail_long,
  animal_long,
  food_long
) %>%
  # Convert geography FIPS to state names for display
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
  resistance_agent,
  "dist/resistance_by_agent.parquet",
  compression = "snappy"
)
message(sprintf("Wrote %d rows to dist/resistance_by_agent.parquet", nrow(resistance_agent)))

# =============================================================================
# RESISTANCE BY PATTERN (human clinical only)
# =============================================================================

resistance_pattern <- human_pattern %>%
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
  resistance_pattern,
  "dist/resistance_by_pattern.parquet",
  compression = "snappy"
)
message(sprintf("Wrote %d rows to dist/resistance_by_pattern.parquet", nrow(resistance_pattern)))
