# =============================================================================
# Bundle: Rural Health
# Combines county- and state-level social, economic, environmental, and health
# resource determinants relevant to rural health into long-format parquet
# files queryable by FIPS code + date + outcome_name.
#
# Sources:
#   - hud_chas/standard/data_county.csv.gz, data_state.csv.gz   (HUD CHAS)
#   - area_health_resource_file/standard/data.csv.gz            (HRSA AHRF)
#   - bls_laus/standard/data_county.csv.gz, data_state.csv.gz   (BLS LAUS)
#   - usda_food_access/standard/data_county.csv.gz              (USDA ERS)
#
# Output:
#   - dist/rural_health_county.parquet
#     One row per county (5-digit FIPS) x time x outcome_name.
#   - dist/rural_health_state.parquet
#     One row per state (2-digit FIPS, "00" = national) x time x outcome_name.
# =============================================================================

library(dplyr)
library(tidyr)
library(vroom)
library(arrow)

# -----------------------------------------------------------------------------
# 1. Load source data
# -----------------------------------------------------------------------------

source_paths <- c(
  hud_county  = "../hud_chas/standard/data_county.csv.gz",
  hud_state   = "../hud_chas/standard/data_state.csv.gz",
  ahrf        = "../area_health_resource_file/standard/data.csv.gz",
  bls_county  = "../bls_laus/standard/data_county.csv.gz",
  bls_state   = "../bls_laus/standard/data_state.csv.gz",
  usda_county = "../usda_food_access/standard/data_county.csv.gz"
)

missing_paths <- source_paths[!file.exists(source_paths)]
if (length(missing_paths) > 0) {
  stop(
    "Missing source files (run the corresponding ingest first):\n",
    paste(" -", missing_paths, collapse = "\n")
  )
}

read_source <- function(path, source_label) {
  vroom(path, show_col_types = FALSE,
        col_types = cols(geography = col_character(), .default = col_guess())) %>%
    filter(!is.na(geography)) %>%
    mutate(
      time = as.Date(time),
      geography = if_else(
        nchar(geography) > 2,
        formatC(as.integer(geography), width = 5, flag = "0"),
        formatC(as.integer(geography), width = 2, flag = "0")
      )
    ) %>%
    pivot_longer(
      cols = -c(geography, time),
      names_to = "outcome_name",
      values_to = "value",
      values_transform = as.numeric
    ) %>%
    filter(!is.na(value)) %>%
    mutate(source = source_label)
}

hud_county  <- read_source(source_paths[["hud_county"]],  "HUD CHAS")
hud_state   <- read_source(source_paths[["hud_state"]],   "HUD CHAS")
ahrf        <- read_source(source_paths[["ahrf"]],        "HRSA AHRF")
bls_county  <- read_source(source_paths[["bls_county"]],  "BLS LAUS")
bls_state   <- read_source(source_paths[["bls_state"]],   "BLS LAUS")
usda_county <- read_source(source_paths[["usda_county"]], "USDA Food Access Research Atlas")

# -----------------------------------------------------------------------------
# 2. Split by geography level and combine
# -----------------------------------------------------------------------------

rural_health_county <- bind_rows(
  hud_county,
  ahrf %>% filter(nchar(geography) == 5),
  bls_county,
  usda_county
) %>%
  filter(nchar(geography) == 5) %>%
  select(geography, time, outcome_name, value, source) %>%
  arrange(outcome_name, geography, time)

rural_health_state <- bind_rows(
  hud_state,
  ahrf %>% filter(nchar(geography) == 2),
  bls_state
) %>%
  filter(nchar(geography) == 2) %>%
  select(geography, time, outcome_name, value, source) %>%
  arrange(outcome_name, geography, time)

# -----------------------------------------------------------------------------
# 3. Validate — no duplicate geography-time-outcome_name rows
# -----------------------------------------------------------------------------

check_dupes <- function(df, label) {
  dupes <- df %>%
    count(geography, time, outcome_name) %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    stop(
      nrow(dupes), " duplicate geography-time-outcome_name combinations in ",
      label, ". Inspect before proceeding."
    )
  }
  invisible(TRUE)
}

check_dupes(rural_health_county, "rural_health_county")
check_dupes(rural_health_state,  "rural_health_state")

# -----------------------------------------------------------------------------
# 4. Write output
# -----------------------------------------------------------------------------

dir.create("dist", showWarnings = FALSE)

write_parquet(rural_health_county, "dist/rural_health_county.parquet", compression = "snappy")
message(sprintf(
  "Wrote %d rows to dist/rural_health_county.parquet (%d counties, %d measures, %s to %s)",
  nrow(rural_health_county),
  n_distinct(rural_health_county$geography),
  n_distinct(rural_health_county$outcome_name),
  format(min(rural_health_county$time), "%Y"),
  format(max(rural_health_county$time), "%Y")
))

write_parquet(rural_health_state, "dist/rural_health_state.parquet", compression = "snappy")
message(sprintf(
  "Wrote %d rows to dist/rural_health_state.parquet (%d states, %d measures, %s to %s)",
  nrow(rural_health_state),
  n_distinct(rural_health_state$geography),
  n_distinct(rural_health_state$outcome_name),
  format(min(rural_health_state$time), "%Y"),
  format(max(rural_health_state$time), "%Y")
))
