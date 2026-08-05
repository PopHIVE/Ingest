# =============================================================================
# Bundle: Group A Streptococcus (GAS)
# Combines: epic_gas  (Epic Cosmos strep throat patients, quarterly, by state/age)
#           nnds      (NNDSS streptococcal toxic shock syndrome, weekly, by state)
#           abcs_gas  (CDC ABCs Group A Streptococcus, annual, national)
# Outputs (all long format, `value` is the plotting column, `geography` holds
# state names / "United States"):
#   1. epic_gas.parquet             - strep throat counts, percent, denominator
#   2. nnds_stss.parquet            - STSS cases, weekly-incident and cumulative
#   3. abcs_gas.parquet             - GAS case/death rates and counts
#   4. abcs_gas_syndromes.parquet   - clinical syndrome distribution
#   5. abcs_gas_resistance.parquet  - antibiotic resistance
#   6. abcs_gas_emm.parquet         - emm type distribution
# =============================================================================

library(dplyr)
library(tidyr)
library(arrow)

process <- dcf::dcf_process_record()

dir.create("dist", showWarnings = FALSE)

# -----------------------------------------------------------------------------
# 0. FIPS -> state name lookup (dist files use names, not FIPS, for states)
# -----------------------------------------------------------------------------
state_name_lookup <- vroom::vroom(
  "../../resources/all_fips.csv.gz",
  show_col_types = FALSE
) %>%
  filter(nchar(geography) == 2) %>%
  select(fips = geography, geography_name)

# Keep only the 50 states, DC, and the national total
keep_geographies <- c(state.name, "District of Columbia", "United States")

fips_to_name <- function(df) {
  df %>%
    rename(fips = geography) %>%
    left_join(state_name_lookup, by = "fips") %>%
    mutate(geography = if_else(fips == "00", "United States", geography_name)) %>%
    filter(geography %in% keep_geographies) %>%
    select(-fips, -geography_name) %>%
    relocate(geography)
}

# -----------------------------------------------------------------------------
# 1. Epic Cosmos strep throat -> long
#    Two suppression flags upstream: the numerator flag covers both the count
#    and the percent (the percent is derived from that same cell), the
#    denominator flag covers the patient total. Map each measure to its own.
# -----------------------------------------------------------------------------
epic_gas <- vroom::vroom(
  "../epic_gas/standard/data.csv.gz",
  show_col_types = FALSE,
  col_types = vroom::cols(geography = "c", time = "D", age = "c")
) %>%
  fips_to_name() %>%
  select(
    geography, time, age,
    n_strep_throat   = epic_n_strep_throat,
    pct_strep_throat = epic_pct_strep_throat,
    n_patients       = epic_n_patients,
    .numerator_flag  = epic_strep_throat_suppressed_flag,
    .denominator_flag = epic_n_patients_suppressed_flag
  ) %>%
  pivot_longer(
    c(n_strep_throat, pct_strep_throat, n_patients),
    names_to = "measure",
    values_to = "value"
  ) %>%
  mutate(
    suppressed = if_else(measure == "n_patients", .denominator_flag, .numerator_flag)
  ) %>%
  select(geography, time, age, measure, value, suppressed) %>%
  arrange(geography, time, age, measure)

arrow::write_parquet(epic_gas, "dist/epic_gas.parquet")

# -----------------------------------------------------------------------------
# 2. NNDSS streptococcal toxic shock syndrome -> long
#    NNDSS reports counts cumulatively within each MMWR year (national 2024
#    ramps 5 -> 647 across weeks 1-52), so the series must be de-accumulated
#    before it can be plotted as weekly incidence. Both forms are emitted.
# -----------------------------------------------------------------------------
nnds_stss <- vroom::vroom(
  "../nnds/standard/data.csv.gz",
  show_col_types = FALSE,
  col_select = c(time, mmwr_year, mmwr_week, geography,
                 streptococcal_toxic_shock_syndrome),
  col_types = vroom::cols(geography = "c", time = "D")
) %>%
  rename(stss_cases_cumulative = streptococcal_toxic_shock_syndrome) %>%
  filter(!is.na(geography)) %>%
  fips_to_name() %>%
  arrange(geography, mmwr_year, mmwr_week) %>%
  group_by(geography, mmwr_year) %>%
  # cumulative counts reset each MMWR year, so the year's first week is itself
  # the increment (default = 0)
  mutate(
    stss_cases_weekly = stss_cases_cumulative -
      lag(stss_cases_cumulative, default = 0)
  ) %>%
  ungroup()

# NNDSS revises prior weeks downward on occasion, which shows up as a negative
# increment. Report rather than silently clamp.
n_negative <- sum(nnds_stss$stss_cases_weekly < 0, na.rm = TRUE)
if (n_negative > 0) {
  message(
    "NNDSS: ", n_negative, " of ", nrow(nnds_stss),
    " weekly increments are negative (downward revisions to the cumulative ",
    "count); left as-is."
  )
}

nnds_stss <- nnds_stss %>%
  select(geography, time, stss_cases_weekly, stss_cases_cumulative) %>%
  pivot_longer(
    c(stss_cases_weekly, stss_cases_cumulative),
    names_to = "measure",
    values_to = "value"
  ) %>%
  filter(!is.na(value)) %>%
  arrange(geography, time, measure)

arrow::write_parquet(nnds_stss, "dist/nnds_stss.parquet")

# -----------------------------------------------------------------------------
# 3. ABCs Group A Streptococcus -> long (one parquet per upstream file)
#    All four are national-only (geography "00") and annual (YYYY-12-31).
#    Measure names drop the redundant `abcs_gas_` source prefix.
# -----------------------------------------------------------------------------
melt_abcs <- function(file, id_cols) {
  vroom::vroom(
    file.path("../abcs_gas/standard", file),
    show_col_types = FALSE,
    col_types = vroom::cols(geography = "c", time = "D")
  ) %>%
    fips_to_name() %>%
    pivot_longer(
      -all_of(id_cols),
      names_to = "measure",
      values_to = "value"
    ) %>%
    mutate(measure = sub("^abcs_gas_", "", measure)) %>%
    filter(!is.na(value)) %>%
    arrange(across(all_of(c(id_cols, "measure"))))
}

arrow::write_parquet(
  melt_abcs("data.csv.gz", c("geography", "time", "age", "sex", "race_ethnicity")),
  "dist/abcs_gas.parquet"
)

arrow::write_parquet(
  melt_abcs("data_syndromes.csv.gz", c("geography", "time")),
  "dist/abcs_gas_syndromes.parquet"
)

arrow::write_parquet(
  melt_abcs("data_resistance.csv.gz", c("geography", "time")),
  "dist/abcs_gas_resistance.parquet"
)

arrow::write_parquet(
  melt_abcs("data_emm.csv.gz", c("geography", "time")),
  "dist/abcs_gas_emm.parquet"
)
