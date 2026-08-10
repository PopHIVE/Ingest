# =============================================================================
# Bundle: Group A and Group B Streptococcus
#
# Combines:
#   epic_resp_infections  quarterly_gas.csv.gz - Epic Cosmos strep throat
#                         patients, by state and age
#   nnds                  streptococcal toxic shock syndrome, weekly by state
#   abcs                  CDC ABCs Group A and Group B Streptococcus, annual
#                         national (the strep_* / gas_* / gbs_* files; the
#                         pneumococcal data in that source is not used here)
#
# One dist parquet per contributing source standard file, each long format:
#   geography       state name, or "United States" for the national total
#   geography_fips  the FIPS code for that geography ("00" = national)
#   time            ISO period end date
#   measure         which measure the row reports
#   value           the plotting column
#   not_reported    1 where the source did not report that measure (value is NA)
#   (plus whatever dimension columns that source file carries)
# =============================================================================

library(dplyr)
library(tidyr)
library(arrow)

process <- dcf::dcf_process_record()

dir.create("dist", showWarnings = FALSE)

# -----------------------------------------------------------------------------
# 0. FIPS <-> state name lookup. Dist files carry the name for readability and
#    the FIPS code for joining.
# -----------------------------------------------------------------------------
state_lookup <- vroom::vroom(
  "../../resources/all_fips.csv.gz",
  show_col_types = FALSE
) %>%
  filter(nchar(geography) == 2) %>%
  select(geography_fips = geography, geography_name)

keep_geographies <- c(state.name, "District of Columbia", "United States")

add_geography_names <- function(df) {
  df %>%
    rename(geography_fips = geography) %>%
    left_join(state_lookup, by = "geography_fips") %>%
    mutate(geography = if_else(geography_fips == "00", "United States",
                               geography_name)) %>%
    filter(geography %in% keep_geographies) %>%
    select(-geography_name) %>%
    relocate(geography, geography_fips)
}

DIMS <- c("pathogen", "age", "sex", "race_ethnicity", "onset")

# In the wide standard files a not-reported flag can cover several measures at
# once, so it is 1 wherever ANY of them is absent. Carrying that flag straight
# through to long format would over-mark: the serotype grouping flag is 1 on
# every row, but only one of its three columns is missing per row. Once melted,
# the per-row question is simply "was THIS measure reported for THIS row", which
# is exactly whether the value is absent.
#
# That is only safe because the ingest guarantees every NA in these files is a
# not-reported case - it errors if a column contains NAs without a covering
# flag. Assert the source really did carry a flag for each sparse column, so
# this stays true if the upstream layout changes.
assert_flags_cover_nas <- function(d, meas_cols, flag_cols, path) {
  sparse <- meas_cols[vapply(d[meas_cols], function(x) any(is.na(x)), logical(1))]
  for (m in sparse) {
    na <- is.na(d[[m]])
    covered <- any(vapply(flag_cols, function(f) all(d[[f]][na] == 1L), logical(1)))
    if (!covered) {
      stop("bundle_gas: ", path, " column ", m,
           " has NAs that no not-reported flag covers.")
    }
  }
}

# Read a source standard file, melt its measures to long, and attach the
# not-reported flag that applies to each row's measure.
melt_to_parquet <- function(path, out, strip = "^abcs_(gas_|gbs_)?") {
  d <- vroom::vroom(
    file.path("..", path),
    show_col_types = FALSE,
    # Explicit id types; several of these files have sparse numeric columns
    # where vroom's guessing can otherwise infer logical and blank out values
    col_types = vroom::cols(geography = "c", time = "D", .default = "?")
  ) %>%
    add_geography_names()

  flag_cols <- grep("^abcs_not_reported_flag_", names(d), value = TRUE)
  id_cols <- c("geography", "geography_fips", "time", intersect(DIMS, names(d)))
  meas_cols <- setdiff(names(d), c(id_cols, flag_cols))

  assert_flags_cover_nas(d, meas_cols, flag_cols, path)

  d %>%
    mutate(across(all_of(meas_cols), as.numeric)) %>%
    pivot_longer(all_of(meas_cols), names_to = "measure", values_to = "value") %>%
    mutate(not_reported = as.integer(is.na(value))) %>%
    mutate(measure = sub(strip, "", measure)) %>%
    select(all_of(c(id_cols, "measure", "value", "not_reported"))) %>%
    arrange(across(all_of(c(id_cols, "measure")))) %>%
    arrow::write_parquet(file.path("dist", out))
}

# -----------------------------------------------------------------------------
# 1. Epic Cosmos strep throat (from epic_resp_infections)
#    Two upstream suppression flags: the numerator flag covers both the count
#    and the percent (the percent derives from that same cell), the denominator
#    flag covers the patient total. Each measure gets its own.
# -----------------------------------------------------------------------------
epic_gas <- vroom::vroom(
  "../epic_resp_infections/standard/quarterly_gas.csv.gz",
  show_col_types = FALSE,
  col_types = vroom::cols(geography = "c", time = "D", age = "c",
                          .default = vroom::col_double())
) %>%
  add_geography_names() %>%
  select(
    geography, geography_fips, time, age,
    n_strep_throat    = epic_n_strep_throat,
    pct_strep_throat  = epic_pct_strep_throat,
    n_patients        = epic_n_patients,
    .numerator_flag   = epic_strep_throat_suppressed_flag,
    .denominator_flag = epic_n_patients_suppressed_flag
  ) %>%
  pivot_longer(
    c(n_strep_throat, pct_strep_throat, n_patients),
    names_to = "measure", values_to = "value"
  ) %>%
  mutate(
    suppressed = if_else(measure == "n_patients",
                         .denominator_flag, .numerator_flag)
  ) %>%
  select(geography, geography_fips, time, age, measure, value, suppressed) %>%
  arrange(geography, time, age, measure)

arrow::write_parquet(epic_gas, "dist/epic_gas.parquet")

# -----------------------------------------------------------------------------
# 2. NNDSS streptococcal toxic shock syndrome
#    NNDSS publishes a cumulative year-to-date count that resets each MMWR year
#    (national 2024 runs 5 -> 647 across weeks 1-52), so it is de-accumulated
#    into a weekly-incident series. Both forms are emitted; they are not
#    additive.
#
#    Note this is the only Group A measure NNDSS carries - streptococcal toxic
#    shock syndrome is nationally notifiable, but invasive Group A disease
#    generally and Group B disease are not, so there is no broader NNDSS series
#    to draw on.
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
  add_geography_names() %>%
  arrange(geography, mmwr_year, mmwr_week) %>%
  group_by(geography, mmwr_year) %>%
  # The cumulative count resets each MMWR year, so the year's first week is
  # itself the increment (default = 0)
  mutate(
    stss_cases_weekly = stss_cases_cumulative -
      lag(stss_cases_cumulative, default = 0)
  ) %>%
  ungroup()

# NNDSS revises earlier weeks downward on occasion, which surfaces as a
# negative increment. Report rather than silently clamp.
n_negative <- sum(nnds_stss$stss_cases_weekly < 0, na.rm = TRUE)
if (n_negative > 0) {
  message(
    "NNDSS: ", n_negative, " of ", nrow(nnds_stss),
    " weekly increments are negative (downward revisions to the cumulative ",
    "count); left as reported."
  )
}

nnds_stss <- nnds_stss %>%
  select(geography, geography_fips, time,
         stss_cases_weekly, stss_cases_cumulative) %>%
  pivot_longer(c(stss_cases_weekly, stss_cases_cumulative),
               names_to = "measure", values_to = "value") %>%
  filter(!is.na(value)) %>%
  arrange(geography, time, measure)

arrow::write_parquet(nnds_stss, "dist/nnds_stss.parquet")

# -----------------------------------------------------------------------------
# 3. CDC ABCs, Group A and Group B (national only, annual).
#    Rates, counts and resistance carry both pathogens in one file keyed by the
#    `pathogen` column. Syndromes and typing are per-pathogen upstream because
#    they are not comparable measures - Group A syndromes are a rate per
#    100,000 while Group B's are a percent, and emm types and capsular
#    serotypes are different things entirely.
# -----------------------------------------------------------------------------
melt_to_parquet("abcs/standard/strep_rates.csv.gz",      "abcs_strep_rates.parquet")
melt_to_parquet("abcs/standard/strep_counts.csv.gz",     "abcs_strep_counts.parquet")
melt_to_parquet("abcs/standard/strep_resistance.csv.gz", "abcs_strep_resistance.parquet")
melt_to_parquet("abcs/standard/gas_syndromes.csv.gz",    "abcs_gas_syndromes.parquet")
melt_to_parquet("abcs/standard/gas_emm.csv.gz",          "abcs_gas_emm.parquet")
melt_to_parquet("abcs/standard/gbs_syndromes.csv.gz",    "abcs_gbs_syndromes.parquet")
melt_to_parquet("abcs/standard/gbs_serotypes.csv.gz",    "abcs_gbs_serotypes.parquet")
melt_to_parquet("abcs/standard/gbs_alph.csv.gz",         "abcs_gbs_alph.parquet")
