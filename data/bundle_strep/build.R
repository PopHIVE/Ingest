# =============================================================================
# Bundle: Group A and Group B Streptococcus
#
# Combines:
#   abcs                  CDC ABCs Group A and Group B Streptococcus, annual
#                         national (the strep_* / gas_* / gbs_* files; the
#                         pneumococcal data in that source is not used here)
#   epic_resp_infections  quarterly_gas.csv.gz - Epic Cosmos strep throat
#                         patients, by state and age
#   nnds                  streptococcal toxic shock syndrome, weekly by state
#                         (Epic and NNDSS are combined into gas_state.parquet)
#
# Two long parquets. All eight ABCs topics are stacked into one file with a
# named column per stratification and "Total" wherever a row is not stratified
# on that dimension. Epic and NNDSS share the second: both are state + national
# Group A series keyed on geography, date and one measure. ABCs stays apart
# because it is national-only and annual and carries fourteen dimension and
# companion columns neither of the others has - merging it would leave 62% of
# the cells as padding.
#
# Shared columns: geography (state name or "United States"), geography_fips,
# date (bundles use `date`; the standard files use `time`), year, measure,
# value.
#
# Companion columns rather than extra measure rows, so a tooltip can be built
# from one line: `n_isolates` is the denominator behind every percent, and
# `n_type` the numerator for emm types - "emm1: 22.5% (99 of 440 isolates)".
# Cases, deaths and survivals stay separate `measure` levels: they are three
# plottable series, not metadata for a single value.
#
# Each companion is blank on most rows, so each carries a `<name>_status` column
# saying whether it is blank because the measure has no such companion or
# because CDC never published it. See the status block below.
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

# vroom attaches `spec` and `problems` attributes to what it reads; those ride
# through dplyr and get serialized into the parquet as R metadata, so two builds
# of identical data can differ byte for byte. These files are committed, so strip
# them and keep the output reproducible.
write_dist <- function(df, file) {
  df <- as.data.frame(df)
  attr(df, "spec") <- NULL
  attr(df, "problems") <- NULL
  arrow::write_parquet(df, file.path("dist", file))
}

# Read everything as text and convert the measure columns explicitly, rather
# than letting vroom guess. The measure columns are sparse - a cell CDC never
# published is NA - and vroom infers a column's type from a sample, so an
# all-NA measure would otherwise come back `logical` and silently blank real
# values elsewhere. Naming each column instead would warn on every file that
# lacks one, since the eight files carry different dimensions.
read_standard <- function(path) {
  vroom::vroom(
    file.path("..", path),
    show_col_types = FALSE,
    col_types = vroom::cols(.default = vroom::col_character())
  ) %>%
    mutate(
      time = as.Date(time),
      across(starts_with("abcs_"), as.numeric)
    ) %>%
    add_geography_names()
}

# A measure the source did not publish is NA there, and stays NA here. Nothing
# is filled in, so a 0 in `value` is always a measured zero.
#
# The companion columns (`n_isolates`, `n_type`) are blank on most rows for two
# unrelated reasons, and a bare NA cannot tell them apart: either the measure has
# no such companion at all (a case rate has no isolate denominator), or CDC
# simply never published it for that row. Each companion therefore carries a
# status column saying which:
#
#   "reported"        the companion holds CDC's published figure
#   "not_reported"    the companion applies to this measure but CDC published
#                     nothing - blank rather than 0, since "22.5% of 0 isolates"
#                     would read as broken
#   "not_applicable"  the measure has no such companion
#
# So a status of "reported" always accompanies a value, and the other two always
# accompany NA. The assertion after the stacks below enforces exactly that.
REPORTED       <- "reported"
NOT_REPORTED   <- "not_reported"
NOT_APPLICABLE <- "not_applicable"

COMPANIONS <- c("n_isolates", "n_type")
status_of <- function(x) paste0(x, "_status")

# -----------------------------------------------------------------------------
# 1. ABCs: stack all eight topics into one file
# -----------------------------------------------------------------------------
DIMS <- c("pathogen", "age", "sex", "race_ethnicity", "onset", "rate_denominator")
ENTITIES <- c("syndrome", "antibiotic", "emm_type", "serotype", "alph_type")

# Melt one standard file into the shared long schema. The standard files already
# carry their dimensions as columns - `antibiotic`, `emm_type`, `serotype` and so
# on - so this only stacks the measure columns and normalises their names.
#
#   measures    output measure level -> source column
#   companions  output name -> source column, for columns carried alongside
#               `value` rather than melted. The files name the same isolate
#               count three ways (`abcs_n_isolates`, `abcs_gbs_n_isolates`,
#               `abcs_gas_emm_n_isolates_total`), so normalising here keeps one
#               `n_isolates` column across the stacks.
stack_abcs <- function(path, measures, companions = character()) {
  d <- read_standard(path)
  companions <- companions[companions %in% names(d)]
  ids <- c("geography", "geography_fips", "time",
           intersect(c(DIMS, ENTITIES), names(d)))

  # Record why a companion is blank, which its own NA cannot say on its own.
  comp_cols <- character()
  for (nm in names(companions)) {
    d[[nm]] <- as.numeric(d[[companions[[nm]]]])
    d[[status_of(nm)]] <- if_else(is.na(d[[nm]]), NOT_REPORTED, REPORTED)
    comp_cols <- c(comp_cols, nm, status_of(nm))
  }

  missing <- setdiff(unname(measures), names(d))
  if (length(missing)) {
    stop("bundle_strep: ", path, " has no column ", paste(missing, collapse = ", "))
  }

  bind_rows(lapply(names(measures), function(m) {
    d %>%
      select(all_of(c(ids, comp_cols)), value = all_of(measures[[m]])) %>%
      mutate(measure = m)
  }))
}

abcs_strep <- bind_rows(
  stack_abcs(
    "abcs/standard/strep_rates.csv.gz",
    c(rate_cases = "abcs_rate_cases", rate_deaths = "abcs_rate_deaths")
  ),
  stack_abcs(
    "abcs/standard/strep_counts.csv.gz",
    c(n_cases = "abcs_N_cases", n_deaths = "abcs_N_deaths",
      n_survivals = "abcs_N_survivals")
  ),
  stack_abcs(
    "abcs/standard/strep_resistance.csv.gz",
    c(pct_resistant = "abcs_pct_resistant"),
    companions = c(n_isolates = "abcs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gas_syndromes.csv.gz",
    c(rate_syndrome = "abcs_gas_rate_syndrome")
  ),
  stack_abcs(
    "abcs/standard/gbs_syndromes.csv.gz",
    c(pct_syndrome = "abcs_gbs_pct_syndrome")
  ),
  stack_abcs(
    "abcs/standard/gbs_serotypes.csv.gz",
    c(pct_serotype = "abcs_gbs_pct_serotype"),
    companions = c(n_isolates = "abcs_gbs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gbs_alph.csv.gz",
    c(pct_alph_type = "abcs_gbs_pct_alph"),
    companions = c(n_isolates = "abcs_gbs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gas_emm.csv.gz",
    c(pct_emm_type = "abcs_gas_emm_pct"),
    companions = c(n_type = "abcs_gas_emm_n",
                   n_isolates = "abcs_gas_emm_n_isolates_total")
  )
) %>%
  rename(date = time) %>%
  mutate(
    year = as.integer(format(date, "%Y")),
    # "Total" fills any dimension a row is not stratified on, so every column is
    # populated and a consumer can filter on it without handling NA
    across(all_of(c(DIMS, ENTITIES)), ~ tidyr::replace_na(as.character(.x), "Total")),
    # A stack that never had a companion contributes neither the value nor its
    # status, so both arrive NA from bind_rows. That is the third case: the
    # measure has no such companion at all.
    across(all_of(status_of(COMPANIONS)),
           ~ tidyr::replace_na(as.character(.x), NOT_APPLICABLE))
  ) %>%
  select(all_of(c("geography", "geography_fips", "date", "year", DIMS, ENTITIES,
                  "measure", "value",
                  "n_type", "n_type_status",
                  "n_isolates", "n_isolates_status"))) %>%
  arrange(across(all_of(c("geography", "date", DIMS, ENTITIES, "measure"))))

if (anyDuplicated(abcs_strep[c("geography", "date", DIMS, ENTITIES, "measure")])) {
  stop("bundle_strep: duplicate index rows in abcs_strep.")
}

# `value` may be NA - that is how the file says CDC published nothing - but no
# column a consumer filters on may be, and each companion must be populated
# exactly when its status says "reported".
index_cols <- c("geography", "geography_fips", "date", "year", DIMS, ENTITIES,
                "measure")
if (anyNA(abcs_strep[index_cols])) {
  stop("bundle_strep: NA in an index, dimension or entity column.")
}
for (cc in COMPANIONS) {
  if (!identical(is.na(abcs_strep[[cc]]),
                 abcs_strep[[status_of(cc)]] != REPORTED)) {
    stop("bundle_strep: ", cc, " disagrees with ", status_of(cc), ".")
  }
}

write_dist(abcs_strep, "abcs_strep.parquet")

# -----------------------------------------------------------------------------
# 2. Epic Cosmos strep throat (from epic_resp_infections)
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
                         .denominator_flag, .numerator_flag),
    date = time,
    year = as.integer(format(time, "%Y"))
  ) %>%
  mutate(source = "Epic Cosmos", week = NA_integer_) %>%
  select(geography, geography_fips, date, year, week, age,
         source, measure, value, suppressed)

# -----------------------------------------------------------------------------
# 3. NNDSS streptococcal toxic shock syndrome
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

# NNDSS revises earlier weeks downward on occasion, which surfaces as a negative
# increment. Following the bundle_measles convention, these are kept as reported
# rather than clamped - the transparency is deliberate, and plots should cut the
# y axis at 0 instead.
n_negative <- sum(nnds_stss$stss_cases_weekly < 0, na.rm = TRUE)
if (n_negative > 0) {
  message(
    "NNDSS: ", n_negative, " of ", nrow(nnds_stss),
    " weekly increments are negative (downward revisions to the cumulative ",
    "count); kept as reported, per the bundle_measles convention."
  )
}

nnds_stss <- nnds_stss %>%
  rename(date = time, year = mmwr_year, week = mmwr_week) %>%
  select(geography, geography_fips, date, year, week,
         stss_cases_weekly, stss_cases_cumulative) %>%
  pivot_longer(c(stss_cases_weekly, stss_cases_cumulative),
               names_to = "measure", values_to = "value") %>%
  filter(!is.na(value)) %>%
  # NNDSS publishes no age breakdown, so "Total" per the aggregate convention;
  # `suppressed` is an Epic mechanism and does not apply
  mutate(source = "NNDSS", age = "Total", suppressed = NA_real_) %>%
  select(geography, geography_fips, date, year, week, age,
         source, measure, value, suppressed)

# -----------------------------------------------------------------------------
# 4. State-level Group A surveillance: Epic and NNDSS in one file
#    Both are state + national Group A series keyed on geography, date and a
#    single measure, so they share a schema. `source` separates them, `week`
#    is empty for the quarterly Epic rows and `age` is "Total" for NNDSS.
#    ABCs stays separate: it is national-only and annual, and carries fourteen
#    dimension and companion columns neither of these has.
# -----------------------------------------------------------------------------
gas_state <- bind_rows(epic_gas, nnds_stss) %>%
  arrange(geography, date, source, age, measure)

if (anyNA(gas_state[c("geography", "geography_fips", "date", "year",
                      "age", "source", "measure")])) {
  stop("bundle_strep: NA in an index column of gas_state.")
}

write_dist(gas_state, "gas_state.parquet")
