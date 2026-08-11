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
#
# Three long parquets. All eight ABCs topics are stacked into one file with a
# named column per stratification and "Total" wherever a row is not stratified
# on that dimension. Epic and NNDSS stay separate because they differ in
# geography grain (state vs national), time resolution (quarterly and weekly vs
# annual), and measure.
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

read_standard <- function(path) {
  vroom::vroom(
    file.path("..", path),
    show_col_types = FALSE,
    # Explicit id types; several of these files have sparse numeric columns
    # where vroom's guessing can otherwise infer logical and blank out values
    col_types = vroom::cols(geography = "c", time = "D", .default = "?")
  ) %>%
    add_geography_names()
}

# The standard files carry no NAs: every measure is zero-filled and paired with
# its own `abcs_not_reported_flag_<measure>` saying whether that zero is a
# measured value or a gap CDC never published. So `value_not_reported` here is
# read straight off the source flag rather than inferred from a missing value -
# and a flagged 0 must not be read as a measured zero.
flag_of <- function(m) sub("^abcs_", "abcs_not_reported_flag_", m)

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

# Melt one standard file into the shared long schema.
#   spec        regex -> measure name, and which entity column the matched
#               suffix belongs in
#   companions  output name -> source column, for columns carried alongside
#               `value` instead of melted into their own rows. The two source
#               files name the same isolate count differently
#               (`abcs_n_isolates`, `abcs_gbs_n_isolates`), so normalising here
#               keeps one `n_isolates` column across the stacks.
stack_abcs <- function(path, spec, companions = character()) {
  d <- read_standard(path)
  flag_cols <- grep("^abcs_not_reported_flag_", names(d), value = TRUE)
  companions <- companions[companions %in% names(d)]
  base_ids <- c("geography", "geography_fips", "time",
                intersect(DIMS, names(d)))
  meas_cols <- setdiff(names(d), c(base_ids, flag_cols, unname(companions)))

  # Blank out the zero the source writes for a companion CDC never published,
  # and record which of the two it was in the status column.
  comp_cols <- character()
  for (nm in names(companions)) {
    src <- companions[[nm]]
    fl <- flag_of(src)
    absent <- if (fl %in% names(d)) d[[fl]] == 1L else rep(FALSE, nrow(d))
    d[[nm]] <- replace(as.numeric(d[[src]]), absent, NA_real_)
    d[[status_of(nm)]] <- if_else(absent, NOT_REPORTED, REPORTED)
    comp_cols <- c(comp_cols, nm, status_of(nm))
  }

  out <- list()
  for (m in meas_cols) {
    hit <- NULL
    for (s in spec) {
      if (grepl(s$pattern, m)) { hit <- s; break }
    }
    if (is.null(hit)) stop("bundle_strep: no measure spec matches ", m, " in ", path)
    if (!(flag_of(m) %in% names(d))) {
      stop("bundle_strep: ", m, " in ", path, " has no matching not-reported flag.")
    }

    piece <- d %>%
      select(all_of(c(base_ids, comp_cols)), value = all_of(m),
             value_not_reported = all_of(flag_of(m))) %>%
      mutate(measure = hit$measure)
    if (!is.na(hit$entity)) piece[[hit$entity]] <- sub(hit$pattern, "", m)
    out[[length(out) + 1]] <- piece
  }

  bind_rows(out)
}

# emm needs the per-type isolate count paired with the per-type percent on the
# same row, which a plain melt cannot do (the count lives in a sibling column).
stack_emm <- function(path) {
  d <- read_standard(path)
  base_ids <- c("geography", "geography_fips", "time", intersect(DIMS, names(d)))
  types <- sub("^abcs_gas_emm_pct_", "",
               grep("^abcs_gas_emm_pct_", names(d), value = TRUE))

  # A per-type count or typed total CDC never published is zero in the source;
  # blank those back out, as with the other denominators
  bind_rows(lapply(types, function(t) {
    pct <- paste0("abcs_gas_emm_pct_", t)
    n   <- paste0("abcs_gas_emm_n_", t)
    tot <- "abcs_gas_emm_n_isolates_total"
    piece <- d %>%
      select(all_of(base_ids),
             value = all_of(pct),
             value_not_reported = all_of(flag_of(pct)),
             n_type = all_of(n),
             n_isolates = all_of(tot),
             .n_type_flag = all_of(flag_of(n)),
             .n_isolates_flag = all_of(flag_of(tot))) %>%
      mutate(measure = "pct_emm_type", emm_type = t)
    piece$n_type[piece$.n_type_flag == 1L] <- NA_real_
    piece$n_isolates[piece$.n_isolates_flag == 1L] <- NA_real_
    piece %>%
      mutate(
        n_type_status     = if_else(.n_type_flag == 1L, NOT_REPORTED, REPORTED),
        n_isolates_status = if_else(.n_isolates_flag == 1L, NOT_REPORTED, REPORTED)
      ) %>%
      select(-c(.n_type_flag, .n_isolates_flag))
  }))
}

abcs_strep <- bind_rows(
  stack_abcs(
    "abcs/standard/strep_rates.csv.gz",
    list(
      list(pattern = "^abcs_rate_cases$",  measure = "rate_cases",  entity = NA),
      list(pattern = "^abcs_rate_deaths$", measure = "rate_deaths", entity = NA)
    )
  ),
  stack_abcs(
    "abcs/standard/strep_counts.csv.gz",
    list(
      list(pattern = "^abcs_N_cases$",      measure = "n_cases",      entity = NA),
      list(pattern = "^abcs_N_deaths$",     measure = "n_deaths",     entity = NA),
      list(pattern = "^abcs_N_survivals$",  measure = "n_survivals",  entity = NA)
    )
  ),
  stack_abcs(
    "abcs/standard/strep_resistance.csv.gz",
    list(list(pattern = "^abcs_pct_resistant_", measure = "pct_resistant",
              entity = "antibiotic")),
    companions = c(n_isolates = "abcs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gas_syndromes.csv.gz",
    list(list(pattern = "^abcs_gas_rate_syndrome_", measure = "rate_syndrome",
              entity = "syndrome"))
  ),
  stack_abcs(
    "abcs/standard/gbs_syndromes.csv.gz",
    list(list(pattern = "^abcs_gbs_pct_syndrome_", measure = "pct_syndrome",
              entity = "syndrome"))
  ),
  stack_abcs(
    "abcs/standard/gbs_serotypes.csv.gz",
    list(list(pattern = "^abcs_gbs_pct_serotype_", measure = "pct_serotype",
              entity = "serotype")),
    companions = c(n_isolates = "abcs_gbs_n_isolates")
  ),
  stack_abcs(
    "abcs/standard/gbs_alph.csv.gz",
    list(list(pattern = "^abcs_gbs_pct_alph_", measure = "pct_alph_type",
              entity = "alph_type")),
    companions = c(n_isolates = "abcs_gbs_n_isolates")
  ),
  stack_emm("abcs/standard/gas_emm.csv.gz")
) %>%
  rename(date = time) %>%
  mutate(
    year = as.integer(format(date, "%Y")),
    value_not_reported = as.integer(value_not_reported),
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
                  "measure", "value", "value_not_reported",
                  "n_type", "n_type_status",
                  "n_isolates", "n_isolates_status"))) %>%
  arrange(across(all_of(c("geography", "date", DIMS, ENTITIES, "measure"))))

if (anyDuplicated(abcs_strep[c("geography", "date", DIMS, ENTITIES, "measure")])) {
  stop("bundle_strep: duplicate index rows in abcs_strep.")
}

# `value` is always populated (the source zero-fills and flags), and each
# companion is populated exactly when its status says "reported". Assert both,
# so a future change cannot reintroduce an unexplained blank.
if (anyNA(abcs_strep$value) || anyNA(abcs_strep$value_not_reported)) {
  stop("bundle_strep: NA in value or value_not_reported.")
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
  select(geography, geography_fips, date, year, age, measure, value, suppressed) %>%
  arrange(geography, date, age, measure)

write_dist(epic_gas, "epic_gas.parquet")

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
  arrange(geography, date, measure)

write_dist(nnds_stss, "nnds_stss.parquet")
