# =============================================================================
# Bundle: bundle_sex_disparities
# Combines: abcs, cms_mmd, epic_concussions, nccr, neiss, nhtsa_crash,
#           nis_teen, wisqars, yrbss
#
# For every sex-stratified measure writes one tall file per source,
# <out>_by_sex.parquet: geography, time, strata, sex, measure, value. Ratios and
# differences are left to the consumer. Reads only standard/ files; the
# cdc_wonder_natality source is deliberately excluded.
# =============================================================================

library(dplyr)
library(arrow)

# -----------------------------------------------------------------------------
# 1. Inputs
# -----------------------------------------------------------------------------
# `out` is the output file stem. `values` selects the value columns of the file
# by name. `flag_alias` maps a value column onto the column whose suppression
# flag it shares. `known_geography` drops rows whose geography is not in
# resources/all_fips.csv.gz. `drop_constant` removes strata columns that hold a
# single value in the whole output (checked). `drop_cols` removes columns outright.
specs <- list(
  list(file = "abcs/standard/strep_rates.csv.gz", out = "abcs_strep",
       values = "^abcs_rate_", drop_constant = c("age", "race_ethnicity", "onset")),
  list(file = "cms_mmd/standard/data_state_county_age_by_sex.csv.gz",
       out = "cms_prevalence", values = "^cms_", drop_cols = "geography_level"),
  list(file = "epic_concussions/standard/data.csv.gz",
       out = "epic_concussion_rate", values = "^epic_pct_concussion$",
       flag_alias = c(epic_pct_concussion = "epic_n_concussion")),
  list(file = "epic_concussions/standard/data.csv.gz",
       out = "epic_concussion_count",
       values = "^epic_n_(concussion|ed_encounters)$"),
  list(file = "nccr/standard/data.csv.gz", out = "nccr_incidence",
       values = "^nccr_"),
  list(file = "neiss/standard/data_agegroup_diagnosis_rate.csv.gz",
       out = "neiss_rate", values = "^neiss_rate_"),
  list(file = "neiss/standard/data_agegroup_product_rate.csv.gz",
       out = "neiss_rate", values = "^neiss_rate_"),
  list(file = "neiss/standard/data_infant_diagnosis_rate.csv.gz",
       out = "neiss_rate", values = "^neiss_rate_"),
  list(file = "neiss/standard/data_infant_product_rate.csv.gz",
       out = "neiss_rate", values = "^neiss_rate_"),
  list(file = "neiss/standard/data_agegroup_diagnosis.csv.gz",
       out = "neiss_count", values = "^neiss_n_"),
  list(file = "neiss/standard/data_agegroup_product.csv.gz",
       out = "neiss_count", values = "^neiss_n_"),
  list(file = "neiss/standard/data_infant_diagnosis.csv.gz",
       out = "neiss_count", values = "^neiss_n_"),
  list(file = "neiss/standard/data_infant_product.csv.gz",
       out = "neiss_count", values = "^neiss_n_"),
  list(file = "nhtsa_crash/standard/data_age_sex.csv.gz",
       out = "nhtsa_fatalities", values = "^nhtsa_"),
  # data_crash_type keeps FARS placeholder county codes (997/998, not counties)
  list(file = "nhtsa_crash/standard/data_crash_type.csv.gz",
       out = "nhtsa_fatalities", values = "^nhtsa_", known_geography = TRUE),
  list(file = "nis_teen/standard/data.csv.gz", out = "nis_teen_coverage",
       values = "^nis_teen_coverage$"),
  list(file = "wisqars/standard/data.csv.gz", out = "wisqars_death_rate",
       values = "^wisqars_rate_"),
  list(file = "wisqars/standard/data.csv.gz", out = "wisqars_death_count",
       values = "^wisqars_deaths_"),
  list(file = "yrbss/standard/data_age_sex.csv.gz", out = "yrbss_behavior",
       drop_constant = "age",
       values = "^pct_(?!no_pe_classes$|no_condom_last_sex$|no_birth_control_pills$|never_tested_hiv$|not_tested_std$)")
)

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

ci_or_flag <- "_(lcl|ucl|suppressed|suppressed_flag|not_asked|not_collected)$"
sex_levels <- c(female = "Female", male = "Male")

# -----------------------------------------------------------------------------
# 2. Read one file as tall: one row per stratum x measure x sex
# -----------------------------------------------------------------------------
read_tall <- function(spec) {
  df <- vroom::vroom(
    file.path("..", spec$file),
    col_types = vroom::cols(geography = "c", time = "c"),
    show_col_types = FALSE,
    altrep = FALSE,
    guess_max = Inf
  )
  if (nrow(vroom::problems(df))) stop(spec$file, ": parsing problems")
  df <- df[df$sex %in% sex_levels, ]
  df <- df[setdiff(names(df), spec$drop_cols)]
  if (isTRUE(spec$known_geography)) df <- df[df$geography %in% all_fips$geography, ]

  vals <- names(df)[grepl(spec$values, names(df), perl = TRUE) &
                      !grepl(ci_or_flag, names(df))]
  if (!length(vals)) stop(spec$file, ": no value columns matched")
  id_cols <- setdiff(names(df)[vapply(df, is.character, logical(1))], "sex")
  dataset <- sub("\\.csv\\.gz$", "", sub("^data_?", "", basename(spec$file)))
  dataset <- paste(dirname(dirname(spec$file)),
                   if (nzchar(dataset)) dataset else "data", sep = "/")

  flagged <- function(m) {
    base <- if (m %in% names(spec$flag_alias)) spec$flag_alias[[m]] else m
    fl <- intersect(
      paste0(base, c("_suppressed", "_suppressed_flag", "_not_asked", "_not_collected")),
      names(df)
    )
    if (!length(fl)) return(rep(0L, nrow(df)))
    as.integer(rowSums(df[fl] == 1, na.rm = TRUE) > 0)
  }

  bind_rows(lapply(vals, function(m) {
    d <- df[c(id_cols, "sex")]
    d$measure <- m
    d$value <- as.numeric(df[[m]])
    d$suppressed_flag <- flagged(m)
    d$dataset <- dataset
    d
  })) %>%
    filter(!is.na(value))
}

# -----------------------------------------------------------------------------
# 3. Build and write (parquet only)
# -----------------------------------------------------------------------------
outs <- unique(vapply(specs, `[[`, character(1), "out"))
log_lines <- "bundle_sex_disparities:"

for (out in outs) {
  members <- Filter(function(s) s$out == out, specs)
  tall <- bind_rows(lapply(members, read_tall))
  if (length(members) == 1) tall$dataset <- NULL
  for (col in unlist(lapply(members, `[[`, "drop_constant"))) {
    if (n_distinct(tall[[col]]) != 1) stop(out, ": ", col, " is not constant")
    tall[[col]] <- NULL
  }

  strata <- setdiff(names(tall), c("sex", "measure", "value", "suppressed_flag"))
  keys <- c(strata, "measure")
  if (anyDuplicated(tall[c(keys, "sex")])) {
    stop(out, ": more than one row per stratum x measure x sex")
  }
  strata_first <- c(intersect(c("dataset", "geography", "time"), strata),
                    setdiff(strata, c("dataset", "geography", "time")))

  tall <- tall %>%
    select(all_of(strata_first), sex, measure, value, suppressed_flag)

  arrow::write_parquet(tall, file.path("dist", paste0(out, "_by_sex.parquet")),
                       compression = "snappy")
  log_lines <- c(log_lines, sprintf(
    "  %-24s: %d rows, %d measures, %d geographies",
    out, nrow(tall), n_distinct(tall$measure), n_distinct(tall$geography)
  ))
}
cat(log_lines, sep = "\n")
