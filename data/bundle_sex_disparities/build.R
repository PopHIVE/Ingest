# =============================================================================
# Bundle: bundle_sex_disparities
# Combines: abcs, cms_mmd, epic_concussions, nccr, neiss, nhtsa_crash,
#           nis_teen, wisqars, yrbss
#
# For every sex-stratified measure writes one tall file per source,
# <out>_by_sex.parquet: geography, time, strata, sex, measure, value, source.
# `sex` is Female, Male or Overall (the source's row for both sexes combined).
# Ratios and differences are left to the consumer. Reads only standard/ files;
# the cdc_wonder_natality source is deliberately excluded.
# =============================================================================

library(dplyr)
library(arrow)

# -----------------------------------------------------------------------------
# 1. Inputs
# -----------------------------------------------------------------------------
# `out` is the output file stem. `source` is the label written to the source
# column. `values` selects the value columns of the file by name. `flag_alias`
# maps a value column onto the column whose suppression flag it shares.
# `known_geography` drops rows whose geography is not in
# resources/all_fips.csv.gz. `drop_constant` removes strata columns that hold a
# single value in the whole output (checked). `drop_cols` removes columns
# outright. `keep_aggregate` is a function of the source data frame that marks
# rows whose Overall-sex value is kept even without a Female and a Male row.
# `no_aggregate` declares that the file has no both-sexes row at all.
specs <- list(
  list(file = "abcs/standard/strep_rates.csv.gz", out = "abcs_strep",
       source = "CDC ABCs", values = "^abcs_rate_",
       drop_constant = c("age", "race_ethnicity", "onset")),
  list(file = "cms_mmd/standard/data_state_county_age_by_sex.csv.gz",
       out = "cms_prevalence", source = "Medicare FFS", values = "^cms_",
       drop_cols = "geography_level"),
  list(file = "epic_concussions/standard/data.csv.gz",
       out = "epic_concussion_rate", source = "Epic Cosmos",
       values = "^epic_pct_concussion$",
       flag_alias = c(epic_pct_concussion = "epic_n_concussion")),
  list(file = "epic_concussions/standard/data.csv.gz",
       out = "epic_concussion_count", source = "Epic Cosmos",
       values = "^epic_n_(concussion|ed_encounters)$"),
  list(file = "nccr/standard/data.csv.gz", out = "nccr_incidence",
       source = "NCI NCCR Explorer", values = "^nccr_"),
  list(file = "neiss/standard/data_agegroup_diagnosis_rate.csv.gz",
       out = "neiss_rate", source = "CPSC NEISS", values = "^neiss_rate_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_agegroup_product_rate.csv.gz",
       out = "neiss_rate", source = "CPSC NEISS", values = "^neiss_rate_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_infant_diagnosis_rate.csv.gz",
       out = "neiss_rate", source = "CPSC NEISS", values = "^neiss_rate_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_infant_product_rate.csv.gz",
       out = "neiss_rate", source = "CPSC NEISS", values = "^neiss_rate_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_agegroup_diagnosis.csv.gz",
       out = "neiss_count", source = "CPSC NEISS", values = "^neiss_n_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_agegroup_product.csv.gz",
       out = "neiss_count", source = "CPSC NEISS", values = "^neiss_n_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_infant_diagnosis.csv.gz",
       out = "neiss_count", source = "CPSC NEISS", values = "^neiss_n_",
       no_aggregate = TRUE),
  list(file = "neiss/standard/data_infant_product.csv.gz",
       out = "neiss_count", source = "CPSC NEISS", values = "^neiss_n_",
       no_aggregate = TRUE),
  list(file = "nhtsa_crash/standard/data_age_sex.csv.gz",
       out = "nhtsa_fatalities", source = "NHTSA FARS", values = "^nhtsa_",
       no_aggregate = TRUE),
  # data_crash_type keeps FARS placeholder county codes (997/998, not counties)
  list(file = "nhtsa_crash/standard/data_crash_type.csv.gz",
       out = "nhtsa_fatalities", source = "NHTSA FARS", values = "^nhtsa_",
       known_geography = TRUE),
  list(file = "nis_teen/standard/data.csv.gz", out = "nis_teen_coverage",
       source = "CDC NIS-Teen", values = "^nis_teen_coverage$"),
  # WISQARS reports age Total for all sexes only; keep it so the files have an
  # all-ages row
  list(file = "wisqars/standard/data.csv.gz", out = "wisqars_death_rate",
       source = "CDC/WISQARS", values = "^wisqars_rate_",
       keep_aggregate = function(d) d$age == "Total"),
  list(file = "wisqars/standard/data.csv.gz", out = "wisqars_death_count",
       source = "CDC/WISQARS", values = "^wisqars_deaths_",
       keep_aggregate = function(d) d$age == "Total"),
  list(file = "yrbss/standard/data_age_sex.csv.gz", out = "yrbss_behavior",
       source = "CDC/YRBSS", drop_constant = "age",
       values = "^pct_(?!no_pe_classes$|no_condom_last_sex$|no_birth_control_pills$|never_tested_hiv$|not_tested_std$)")
)

all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

ci_or_flag <- "_(lcl|ucl|suppressed|suppressed_flag|not_asked|not_collected)$"
sex_levels <- c(female = "Female", male = "Male")
agg_label <- "Overall"                    # bundle label for the both-sexes row
source_agg <- c("Total", "Overall", "All") # what the sources call it

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
  if (is.null(spec$source)) stop(spec$file, ": no source label")
  df$sex[df$sex %in% source_agg] <- agg_label
  df <- df[df$sex %in% c(sex_levels, agg_label), ]
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

  keep <- if (is.null(spec$keep_aggregate)) FALSE else spec$keep_aggregate(df) %in% TRUE
  tall <- bind_rows(lapply(vals, function(m) {
    d <- df[c(id_cols, "sex")]
    d$measure <- m
    d$value <- as.numeric(df[[m]])
    d$suppressed_flag <- flagged(m)
    d$dataset <- dataset
    d$source <- spec$source
    d$keep <- keep
    d
  })) %>%
    filter(!is.na(value))

  # An Overall row stays only where the same stratum and measure also have a
  # Female and a Male value (YRBSS ages 14-17 and ABCs Group B and deaths are
  # reported for both sexes only, so they stay out), or where `keep_aggregate`
  # says so.
  key <- do.call(paste, c(tall[c(id_cols, "measure")], sep = "\r"))
  paired <- intersect(key[tall$sex == sex_levels[["female"]]],
                      key[tall$sex == sex_levels[["male"]]])
  tall <- tall[tall$sex != agg_label | key %in% paired | tall$keep, ]
  tall$keep <- NULL
  if (!any(tall$sex == agg_label) && !isTRUE(spec$no_aggregate)) {
    stop(spec$file, ": no both-sexes rows")
  }
  tall
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

  strata <- setdiff(names(tall),
                    c("sex", "measure", "value", "suppressed_flag", "source"))
  keys <- c(strata, "measure")
  if (anyDuplicated(tall[c(keys, "sex")])) {
    stop(out, ": more than one row per stratum x measure x sex")
  }
  strata_first <- c(intersect(c("dataset", "geography", "time"), strata),
                    setdiff(strata, c("dataset", "geography", "time")))

  tall <- tall %>%
    select(all_of(strata_first), sex, measure, value, suppressed_flag, source)

  arrow::write_parquet(tall, file.path("dist", paste0(out, "_by_sex.parquet")),
                       compression = "snappy")
  log_lines <- c(log_lines, sprintf(
    "  %-24s: %d rows (%d Overall), %d measures, %d geographies",
    out, nrow(tall), sum(tall$sex == agg_label), n_distinct(tall$measure),
    n_distinct(tall$geography)
  ))
}
cat(log_lines, sep = "\n")
