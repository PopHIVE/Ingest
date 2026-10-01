# =============================================================================
# NIS-Flu / NIS-FRVM: Weekly Influenza and RSV Vaccination Coverage and Intent
# Sources:
#   judz-8etw  NIS-Flu, children 6 months-17 years (flu), 2019-20 to present
#   sw5n-wg2p  NIS-FRVM (formerly NIS-ACM), adults 18+ (flu), 2021-22 to present
#   qeq7-f3ir  NIS-FRVM, adults 75+ and 50-74 with high-risk conditions (RSV)
#
# Weekly cumulative coverage (percent vaccinated this season; RSV = ever
# vaccinated) plus intent to vaccinate, from CDC's random-digit-dial cellular
# telephone surveys. `population` is the group each survey covers (children
# 6 months-17 years, adults 18+, adults 75+, adults 50-74 at high risk).
#
# Outputs
#   data.csv.gz              national + state, by vaccine and population
#   data_age.csv.gz          national, by age group within the population
#   data_demographics.csv.gz national, by other demographic group
#   data_region.csv.gz       HHS regions (non-FIPS)
#   data_substate.csv.gz     sub-state and local areas (non-FIPS)
#
# Season-specific notes:
#   - Seasons before 2023-24 only carry the "Up-to-date" indicator (coverage);
#     the 4-level coverage/intent breakdown starts in 2023-24.
#   - 2025-26 adult data collection ended 2026-02-21 (earlier than prior seasons).
#   - RSV (qeq7-f3ir) is a single-season dataset; CDC has issued a new dataset
#     ID for each RSV season, so check for a new ID each fall.
# =============================================================================

library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

DATASETS <- c(judz = "judz-8etw", sw5n = "sw5n-wg2p", qeq7 = "qeq7-f3ir")
new_state <- lapply(DATASETS, function(id) dcf::dcf_download_cdc(id, "raw", process$raw_state[[id]]))
names(new_state) <- DATASETS

read_raw <- function(id, needed) {
  d <- vroom::vroom(
    sprintf("raw/%s.csv.xz", id), delim = ",",
    col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
  )
  absent <- setdiff(needed, names(d))
  if (length(absent) > 0) stop(id, " columns not found: ", paste(absent, collapse = ", "))
  d
}
# Dates arrive as "YYYY-MM-DD hh:mm:ss" or "MM/DD/YYYY hh:mm:ss AM"
parse_cdc_date <- function(x) {
  x <- substr(x, 1, 10)
  out <- as.Date(x, format = "%Y-%m-%d")
  i <- is.na(out)
  out[i] <- as.Date(x[i], format = "%m/%d/%Y")
  if (any(is.na(out) & !is.na(x))) stop("unparsed dates: ", paste(head(x[is.na(out)]), collapse = ", "))
  out
}

# CDC publishes each RSV season as a new dataset ID. From November to March the
# newest RSV date should fall in the current season; if it does not, the ID
# list above needs the new season's dataset. message() rather than warning()
# so the text lands in the dcf process log.
check_rsv_current <- function(dates, ids, today = Sys.Date()) {
  m <- as.integer(format(today, "%m")); y <- as.integer(format(today, "%Y"))
  season_start <- as.Date(sprintf("%d-10-01", if (m >= 7) y else y - 1))
  latest <- suppressWarnings(max(dates, na.rm = TRUE))
  if ((m >= 11 || m <= 3) && is.finite(latest) && latest < season_start) {
    message("Warning: latest RSV data is ", latest, "; CDC has probably published ",
            "the current season under a new dataset ID (current: ",
            paste(ids, collapse = ", "), ")")
  }
}

if (!identical(process$raw_state, new_state)) {

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------
  # Saturday on or after the date (a few end-of-season rows fall mid-week)
  to_saturday <- function(d) d + (6 - as.integer(format(d, "%u"))) %% 7
  # "2024-2025" / "2024-25" -> "2024-25"
  norm_season <- function(x) sub("^(\\d{4})-(\\d{2})?(\\d{2})$", "\\1-\\3", x)
  season_from_date <- function(d) {
    y <- as.integer(format(d, "%Y")); m <- as.integer(format(d, "%m"))
    y1 <- ifelse(m >= 7, y, y - 1)
    sprintf("%d-%02d", y1, (y1 + 1) %% 100)
  }
  slug <- function(x) gsub("^_|_$", "", gsub("[^a-z0-9]+", "_", tolower(x)))
  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }

  # Territories have no name in all_fips.csv.gz
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  geo_lookup <- bind_rows(
    all_fips %>% filter(nchar(geography) == 2, !is.na(geography_name)) %>% select(geography, geography_name),
    tibble(
      geography = c("00", "72", "66", "78"),
      geography_name = c("United States", "Puerto Rico", "Guam", "U.S. Virgin Islands")
    )
  ) %>% distinct(geography_name, .keep_all = TRUE)
  state_fips_of <- function(name) geo_lookup$geography[match(name, geo_lookup$geography_name)]

  # ---------------------------------------------------------------------------
  # Read and harmonize the three datasets
  # ---------------------------------------------------------------------------
  judz <- read_raw("judz-8etw", c(
    "Geographic Level", "Geographic Name", "Demographic_Level", "Demographic Name",
    "Indicator_label", "Indicator_category_label", "Week_ending", "Estimate",
    "CI_Half_width_95pct", "Unweighted Sample Size", "suppression_flag", "influenza_season"
  )) %>%
    transmute(
      vaccine = "flu", population = "6 months-17 years",
      geo_level = `Geographic Level`, geo_name = `Geographic Name`,
      demo_level = Demographic_Level, demo_name = `Demographic Name`,
      indicator = Indicator_label, category = Indicator_category_label,
      week_ending = Week_ending, estimate = Estimate, ci_half = CI_Half_width_95pct,
      n_unweighted = `Unweighted Sample Size`, suppression_flag,
      season = norm_season(influenza_season)
    )

  sw5n <- read_raw("sw5n-wg2p", c(
    "Vaccine", "Geographic Level", "Geographic Name", "Demographic Level", "Demographic Name",
    "indicator_label", "indicator_category_label", "Week_ending", "Estimates",
    "CI_Half_width_95pct", "Unweighted Sample Size", "Influenza_Season", "suppression_flag"
  )) %>%
    filter(is.na(Vaccine) | Vaccine == "FLU") %>%
    transmute(
      vaccine = "flu", population = "18+",
      geo_level = `Geographic Level`, geo_name = `Geographic Name`,
      demo_level = `Demographic Level`, demo_name = `Demographic Name`,
      indicator = indicator_label, category = indicator_category_label,
      week_ending = Week_ending, estimate = Estimates, ci_half = CI_Half_width_95pct,
      n_unweighted = `Unweighted Sample Size`, suppression_flag,
      season = norm_season(Influenza_Season)
    )

  qeq7 <- read_raw("qeq7-f3ir", c(
    "Vaccine", "Age Group", "Geography_level", "Geography_name", "Demographic Level",
    "Demographic Name", "Indicator_label", "Indicator_category_label", "Week_ending",
    "Estimate", "CI_Half_width_95pct", "Unweighted Sample Size", "Suppresion_flag"
  )) %>%
    filter(Vaccine == "RSV") %>%
    transmute(
      vaccine = "rsv", population = sub(" years", "", `Age Group`),
      geo_level = Geography_level, geo_name = Geography_name,
      demo_level = `Demographic Level`, demo_name = `Demographic Name`,
      indicator = Indicator_label, category = Indicator_category_label,
      week_ending = Week_ending, estimate = Estimate, ci_half = CI_Half_width_95pct,
      n_unweighted = `Unweighted Sample Size`, suppression_flag = Suppresion_flag,
      season = NA_character_
    )

  # The RSV file reports estimates as proportions (0-1) while the flu files
  # report percents; the CI half-widths are percents in all three.
  qeq7 <- qeq7 %>% mutate(estimate = as.character(suppressWarnings(as.numeric(estimate)) * 100))
  if (any(suppressWarnings(as.numeric(qeq7$estimate)) > 150, na.rm = TRUE)) {
    stop("qeq7-f3ir estimates no longer look like proportions; remove the x100")
  }

  base <- bind_rows(judz, sw5n, qeq7) %>%
    mutate(
      time = to_saturday(parse_cdc_date(week_ending)),
      season = coalesce(season, season_from_date(time)),
      # Early seasons have a blank indicator label on the coverage ("Yes") rows
      indicator = coalesce(indicator, if_else(category == "Yes", "Up-to-date", NA_character_)),
      measure = case_when(
        indicator == "Up-to-date" & category == "Yes"                         ~ "coverage",
        category %in% c("Received a vaccination", "Vaccinated")               ~ "coverage_4level",
        category == "Definitely will get a vaccine"                            ~ "intent_definitely",
        category %in% c("Probably will get a vaccine",
                        "Probably will get a vaccine or are unsure")           ~ "intent_probably",
        category == "Definitely or probably will not get a vaccine"            ~ "intent_no"
      ),
      estimate = suppressWarnings(as.numeric(estimate)),
      ci_half = suppressWarnings(as.numeric(ci_half)),
      n_unweighted = suppressWarnings(as.numeric(n_unweighted)),
      suppressed_flag = as.integer(coalesce(suppression_flag == "1", FALSE) | is.na(estimate)),
      # The "Overall" rows carry the population label as the demographic name
      demo_name = if_else(demo_level == "Overall", "Overall", demo_name)
    )

  unknown <- base %>% filter(is.na(measure)) %>% distinct(indicator, category)
  if (nrow(unknown) > 0) {
    stop("unrecognised indicator/category: ", paste(unknown$indicator, unknown$category, sep = " / ", collapse = "; "))
  }

  # "Received a vaccination" duplicates the "Up-to-date" coverage row, which
  # exists in every season; keep the latter only.
  KEYS <- c("vaccine", "population", "geo_level", "geo_name", "demo_level", "demo_name", "season", "time")
  long <- base %>%
    filter(measure != "coverage_4level") %>%
    # The adult file repeats a few national 50-64 rows with different rounding
    arrange(across(all_of(KEYS)), measure, desc(nchar(as.character(estimate)))) %>%
    distinct(across(all_of(c(KEYS, "measure"))), .keep_all = TRUE)

  coverage <- long %>%
    filter(measure == "coverage") %>%
    transmute(
      across(all_of(KEYS)),
      nis_coverage = estimate,
      nis_coverage_lcl = pmax(estimate - ci_half, 0),
      nis_coverage_ucl = pmin(estimate + ci_half, 100),
      nis_sample_size = n_unweighted,
      suppressed_flag
    )
  intent <- long %>%
    filter(measure != "coverage") %>%
    select(all_of(KEYS), measure, estimate) %>%
    pivot_wider(names_from = measure, values_from = estimate, names_prefix = "nis_")

  wide <- coverage %>% left_join(intent, by = KEYS)

  VALUE_COLS <- c("nis_coverage", "nis_coverage_lcl", "nis_coverage_ucl",
                  "nis_intent_definitely", "nis_intent_probably", "nis_intent_no",
                  "nis_sample_size", "suppressed_flag")

  # ---------------------------------------------------------------------------
  # standard/data.csv.gz: national + state, overall population
  # ---------------------------------------------------------------------------
  data <- wide %>%
    filter(geo_level %in% c("National", "State"), demo_level == "Overall") %>%
    mutate(geography = if_else(geo_level == "National", "00", state_fips_of(geo_name)))
  unmapped <- unique(data$geo_name[is.na(data$geography)])
  if (length(unmapped) > 0) stop("State names not mapped to FIPS: ", paste(unmapped, collapse = ", "))
  data <- data %>%
    mutate(time = format(time, "%Y-%m-%d")) %>%
    select(geography, time, season, vaccine, population, all_of(VALUE_COLS)) %>%
    arrange(vaccine, population, geography, time) %>%
    check_unique(c("geography", "time", "vaccine", "population"), "data")
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # standard/data_age.csv.gz: national, by age group within the population
  # ---------------------------------------------------------------------------
  data_age <- wide %>%
    filter(geo_level == "National", demo_level == "Age") %>%
    mutate(geography = "00", time = format(time, "%Y-%m-%d")) %>%
    select(geography, time, season, vaccine, population, age = demo_name, all_of(VALUE_COLS)) %>%
    arrange(vaccine, population, age, time) %>%
    check_unique(c("geography", "time", "vaccine", "population", "age"), "data_age")
  vroom::vroom_write(data_age, "standard/data_age.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # standard/data_demographics.csv.gz: national, by other demographic group
  # ---------------------------------------------------------------------------
  data_demographics <- wide %>%
    filter(geo_level == "National", !demo_level %in% c("Overall", "Age")) %>%
    mutate(geography = "00", time = format(time, "%Y-%m-%d")) %>%
    select(geography, time, season, vaccine, population,
           stratum_type = demo_level, stratum = demo_name, all_of(VALUE_COLS)) %>%
    arrange(vaccine, population, stratum_type, stratum, time) %>%
    check_unique(c("geography", "time", "vaccine", "population", "stratum_type", "stratum"), "data_demographics")
  vroom::vroom_write(data_demographics, "standard/data_demographics.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # standard/data_region.csv.gz: HHS regions (geography = hhs_<n>, not FIPS)
  # ---------------------------------------------------------------------------
  data_region <- wide %>%
    filter(geo_level == "Region", demo_level == "Overall") %>%
    mutate(geography = sub("^Region ", "hhs_", geo_name), time = format(time, "%Y-%m-%d")) %>%
    select(geography, geography_name = geo_name, time, season, vaccine, population, all_of(VALUE_COLS)) %>%
    arrange(vaccine, population, geography, time) %>%
    check_unique(c("geography", "time", "vaccine", "population"), "data_region")
  vroom::vroom_write(data_region, "standard/data_region.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # standard/data_substate.csv.gz: sub-state and local areas. geography is a
  # slug of the CDC area name, not a FIPS code; state_fips is the parent state.
  # ---------------------------------------------------------------------------
  LOCAL_STATE <- c(
    "Bexar County" = "48", "City of Chicago" = "17", "City of Houston" = "48",
    "New York City" = "36", "Philadelphia County" = "42"
  )
  data_substate <- wide %>%
    filter(geo_level %in% c("Substate", "Local"), demo_level == "Overall") %>%
    mutate(
      geography = slug(geo_name),
      state_fips = if_else(geo_level == "Substate",
                           state_fips_of(sub("-.*$", "", geo_name)),
                           unname(LOCAL_STATE[geo_name])),
      time = format(time, "%Y-%m-%d")
    ) %>%
    select(geography, geography_name = geo_name, geography_level = geo_level, state_fips,
           time, season, vaccine, population, all_of(VALUE_COLS)) %>%
    arrange(vaccine, population, geography, time) %>%
    check_unique(c("geography", "time", "vaccine", "population"), "data_substate")
  vroom::vroom_write(data_substate, "standard/data_substate.csv.gz", ",")

  process$raw_state <- new_state
  dcf::dcf_process_record(updated = process)
}

rsv_raw <- read_raw(DATASETS[["qeq7"]], "Week_ending")
check_rsv_current(parse_cdc_date(rsv_raw$Week_ending), DATASETS[["qeq7"]])
