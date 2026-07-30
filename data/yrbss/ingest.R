# =============================================================================
# CDC YRBSS (Youth Risk Behavior Surveillance System) Data Ingestion
# Source: YRBS Explorer  https://yrbs-explorer.services.cdc.gov/#/
#
# The YRBS Explorer is a single-page app backed by a REST API at
#   https://yrbs-explorer.services.cdc.gov/api
# Relevant endpoints (reverse-engineered from the app bundle):
#   /Years/2                                                 -> survey years
#   /YrbsExplorerLocations                                   -> national/state/local
#   /Questions?SurveyId=2&ListOfYears=..&ListOfLocations=XX  -> question catalog
#   /ChartData?QuestionId={code}&LocationId={loc}&Yr=9999    -> all years, all strata
#
# Scope (per project request):
#   - Geographies : National (00) + 47 states + DC
#   - Years       : 2005 onward
#   - Questions   : Curated subset across topics (see FULL_TOPICS +
#                   SELECT_CODES below): all of Physical Activity (C06) plus
#                   selected injury/violence, mental-health, tobacco,
#                   substance-use, diet, and other-health-topic items.
#   - Strata      : Total, Sex, Race, Grade. Sex->sex, Race->race_ethnicity,
#                   Grade->age (approximate modal age), Total->Overall.
#                   YRBSS provides MARGINAL strata only (each estimate is broken
#                   out by exactly one of these), so there is no age x sex cross.
#
# Output: three wide standard files, one per stratification dimension, each
#   with one column per measure (value / _lcl / _ucl), BRFSS-style:
#     standard/data_age.csv.gz        keys: geography, time, age
#     standard/data_age_sex.csv.gz    keys: geography, time, age, sex
#     standard/data_age_ethnicity.csv.gz  keys: geography, time, age, race_ethnicity
#   Suppressed values, topic, and question_code are dropped from the data files
#   and documented in measure_info.json instead.
# =============================================================================

library(tidyverse)
library(jsonlite)
library(parallel)
library(dcf)

BASE        <- "https://yrbs-explorer.services.cdc.gov/api"
MIN_YEAR    <- 2005
# Topic codes to include in full (every question in the topic)
FULL_TOPICS <- c("C06")                                # all Physical Activity
# Individual question codes to include (in addition to FULL_TOPICS)
SELECT_CODES <- c(
  # C01 Injuries & Violence
  "H8", "H9", "H10", "H11", "H12", "H13", "H15", "H24", "H25", "H14",
  "H27", "H28", "H29", "H30",
  # C02 Tobacco
  "H31", "H33", "H35", "H36", "H38",
  # C03 Alcohol & Other Drug Use
  "H42", "H43", "H46", "H47", "H48", "H49", "QNCURRENTOPIOID",
  "H50", "H51", "H52", "H53", "H54", "QNHALLUCDRUG", "H55", "QNILLICT",
  # C05 Dietary
  "QNFR0", "QNVEG0", "H75", "QNBK7DAY",
  # C08 Other Health Topics
  "H84", "H85", "H86", "QNCLOSE2PEOPLE", "H80"
)
RAW_FILE    <- "raw/yrbss_chartdata.csv.gz"

# -----------------------------------------------------------------------------
# Measure dictionary: maps each YRBSS question_code to a short descriptive
# variable slug used as the column name in the standard files. The value column
# is the slug itself; the 95% CI bounds are <slug>_lcl and <slug>_ucl. topic,
# question_code, and the full question text are carried through to
# measure_info.json (and dropped from the data files).
# -----------------------------------------------------------------------------
measure_dict <- tibble::tribble(
  ~question_code,     ~slug,                          ~short_label,                       ~question_text,                                                                                                          ~topic,
  # ---- Unintentional Injuries and Violence (category: injury) ----
  "H8",               "pct_no_seatbelt",              "Did not always wear a seat belt",  "Did not always wear a seat belt",                                                                                       "Unintentional Injuries and Violence",
  "H9",               "pct_rode_drinking_driver",     "Rode with a drinking driver",      "Rode with a driver who had been drinking alcohol",                                                                      "Unintentional Injuries and Violence",
  "H10",              "pct_drove_drinking",           "Drove after drinking alcohol",     "Drove when they had been drinking alcohol",                                                                             "Unintentional Injuries and Violence",
  "H11",              "pct_text_while_driving",       "Texted/e-mailed while driving",    "Texted or e-mailed while driving a car or other vehicle",                                                               "Unintentional Injuries and Violence",
  "H12",              "pct_carried_weapon_school",    "Carried a weapon at school",       "Carried a weapon on school property",                                                                                   "Unintentional Injuries and Violence",
  "H13",              "pct_carried_gun",              "Carried a gun",                    "Carried a gun",                                                                                                         "Unintentional Injuries and Violence",
  "H14",              "pct_unsafe_at_school",         "Did not go to school, unsafe",     "Did not go to school because they felt unsafe at school or on their way to or from school",                              "Unintentional Injuries and Violence",
  "H15",              "pct_threatened_weapon_school", "Threatened with weapon at school", "Were threatened or injured with a weapon on school property",                                                           "Unintentional Injuries and Violence",
  "H24",              "pct_bullied_at_school",        "Bullied on school property",       "Were bullied on school property",                                                                                       "Unintentional Injuries and Violence",
  "H25",              "pct_bullied_electronic",       "Electronically bullied",           "Were electronically bullied",                                                                                           "Unintentional Injuries and Violence",
  "H27",              "pct_considered_suicide",       "Seriously considered suicide",     "Seriously considered attempting suicide",                                                                               "Unintentional Injuries and Violence",
  "H28",              "pct_planned_suicide",          "Made a suicide plan",              "Made a plan about how they would attempt suicide",                                                                      "Unintentional Injuries and Violence",
  "H29",              "pct_attempted_suicide",        "Attempted suicide",                "Actually attempted suicide",                                                                                            "Unintentional Injuries and Violence",
  "H30",              "pct_injurious_suicide_attempt","Injurious suicide attempt",        "Had a suicide attempt that resulted in an injury, poisoning, or overdose that had to be treated by a doctor or nurse",   "Unintentional Injuries and Violence",
  # ---- Tobacco Use (category: chronic) ----
  "H31",              "pct_ever_cigarette",           "Ever smoked a cigarette",          "Ever smoked a cigarette",                                                                                               "Tobacco Use",
  "H33",              "pct_current_cigarette",        "Currently smoked cigarettes",      "Currently smoked cigarettes",                                                                                           "Tobacco Use",
  "H35",              "pct_ever_vape",                "Ever used vapor products",         "Ever used electronic vapor products",                                                                                   "Tobacco Use",
  "H36",              "pct_current_vape",             "Currently used vapor products",    "Currently used electronic vapor products",                                                                              "Tobacco Use",
  "H38",              "pct_current_smokeless_tobacco","Currently used smokeless tobacco", "Currently used smokeless tobacco",                                                                                      "Tobacco Use",
  # ---- Alcohol and Other Drug Use (category: chronic) ----
  "H42",              "pct_current_alcohol",          "Currently drank alcohol",          "Currently drank alcohol",                                                                                               "Alcohol and Other Drug Use",
  "H43",              "pct_binge_drinking",           "Currently binge drinking",         "Currently were binge drinking",                                                                                         "Alcohol and Other Drug Use",
  "H46",              "pct_ever_marijuana",           "Ever used marijuana",              "Ever used marijuana",                                                                                                   "Alcohol and Other Drug Use",
  "H47",              "pct_early_marijuana",          "Tried marijuana before age 13",    "Tried marijuana for the first time before age 13 years",                                                                "Alcohol and Other Drug Use",
  "H48",              "pct_current_marijuana",        "Currently used marijuana",         "Currently used marijuana",                                                                                              "Alcohol and Other Drug Use",
  "H49",              "pct_ever_rx_opioid_misuse",    "Ever misused Rx opioids",          "Ever took prescription pain medicine without a doctor's prescription or differently than how a doctor told them to use it","Alcohol and Other Drug Use",
  "QNCURRENTOPIOID",  "pct_current_rx_opioid_misuse", "Currently misused Rx opioids",     "Currently took prescription pain medicine without a doctor's prescription or differently than how a doctor told them to use it","Alcohol and Other Drug Use",
  "H50",              "pct_ever_cocaine",             "Ever used cocaine",                "Ever used cocaine",                                                                                                     "Alcohol and Other Drug Use",
  "H51",              "pct_ever_inhalants",           "Ever used inhalants",              "Ever used inhalants",                                                                                                   "Alcohol and Other Drug Use",
  "H52",              "pct_ever_heroin",              "Ever used heroin",                 "Ever used heroin",                                                                                                      "Alcohol and Other Drug Use",
  "H53",              "pct_ever_methamphetamines",    "Ever used methamphetamines",       "Ever used methamphetamines",                                                                                            "Alcohol and Other Drug Use",
  "H54",              "pct_ever_ecstasy",             "Ever used ecstasy",                "Ever used ecstasy",                                                                                                     "Alcohol and Other Drug Use",
  "QNHALLUCDRUG",     "pct_ever_hallucinogens",       "Ever used hallucinogens",          "Ever used hallucinogenic drugs",                                                                                        "Alcohol and Other Drug Use",
  "H55",              "pct_ever_inject_drug",         "Ever injected illegal drug",       "Ever injected any illegal drug",                                                                                        "Alcohol and Other Drug Use",
  "QNILLICT",         "pct_ever_illicit_drug",        "Ever used select illicit drugs",   "Ever used select illicit drugs",                                                                                        "Alcohol and Other Drug Use",
  # ---- Dietary Behaviors (category: chronic) ----
  "H75",              "pct_no_breakfast",             "Did not eat breakfast",            "Did not eat breakfast (during the 7 days before the survey)",                                                           "Dietary Behaviors",
  "QNBK7DAY",         "pct_no_breakfast_7days",       "No breakfast on all 7 days",       "Did not eat breakfast on all 7 days (before the survey)",                                                               "Dietary Behaviors",
  "QNFR0",            "pct_no_fruit",                 "Did not eat fruit",                "Did not eat fruit or drink 100% fruit juices (during the 7 days before the survey)",                                    "Dietary Behaviors",
  "QNVEG0",           "pct_no_vegetables",            "Did not eat vegetables",           "Did not eat vegetables (during the 7 days before the survey)",                                                          "Dietary Behaviors",
  # ---- Physical Activity (category: chronic) ----
  "H76",              "pct_inactive_60min_5days",     "Inactive <5 days/wk",              "Were not physically active at least 60 minutes per day on 5 or more days",                                              "Physical Activity",
  "H77",              "pct_no_pe_classes",            "Did not attend PE classes",        "Did not attend physical education (PE) classes on 1 or more days",                                                      "Physical Activity",
  "H78",              "pct_no_sports_team",           "Did not play on a sports team",    "Did not play on at least one sports team",                                                                              "Physical Activity",
  "H79",              "pct_sports_concussion",        "Concussion from sport/activity",   "Had a concussion from playing a sport or being physically active",                                                      "Physical Activity",
  "QNDLYPE",          "pct_no_daily_pe",              "No daily PE",                      "Did not attend physical education (PE) classes on all 5 days",                                                          "Physical Activity",
  "QNMUSCLESTRENGTH", "pct_no_muscle_strengthening",  "No muscle strengthening",          "Did not do exercises to strengthen or tone muscles on three or more days",                                              "Physical Activity",
  "QNPA0DAY",         "pct_inactive_all_days",        "Inactive every day",               "Were not physically active for at least 60 minutes on at least 1 day",                                                  "Physical Activity",
  "QNPA7DAY",         "pct_inactive_60min_7days",     "Inactive <7 days/wk",              "Were not physically active at least 60 minutes per day on all 7 days",                                                  "Physical Activity",
  # ---- Other Health Topics (category: chronic) ----
  "H80",              "pct_social_media_daily",       "Used social media several/day",    "Used social media at least several times a day",                                                                        "Other Health Topics",
  "H84",              "pct_poor_mental_health",       "Poor mental health",               "Reported that their mental health was most of the time or always not good",                                             "Other Health Topics",
  "H85",              "pct_insufficient_sleep",       "Insufficient sleep (<8 hrs)",      "Did not get 8 or more hours of sleep (on an average school night)",                                                     "Other Health Topics",
  "H86",              "pct_unstable_housing",         "Experienced unstable housing",     "Experienced unstable housing",                                                                                          "Other Health Topics",
  "QNCLOSE2PEOPLE",   "pct_not_close_at_school",      "Did not feel close at school",     "Strongly disagreed or disagreed that they feel close to people at their school",                                        "Other Health Topics"
)

# -----------------------------------------------------------------------------
# Initialize process record (process.json is created by dcf::dcf_add_source())
# -----------------------------------------------------------------------------
process <- dcf::dcf_process_record()

# Small wrapper: fetch a URL as parsed JSON, retrying once on failure.
fetch_json <- function(url) {
  for (attempt in 1:2) {
    res <- tryCatch(jsonlite::fromJSON(url, simplifyVector = TRUE),
                    error = function(e) NULL)
    if (!is.null(res)) return(res)
    Sys.sleep(0.5)
  }
  NULL
}

# -----------------------------------------------------------------------------
# 1. Survey years and question catalog
# -----------------------------------------------------------------------------
years_resp <- fetch_json(paste0(BASE, "/Years/2"))
year_vec   <- as.integer(unlist(years_resp$Years))
year_vec   <- sort(year_vec)
list_years <- paste(year_vec, collapse = ",")

cat_raw <- jsonlite::fromJSON(
  paste0(BASE, "/Questions?SurveyId=2&ListOfYears=", list_years,
         "&ListOfLocations=XX"),
  simplifyVector = FALSE
)
topics <- cat_raw[[1]]$Topics

catalog <- purrr::map_dfr(topics, function(tp) {
  purrr::map_dfr(tp$TopicQuestions, function(q) {
    tibble(
      topic_code    = tp$TopicCode,
      topic_text    = tp$TopicText,
      question_code = q$QuestionCode,
      question_text = q$GreaterRiskQuestionText
    )
  })
})

# Select the questions in scope
selected <- catalog %>%
  filter(topic_code %in% FULL_TOPICS | question_code %in% SELECT_CODES) %>%
  distinct(question_code, .keep_all = TRUE)

# -----------------------------------------------------------------------------
# 2. Target locations: National (XX) + states + DC
# -----------------------------------------------------------------------------
loc_raw <- fetch_json(paste0(BASE, "/YrbsExplorerLocations"))
locations <- loc_raw %>%
  as_tibble() %>%
  filter(LocationType == "State" |
           LocationDescription == "District of Columbia" |
           LocationCode == "XX") %>%
  mutate(
    # Map each location to the name used in resources/all_fips.csv.gz
    fips_name = case_when(
      LocationCode == "XX"  ~ "United States",
      LocationCode == "NYA" ~ "New York",   # YRBSS NY excludes NYC (see caveat)
      TRUE                  ~ LocationDescription
    )
  )

# -----------------------------------------------------------------------------
# 3. Download ChartData for every question x location (all years at once)
# -----------------------------------------------------------------------------
sig <- list(years = year_vec, questions = sort(selected$question_code))

if (!identical(process$raw_state$sig, sig) || !file.exists(RAW_FILE)) {

  message(sprintf("Downloading YRBSS ChartData: %d questions x %d locations",
                  nrow(selected), nrow(locations)))

  grid <- tidyr::expand_grid(
    question_code = selected$question_code,
    LocationCode  = locations$LocationCode
  )
  grid$url <- sprintf("%s/ChartData?QuestionId=%s&LocationId=%s&Yr=9999",
                      BASE, grid$question_code, grid$LocationCode)
  n_req <- nrow(grid)

  # Parallel fetch via a PSOCK cluster: each worker independently issues plain
  # sequential HTTPS requests (one connection at a time per worker). This is
  # far more robust against this server than curl's shared multiplex pool.

  # Progress instrumentation: each worker drops a marker file per completed
  # request, so live progress can be read by counting files in PROG_DIR.
  PROG_DIR <- "raw/.prog"
  unlink(PROG_DIR, recursive = TRUE)
  dir.create(PROG_DIR, showWarnings = FALSE, recursive = TRUE)

  pull_one <- function(i) {
    url <- grid$url[i]
    d <- NULL
    for (attempt in 1:3) {
      # Bounded timeout so a hung connection can't stall a worker for 60s
      d <- tryCatch(
        jsonlite::fromJSON(curl::curl(url, handle = curl::new_handle(
          timeout = 25, connecttimeout = 10)), simplifyVector = TRUE),
        error = function(e) NULL
      )
      if (!is.null(d)) break
      Sys.sleep(0.3)
    }
    file.create(file.path(PROG_DIR, as.character(i)))  # progress marker
    if (is.null(d) || length(d) == 0 || !is.data.frame(d) || nrow(d) == 0)
      return(NULL)
    # Keep only the strata in scope (Total, Sex, Race, Grade); drop the
    # sexual-identity / transgender / sex-of-sexual-contacts strata at the
    # scrape stage so they never enter the raw file.
    d <- d[d$StratType %in% c("Total", "Sex", "Race", "Grade"), , drop = FALSE]
    if (nrow(d) == 0) return(NULL)
    d$question_code <- grid$question_code[i]
    d$LocationCode  <- grid$LocationCode[i]
    d
  }

  n_workers <- 8
  message(sprintf("Fetching %d requests across %d workers (load-balanced)...",
                  n_req, n_workers))
  cl <- makeCluster(n_workers)
  on.exit(stopCluster(cl), add = TRUE)
  clusterEvalQ(cl, { library(jsonlite); library(curl) })
  clusterExport(cl, c("grid", "PROG_DIR"), envir = environment())
  # Load-balanced, one request per dispatch: idle workers immediately pull the
  # next request, so a few slow responses never stall the whole job.
  results <- parLapplyLB(cl, seq_len(n_req), pull_one, chunk.size = 1)
  stopCluster(cl)
  unlink(PROG_DIR, recursive = TRUE)  # clean up progress markers

  n_ok <- sum(!vapply(results, is.null, logical(1)))
  message(sprintf("Got data for %d / %d question-location pairs", n_ok, n_req))

  raw_all <- bind_rows(results)
  vroom::vroom_write(raw_all, RAW_FILE, delim = ",")
  message(sprintf("Saved %d raw rows to %s", nrow(raw_all), RAW_FILE))
} else {
  message("Raw signature unchanged; reusing cached ", RAW_FILE)
  raw_all <- vroom::vroom(RAW_FILE, show_col_types = FALSE,
                          col_types = vroom::cols(.default = "c"))
}

# -----------------------------------------------------------------------------
# 4. FIPS + stratum mapping helpers
# -----------------------------------------------------------------------------
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

all_fips_state <- all_fips %>%
  filter(geography_name %in% c("United States", "District of Columbia", state.name)) %>%
  filter(geography != "11001") %>%
  select(geography, geography_name)

loc_fips <- locations %>%
  left_join(all_fips_state, by = c("fips_name" = "geography_name")) %>%
  select(LocationCode, geography)

recode_race <- function(x) {
  dplyr::recode(x,
    "Black or African American"                  = "Black",
    "Hispanic or Latino"                         = "Hispanic",
    "American Indian or Alaska Native"           = "AI/AN",
    "Native Hawaiian or Other Pacific Islander"  = "NH/PI",
    "Multiple race"                              = "Multiple",
    .default = x
  )
}

# Approximate modal age for US grade levels (documented in measure_info.json)
grade_to_age <- function(x) {
  dplyr::recode(x,
    "9th" = "14", "10th" = "15", "11th" = "16", "12th" = "17",
    .default = x
  )
}

# -----------------------------------------------------------------------------
# 5. Build a tidy long table (one row per geography x time x stratum x measure).
#    Suppressed estimates (CDC emits a row with an empty MainValue) are KEPT
#    here as value = NA so they can be distinguished from questions that were
#    never asked (no row at all). topic / question_code are documented only in
#    measure_info.json and are NOT written to the data files.
# -----------------------------------------------------------------------------
raw_clean <- raw_all %>%
  filter(StratType %in% c("Total", "Sex", "Race", "Grade")) %>%
  mutate(
    Year      = suppressWarnings(as.integer(Year)),
    value     = suppressWarnings(as.numeric(na_if(trimws(as.character(MainValue)), ""))),
    value_lcl = suppressWarnings(as.numeric(na_if(trimws(as.character(LowCI)),  ""))),
    value_ucl = suppressWarnings(as.numeric(na_if(trimws(as.character(HighCI)), "")))
  ) %>%
  filter(Year >= MIN_YEAR) %>%
  left_join(loc_fips, by = "LocationCode") %>%
  filter(!is.na(geography)) %>%
  inner_join(select(measure_dict, question_code, slug), by = "question_code") %>%
  mutate(
    sex            = if_else(StratType == "Sex", Strat, "Overall"),
    race_ethnicity = if_else(StratType == "Race", recode_race(Strat), "Overall"),
    age            = if_else(StratType == "Grade", grade_to_age(Strat), "Overall"),
    time           = paste0(Year, "-12-31")
  ) %>%
  select(geography, time, age, sex, race_ethnicity,
         slug, value, value_lcl, value_ucl)

# A question was "asked" for a given geography + year if it appears in the raw
# data for that geography and year at all (in any stratum). Anything else is
# "not asked" (the question was not part of that jurisdiction's survey).
asked_set <- raw_clean %>%
  distinct(geography, time, slug) %>%
  mutate(asked = TRUE)

# Build the full set of cells for one stratification dimension: the cross of
# every observed key-row with every measure, annotated with value / CI / flags.
#   suppressed = 1  -> question asked but CDC suppressed this estimate (value 0)
#   not_asked  = 1  -> question not asked for this geography & year   (value 0)
# All missing value / _lcl / _ucl entries are set to 0.
build_cells <- function(sub, key_cols) {
  key_rows <- sub %>% distinct(across(all_of(key_cols)))
  tidyr::expand_grid(key_rows, slug = measure_dict$slug) %>%
    left_join(
      sub %>% select(all_of(key_cols), slug, value, value_lcl, value_ucl),
      by = c(key_cols, "slug")
    ) %>%
    left_join(asked_set, by = c("geography", "time", "slug")) %>%
    mutate(
      asked      = coalesce(asked, FALSE),
      not_asked  = if_else(asked, 0L, 1L),
      suppressed = if_else(asked & is.na(value), 1L, 0L),
      value      = coalesce(value, 0),
      value_lcl  = coalesce(value_lcl, 0),
      value_ucl  = coalesce(value_ucl, 0)
    ) %>%
    select(all_of(key_cols), slug, value, value_lcl, value_ucl,
           suppressed, not_asked)
}

# Reshape cells to wide: each measure becomes the columns <slug>, <slug>_lcl,
# <slug>_ucl, <slug>_suppressed, <slug>_not_asked (in that order), keyed by the
# supplied id columns.
col_order <- as.vector(t(outer(
  measure_dict$slug,
  c("", "_lcl", "_ucl", "_suppressed", "_not_asked"),
  paste0
)))

make_wide <- function(cells, id_cols) {
  cells %>%
    tidyr::pivot_longer(c(value, value_lcl, value_ucl, suppressed, not_asked),
                        names_to = "stat", values_to = "val") %>%
    mutate(variable = case_when(
      stat == "value"      ~ slug,
      stat == "value_lcl"  ~ paste0(slug, "_lcl"),
      stat == "value_ucl"  ~ paste0(slug, "_ucl"),
      stat == "suppressed" ~ paste0(slug, "_suppressed"),
      stat == "not_asked"  ~ paste0(slug, "_not_asked")
    )) %>%
    mutate(variable = factor(variable, levels = col_order)) %>%
    select(all_of(id_cols), variable, val) %>%
    tidyr::pivot_wider(names_from = variable, values_from = val) %>%
    arrange(across(all_of(id_cols)))
}

# data_age: age stratification only (sex & race held at Overall)
data_age <- raw_clean %>%
  filter(sex == "Overall", race_ethnicity == "Overall") %>%
  build_cells(c("geography", "time", "age")) %>%
  make_wide(c("geography", "time", "age"))

# data_age_sex: age and sex stratifications (race held at Overall)
data_age_sex <- raw_clean %>%
  filter(race_ethnicity == "Overall") %>%
  build_cells(c("geography", "time", "age", "sex")) %>%
  make_wide(c("geography", "time", "age", "sex"))

# data_age_ethnicity: age and race/ethnicity stratifications (sex held Overall)
data_age_ethnicity <- raw_clean %>%
  filter(sex == "Overall") %>%
  build_cells(c("geography", "time", "age", "race_ethnicity")) %>%
  make_wide(c("geography", "time", "age", "race_ethnicity"))

vroom::vroom_write(data_age,           "standard/data_age.csv.gz",           delim = ",")
vroom::vroom_write(data_age_sex,       "standard/data_age_sex.csv.gz",       delim = ",")
vroom::vroom_write(data_age_ethnicity, "standard/data_age_ethnicity.csv.gz", delim = ",")

message(sprintf(
  "Wrote standard files: data_age (%d rows), data_age_sex (%d rows), data_age_ethnicity (%d rows); %d measures across %d geographies",
  nrow(data_age), nrow(data_age_sex), nrow(data_age_ethnicity),
  dplyr::n_distinct(raw_clean$slug), dplyr::n_distinct(raw_clean$geography)))

# -----------------------------------------------------------------------------
# 6. Generate measure_info.json. One entry per measure (value + _lcl + _ucl).
#    topic and question_code, dropped from the data files, are documented here.
# -----------------------------------------------------------------------------
measures <- measure_dict %>%
  filter(slug %in% unique(raw_clean$slug)) %>%
  arrange(topic, question_code)

lc_first <- function(s) paste0(tolower(substr(s, 1, 1)), substring(s, 2))

measure_entries <- list()
for (i in seq_len(nrow(measures))) {
  m <- measures[i, ]
  category <- if (m$topic == "Unintentional Injuries and Violence") "injury" else "chronic"
  base_long_desc <- paste0(
    "Weighted percentage of U.S. high school students who ", lc_first(m$question_text),
    ", from the CDC Youth Risk Behavior Surveillance System (YRBSS) ",
    "(topic: ", m$topic, "; YRBSS question code: ", m$question_code, "). ",
    "Estimates are reported overall and stratified (separately) by sex, ",
    "race/ethnicity, and grade; grade is mapped to approximate modal ages ",
    "(9th=14, 10th=15, 11th=16, 12th=17). Missing estimates are set to 0 and ",
    "flagged: ", m$slug, "_suppressed = 1 marks values CDC suppressed (e.g., ",
    "small sample size), and ", m$slug, "_not_asked = 1 marks geography-years ",
    "in which the question was not asked. ",
    "Note: New York state estimates exclude New York City."
  )

  # Main value
  measure_entries[[m$slug]] <- list(
    id                = m$slug,
    short_name        = m$short_label,
    long_name         = paste0("YRBSS: ", m$question_text, " (", m$topic, ")"),
    category          = category,
    topic             = m$topic,
    question_code     = m$question_code,
    short_description = paste0("Percent of high school students who ",
                               lc_first(m$question_text), "."),
    long_description  = base_long_desc,
    measure_type      = "Percent",
    unit              = "Percent",
    time_resolution   = "Year",
    sources           = list(list(id = "yrbss"))
  )
  # Lower 95% CI
  measure_entries[[paste0(m$slug, "_lcl")]] <- list(
    id                = paste0(m$slug, "_lcl"),
    short_name        = paste0(m$short_label, " - lower 95% CI"),
    long_name         = paste0("YRBSS: ", m$question_text,
                               " - lower bound of 95% confidence interval"),
    category          = category,
    topic             = m$topic,
    question_code     = m$question_code,
    short_description = paste0("Lower bound of the 95% confidence interval for the percent of high school students who ",
                               lc_first(m$question_text), "."),
    long_description  = paste0("Lower bound of the 95% confidence interval for ", m$slug, ". ",
                               base_long_desc),
    measure_type      = "Percent",
    unit              = "Percent",
    time_resolution   = "Year",
    sources           = list(list(id = "yrbss"))
  )
  # Upper 95% CI
  measure_entries[[paste0(m$slug, "_ucl")]] <- list(
    id                = paste0(m$slug, "_ucl"),
    short_name        = paste0(m$short_label, " - upper 95% CI"),
    long_name         = paste0("YRBSS: ", m$question_text,
                               " - upper bound of 95% confidence interval"),
    category          = category,
    topic             = m$topic,
    question_code     = m$question_code,
    short_description = paste0("Upper bound of the 95% confidence interval for the percent of high school students who ",
                               lc_first(m$question_text), "."),
    long_description  = paste0("Upper bound of the 95% confidence interval for ", m$slug, ". ",
                               base_long_desc),
    measure_type      = "Percent",
    unit              = "Percent",
    time_resolution   = "Year",
    sources           = list(list(id = "yrbss"))
  )
  # Suppressed flag
  measure_entries[[paste0(m$slug, "_suppressed")]] <- list(
    id                = paste0(m$slug, "_suppressed"),
    short_name        = paste0(m$short_label, " - suppressed flag"),
    long_name         = paste0("YRBSS: ", m$question_text, " - suppressed flag"),
    category          = category,
    topic             = m$topic,
    question_code     = m$question_code,
    short_description = paste0("1 if the estimate for '", m$slug,
                               "' was suppressed by CDC and set to 0; 0 otherwise."),
    long_description  = paste0(
      "Flag indicating CDC suppression. 1 means the question was asked for this ",
      "geography and year but CDC suppressed the estimate (e.g., insufficient ",
      "sample size), and ", m$slug, " (with its CI) was set to 0; 0 otherwise. ",
      "Distinct from ", m$slug, "_not_asked, which marks geography-years where ",
      "the question was not asked at all."
    ),
    measure_type      = "Indicator",
    unit              = "0/1 flag",
    time_resolution   = "Year",
    sources           = list(list(id = "yrbss"))
  )
  # Not-asked flag
  measure_entries[[paste0(m$slug, "_not_asked")]] <- list(
    id                = paste0(m$slug, "_not_asked"),
    short_name        = paste0(m$short_label, " - not-asked flag"),
    long_name         = paste0("YRBSS: ", m$question_text, " - not-asked flag"),
    category          = category,
    topic             = m$topic,
    question_code     = m$question_code,
    short_description = paste0("1 if '", m$slug,
                               "' was not asked for this geography and year and set to 0; 0 otherwise."),
    long_description  = paste0(
      "Flag indicating the question was not asked. 1 means '", m$question_text,
      "' was not part of the YRBSS survey for this geography and year, so ", m$slug,
      " (with its CI) was set to 0; 0 otherwise. Distinct from ", m$slug,
      "_suppressed, which marks estimates CDC suppressed."
    ),
    measure_type      = "Indicator",
    unit              = "0/1 flag",
    time_resolution   = "Year",
    sources           = list(list(id = "yrbss"))
  )
}

measure_info <- measure_entries
measure_info[["_sources"]] <- list(
  yrbss = list(
    name             = "CDC Youth Risk Behavior Surveillance System (YRBSS)",
    url              = "https://yrbs-explorer.services.cdc.gov/",
    date_accessed    = 2025,
    organization     = "Centers for Disease Control and Prevention",
    organization_url = "https://www.cdc.gov/yrbs/",
    description      = paste0(
      "The Youth Risk Behavior Surveillance System (YRBSS) is a set of ",
      "school-based surveys conducted by the CDC that monitor health-related ",
      "behaviors among U.S. high school students. The biennial national, ",
      "state, and local surveys provide weighted prevalence estimates of ",
      "behaviors contributing to the leading causes of death and disability. ",
      "Data were accessed via the YRBS Explorer API. Estimates are provided ",
      "overall and stratified (separately, not crossed) by sex, race/ethnicity, ",
      "and grade. Estimates that CDC suppressed (e.g., small sample sizes) are ",
      "omitted rather than imputed. State estimates are available only for ",
      "jurisdictions that share data with CDC; Minnesota, Oregon, and ",
      "Washington are not included, and New York state excludes New York City."
    ),
    restrictions     = "Public domain. Suggested attribution: Centers for Disease Control and Prevention (CDC). Youth Risk Behavior Surveillance System (YRBSS)."
  )
)

jsonlite::write_json(measure_info, "measure_info.json",
                     pretty = TRUE, auto_unbox = TRUE, null = "null")

# -----------------------------------------------------------------------------
# 7. Update process record
# -----------------------------------------------------------------------------
process$raw_state <- list(sig = sig, hash = unname(tools::md5sum(RAW_FILE)))
dcf::dcf_process_record(updated = process)
