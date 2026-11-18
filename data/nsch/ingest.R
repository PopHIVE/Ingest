# =============================================================================
# NSCH (National Survey of Children's Health) Data Ingestion
# Source: U.S. Census Bureau public-use topical files (Stata), 2016 onward
#   https://www.census.gov/programs-surveys/nsch/data/datasets.html
#   https://www2.census.gov/programs-surveys/nsch/datasets/{year}/
#
# What this script does
#   1. Finds every survey year's topical Stata file on www2.census.gov and
#      records its Last-Modified / Content-Length. A year is (re)downloaded only
#      when that changes or its extract is missing. The zip lands in
#      raw/zip_cache/ (gitignored); only the variables this script uses are kept,
#      in raw/nsch_{year}_extract.csv.gz (committed).
#   2. Codes each indicator with an exact R port of the Census Bureau / HRSA
#      MCHB "Title V" code ("Title _V_Code_SAS_2016-2025.sas", published on the
#      NSCH datasets page). Variable names, response codes, universes and
#      year restrictions follow that file line by line; the SAS variable each
#      measure comes from is recorded in measure_info.json.
#   3. Produces single-year, design-based weighted percentages with 95% logit
#      confidence intervals, nationally and for each state + DC, by age group,
#      age x sex, and age x race/ethnicity.
#   4. Applies the NSCH Comprehensive Analytic Guide "Data Presentation
#      Standards" (unweighted denominator < 30 -> suppressed; 95% CI width
#      > 20 percentage points or > 1.2 x the estimate -> unreliable flag).
#
# Missing-value codes. The Stata files carry Census's extended missing values
# (.m = missing/no response, .l = logical skip, .n = not in universe), which
# haven reads as tagged NAs. They are kept as the strings ".M", ".L", ".N" (and
# "." for a variable absent from a year's file), so the Title V logic -- which
# tests e.g. `K2Q33B in (.L,2)` -- can be ported verbatim. A measure value of
# "1" is the Title V numerator ("Yes"), "2" the remaining denominator ("No");
# anything else is excluded, exactly as PROC SURVEYFREQ excludes missing.
#
# Variance. Strata = FIPSST x STRATUM, PSU = HHID, weight = FWC (Analytic Guide,
# "Variance Estimation"). Subgroup estimates use the full design (domain
# estimation), as the Guide requires, via Taylor linearization computed from
# per-stratum sufficient statistics. CIs are logit-transformed (SAS
# `cl(type=logit)`, as in the Title V code) with t degrees of freedom =
# #PSUs - #strata of the year's full file.
# =============================================================================

library(dplyr)
library(tidyr)

BASE       <- "https://www2.census.gov/programs-surveys/nsch/datasets"
FIRST_YEAR <- 2016          # redesigned NSCH; not comparable with earlier surveys
ZIP_DIR    <- "raw/zip_cache"
MIN_N      <- 30            # Analytic Guide: present only if unweighted denominator >= 30
CI_MAX_PP  <- 20            # Analytic Guide: flag if 95% CI width > 20 percentage points
CI_MAX_REL <- 1.2           #                 ... or > 1.2 x the estimate

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# Variables kept in the raw extract (upper case, as in the Title V code)
# -----------------------------------------------------------------------------
DIFFICULTY_VARS <- c("BREATHING", "SWALLOWING", "STOMACH", "PHYSICALPAIN", "HANDS",
                     "COORDINATION", "MEMORYCOND", "WALKSTAIRS", "DRESSING",
                     "ERRANDALONE", "K2Q43B", "BLINDNESS")
CONDITION_VARS  <- c("AUTOIMMUNE", "ALLERGIES_CURR", "ARTHRITIS_CURR", "K2Q40B", "BLOOD",
                     "K2Q61A", "CYSTFIB", "K2Q41B", "DIABETES_CURR", "DOWNSYN", "K2Q42B",
                     "GENETIC", "HEART_CURR", "HEADACHE_CURR", "K2Q38B", "K2Q33B", "K2Q32B",
                     "K2Q34B", "K2Q36B", "K2Q60B", "K2Q37B", "K2Q30B", "K2Q35B", "K2Q31B",
                     "FASD")
KEEP_VARS <- unique(c(
  "YEAR", "FIPSST", "STRATUM", "HHID", "FWC",
  "SC_AGE_YEARS", "SC_AGE_LT6", "SC_SEX", "SC_RACE_R", "SC_HISPANIC_R", "SC_CSHCN",
  "K6Q70_R", "K6Q71_R", "K6Q72_R", "K6Q73_R", "K7Q84_R", "K7Q85_R",
  "K4Q22_R", "K9Q96", "BULLY", "BULLIED_R",
  paste0("ACE", c(1, 3:11)),
  "K2Q01", "BMICLASS", "PHYSACTIV", "K4Q20R", "S4Q01",
  "DENTISTVISIT", "K4Q30_R", "K4Q30_R_3", "CAVITIES",
  "CURRINS", "INSGAP", "K3Q20", "K3Q21B", "K3Q22", "HOWMUCH", "K4Q27",
  "FOODSIT", "EVERHOMELESS", "MISSMORTGAGE", "PLACESLIVED", "MOVES", "K9Q40",
  "K8Q21",
  DIFFICULTY_VARS, CONDITION_VARS
))
# Changing KEEP_VARS forces every year's extract to be rebuilt
VARS_SIG <- paste(sort(KEEP_VARS), collapse = ",")

# -----------------------------------------------------------------------------
# 1. Discover source files and their server metadata
# -----------------------------------------------------------------------------
list_links <- function(url) {
  html <- paste(readLines(url, warn = FALSE), collapse = "\n")
  m <- gregexpr('href="[^"]+"', html)[[1]]
  sub('^href="([^"]+)"$', "\\1", regmatches(html, list(m))[[1]])
}

head_meta <- function(url) {
  res <- curl::curl_fetch_memory(url, handle = curl::new_handle(nobody = TRUE))
  if (res$status_code != 200) stop("HEAD ", url, " returned ", res$status_code, call. = FALSE)
  h <- curl::parse_headers_list(res$headers)
  paste(h[["last-modified"]], h[["content-length"]], sep = " | ")
}

years <- as.integer(sub("/$", "", grep("^[0-9]{4}/$", list_links(paste0(BASE, "/")), value = TRUE)))
years <- sort(years[years >= FIRST_YEAR])

source_files <- bind_rows(lapply(years, function(y) {
  zip <- grep("^nsch_[0-9]{4}e?_topical_Stata\\.zip$",
              list_links(sprintf("%s/%d/", BASE, y)), value = TRUE)
  if (length(zip) != 1) stop("Expected one topical Stata zip for ", y, ", found ",
                             length(zip), call. = FALSE)
  url <- sprintf("%s/%d/%s", BASE, y, zip)
  tibble(year = y, zip = zip, url = url, meta = head_meta(url))
}))
message("NSCH topical files found for ", min(years), "-", max(years))

# One string per year; compared against the stored state. (Stored as a JSON
# array, which dcf_process_record() reads back as a list, hence unlist().)
current_files <- with(source_files, paste(year, zip, meta, sep = " | "))
stored_files  <- as.character(unlist(process$raw_state$files))
vars_changed  <- !identical(as.character(unlist(process$raw_state$vars)), VARS_SIG)
extract_path  <- function(y) sprintf("raw/nsch_%d_extract.csv.gz", y)

# -----------------------------------------------------------------------------
# 2. Download and extract changed years
# -----------------------------------------------------------------------------
# Encode one variable from haven: numbers as text, Census extended missing
# values as ".M" / ".L" / ".N", any other NA as ".".
encode_var <- function(x) {
  x <- haven::zap_labels(x)
  if (is.character(x)) return(ifelse(is.na(x) | x == "", ".", x))
  out <- ifelse(is.na(x), ".", sprintf("%.15g", as.numeric(x)))
  tag <- haven::na_tag(x)
  out[!is.na(tag)] <- paste0(".", toupper(tag[!is.na(tag)]))
  out
}

to_extract <- source_files %>%
  filter(vars_changed | !current_files %in% stored_files | !file.exists(extract_path(year)))

if (nrow(to_extract) > 0) {
  dir.create(ZIP_DIR, showWarnings = FALSE, recursive = TRUE)
  for (i in seq_len(nrow(to_extract))) {
    f <- to_extract[i, ]
    zip_file <- file.path(ZIP_DIR, f$zip)
    server_size <- suppressWarnings(as.numeric(sub(".*\\| ", "", f$meta)))
    if (!file.exists(zip_file) || is.na(server_size) || file.size(zip_file) != server_size) {
      message("Downloading ", f$url)
      old_timeout <- options(timeout = max(600, getOption("timeout")))
      download.file(f$url, zip_file, mode = "wb", quiet = TRUE)
      options(old_timeout)
    }
    exdir <- file.path(tempdir(), paste0("nsch_", f$year))
    dta <- grep("\\.dta$", unzip(zip_file, exdir = exdir), value = TRUE)
    if (length(dta) != 1) stop("Expected one .dta in ", f$zip, call. = FALSE)
    d <- haven::read_dta(dta)
    names(d) <- toupper(names(d))
    if (!all(as.numeric(d$YEAR) == f$year)) stop("YEAR in ", f$zip, " does not match ", f$year, call. = FALSE)
    if (anyDuplicated(d$HHID)) stop("HHID is not unique in ", f$zip, call. = FALSE)
    present <- intersect(KEEP_VARS, names(d))
    ext <- as_tibble(lapply(d[present], encode_var))
    vroom::vroom_write(ext, extract_path(f$year), delim = ",")
    message(sprintf("  %d: %d children, %d of %d variables present (absent: %s)",
                    f$year, nrow(ext), length(present), length(KEEP_VARS),
                    paste(setdiff(KEEP_VARS, present), collapse = " ")))
    unlink(exdir, recursive = TRUE)
  }
}

data_changed <- vars_changed || !identical(sort(stored_files), sort(current_files)) ||
  !all(file.exists(extract_path(years)))

# ---------------------------------------------------------------------------
# 3. Measure dictionary (always built: used for estimation and measure_info)
#    tv_var: the Title V code's variable name (or, for K8Q21, the NSCH item).
#    description / coding / notes: plain-language text for measure_info.json.
#    years: data years the measure is reported for (Title V year restrictions).
# ---------------------------------------------------------------------------
measure_dict <- tibble::tribble(
  ~slug, ~tv_var, ~short_name, ~category, ~universe,
  "flourishing_6_17", "flrish6to17",
    "Flourishing, ages 6-17", "behavioral_health", "Children ages 6-17",
  "flourishing_6mo_5", "flrish0to5",
    "Flourishing, ages 6 months-5 years", "behavioral_health", "Children ages 6 months-5 years",
  "depression_anxiety_12_17", "DepAnx_12to17",
    "Current depression or anxiety, ages 12-17", "behavioral_health", "Adolescents ages 12-17",
  "anxiety_12_17", "anxiety",
    "Current anxiety problems, ages 12-17", "behavioral_health", "Adolescents ages 12-17",
  "depression_12_17", "depress",
    "Current depression, ages 12-17", "behavioral_health", "Adolescents ages 12-17",
  "behavior_conduct_6_11", "BehConduct_6to11",
    "Current behavioral or conduct problem, ages 6-11", "behavioral_health", "Children ages 6-11",
  "mh_treatment_12_17", "MHTx_12to17",
    "Received needed mental health treatment, ages 12-17", "utilization",
    "Adolescents ages 12-17 who needed mental health treatment or counseling",
  "adult_mentor_12_17", "Mentor_12to17",
    "Has an adult mentor outside the home, ages 12-17", "behavioral_health", "Adolescents ages 12-17",
  "bullied_12_17", "AreBullied",
    "Bullied, picked on, or excluded, ages 12-17", "behavioral_health", "Adolescents ages 12-17",
  "bullies_others_12_17", "BullyOthers",
    "Bullies, picks on, or excludes others, ages 12-17", "behavioral_health", "Adolescents ages 12-17",
  "aces_2plus", "ACE2",
    "Two or more adverse childhood experiences", "behavioral_health", "Children ages 0-17",
  "health_excellent_vgood", "HealthStatus",
    "Health excellent or very good", "chronic", "Children ages 0-17",
  "obesity_6_17", "ChAdolBMI",
    "Obesity (BMI >= 95th percentile), ages 6-17", "chronic", "Children ages 6-17",
  "active_daily_6_11", "Physactiv_6to11",
    "Physically active 60+ minutes every day, ages 6-11", "chronic", "Children ages 6-11",
  "active_daily_12_17", "PhysActiv_12to17",
    "Physically active 60+ minutes every day, ages 12-17", "chronic", "Adolescents ages 12-17",
  "well_visit_12_17", "WellVisit_12to17",
    "Preventive medical visit in past year, ages 12-17", "utilization", "Adolescents ages 12-17",
  "preventive_dental_1_17", "PrevDent_1to17",
    "Preventive dental visit in past year, ages 1-17", "utilization", "Children ages 1-17",
  "tooth_decay_1_17", "ToothDecay",
    "Decayed teeth or cavities in past year, ages 1-17", "chronic", "Children ages 1-17",
  "adequately_insured", "AdqIns",
    "Continuously and adequately insured", "access", "Children ages 0-17",
  "forgone_care", "ForgoneHlth",
    "Needed health care but did not receive it", "access", "Children ages 0-17",
  "food_sufficient_0_11", "Food_0to11",
    "Household food sufficient, ages 0-11", "demographic", "Children ages 0-11",
  "housing_instability_0_11", "Housing_0to11",
    "Housing instability in past year, ages 0-11", "demographic", "Children ages 0-11",
  "household_smoking", "HHSmoking",
    "Lives in a household where someone smokes", "environment", "Children ages 0-17",
  "cshcn", "F11CSHCN",
    "Special health care needs (expanded definition)", "chronic", "Children ages 0-17",
  "adhd_3_17", "F11ADD",
    "Current ADD/ADHD, ages 3-17", "chronic", "Children ages 3-17",
  "autism_3_17", "F11Autism",
    "Current autism/ASD, ages 3-17", "chronic", "Children ages 3-17",
  "share_ideas_well_6_17", "K8Q21",
    "Parent and child share ideas very or somewhat well, ages 6-17", "behavioral_health", "Children ages 6-17"
) %>% mutate(col = paste0("nsch_pct_", slug),
             is_title_v = tv_var != "K8Q21")

measure_dict <- measure_dict %>% mutate(
  # One plain-language sentence (short_description). Each starts with
  # "Percent of" so the narrative `statement` can be derived from it.
  description = recode(tv_var,
    flrish6to17 = "Percent of children ages 6-17 whose parent says they always or usually show curiosity about learning, finish tasks they start, and stay calm when challenged (all three).",
    flrish0to5 = "Percent of children ages 6 months-5 years whose parent says they are always or usually affectionate, curious, smiling and laughing, and quick to bounce back (all four).",
    DepAnx_12to17 = "Percent of adolescents ages 12-17 whose parent reports they currently have depression or anxiety problems diagnosed by a health care provider.",
    anxiety = "Percent of adolescents ages 12-17 whose parent reports they currently have anxiety problems diagnosed by a health care provider.",
    depress = "Percent of adolescents ages 12-17 whose parent reports they currently have depression diagnosed by a health care provider.",
    BehConduct_6to11 = "Percent of children ages 6-11 whose parent reports they currently have behavioral or conduct problems diagnosed by a health care provider.",
    MHTx_12to17 = "Percent of adolescents ages 12-17 who needed mental health treatment or counseling in the past year and received it.",
    Mentor_12to17 = "Percent of adolescents ages 12-17 with at least one adult outside the home whom they can rely on for advice or guidance.",
    AreBullied = "Percent of adolescents ages 12-17 who were bullied, picked on, or excluded by other children at least once in the past year.",
    BullyOthers = "Percent of adolescents ages 12-17 who bullied, picked on, or excluded other children at least once in the past year.",
    ACE2 = "Percent of children ages 0-17 who have had two or more adverse childhood experiences, such as parental divorce, a parent in jail, or witnessing violence.",
    HealthStatus = "Percent of children ages 0-17 whose parent rates their health as excellent or very good.",
    ChAdolBMI = "Percent of children ages 6-17 with obesity (body mass index at or above the 95th percentile).",
    Physactiv_6to11 = "Percent of children ages 6-11 who were physically active for at least 60 minutes every day in the past week.",
    PhysActiv_12to17 = "Percent of adolescents ages 12-17 who were physically active for at least 60 minutes every day in the past week.",
    WellVisit_12to17 = "Percent of adolescents ages 12-17 with at least one preventive medical check-up in the past year.",
    PrevDent_1to17 = "Percent of children ages 1-17 with at least one preventive dental visit in the past year.",
    ToothDecay = "Percent of children ages 1-17 who had frequent or chronic decayed teeth or cavities in the past year.",
    AdqIns = "Percent of children ages 0-17 who were insured all year with adequate coverage (meets their needs, lets them see needed providers, and has reasonable costs).",
    ForgoneHlth = "Percent of children ages 0-17 who needed health care in the past year but did not receive it.",
    Food_0to11 = "Percent of children ages 0-11 whose household could always afford to eat good, nutritious meals in the past year.",
    Housing_0to11 = "Percent of children ages 0-11 who experienced housing instability: ever homeless, a late rent or mortgage payment in the past year, or two or more moves in the past year.",
    HHSmoking = "Percent of children ages 0-17 who live with someone who uses cigarettes, cigars, or pipe tobacco.",
    F11CSHCN = "Percent of children ages 0-17 with special health care needs (qualify on the CSHCN screener, or have at least one listed condition and at least one functional difficulty).",
    F11ADD = "Percent of children ages 3-17 whose parent reports they currently have ADD/ADHD diagnosed by a health care provider.",
    F11Autism = "Percent of children ages 3-17 whose parent reports they currently have autism or autism spectrum disorder diagnosed by a health care provider.",
    K8Q21 = "Percent of children ages 6-17 whose parent says they and the child can share ideas or talk about things that really matter very well or somewhat well."),
  # How the measure is counted, restated from the Title V code
  coding = recode(tv_var,
    flrish6to17 = "A child counts as flourishing only if the parent answered 'Always' or 'Usually' to all three questions; a skipped question counts as not met. Children with all three questions skipped are excluded.",
    flrish0to5 = "A child counts as flourishing only if the parent answered 'Always' or 'Usually' to all four questions; a skipped question counts as not met. Children younger than 6 months and children with all four questions skipped are excluded.",
    DepAnx_12to17 = "An adolescent counts if a health care provider ever said they had depression or anxiety problems and the parent says they currently have it. Adolescents never diagnosed count as not having either condition.",
    anxiety = "An adolescent counts if a health care provider ever said they had anxiety problems and the parent says they currently have them. Adolescents never diagnosed count as not having anxiety problems.",
    depress = "An adolescent counts if a health care provider ever said they had depression and the parent says they currently have it. Adolescents never diagnosed count as not having depression.",
    BehConduct_6to11 = "A child counts if a health care provider ever said they had behavioral or conduct problems and the parent says they currently have them. Children never diagnosed count as not having these problems.",
    MHTx_12to17 = "Includes only adolescents who needed to see a mental health professional in the past 12 months; an adolescent counts if they received treatment or counseling.",
    Mentor_12to17 = "An adolescent counts if the parent says at least one adult in the child's school, neighborhood, or community knows them well and is someone they can rely on for advice or guidance.",
    AreBullied = "An adolescent counts if they were bullied, picked on, or excluded at least once or twice in the past 12 months; the rest answered 'Never'.",
    BullyOthers = "An adolescent counts if they bullied, picked on, or excluded others at least once or twice in the past 12 months; the rest answered 'Never'.",
    ACE2 = "Experiences counted: hard to cover basics like food or housing on the family's income, parent divorced or separated, parent died, parent served time in jail or prison, violence between adults in the home, victim or witness of neighborhood violence, lived with someone mentally ill or with an alcohol or drug problem, and treated unfairly because of race or ethnicity or a health condition. A child counts with two or more; children with every question skipped are excluded.",
    HealthStatus = "The parent rates the child's health as excellent, very good, good, fair, or poor; the measure counts excellent or very good.",
    ChAdolBMI = "A child counts if their body mass index is at or above the 95th percentile.",
    Physactiv_6to11 = "Based on how many days in the past week the child was active for at least 60 minutes; the measure counts 'every day'.",
    PhysActiv_12to17 = "Based on how many days in the past week the adolescent was active for at least 60 minutes; the measure counts 'every day'.",
    WellVisit_12to17 = "An adolescent counts if they had one or more preventive check-ups in the past 12 months. Adolescents with no health care visit of any kind count as not having one.",
    PrevDent_1to17 = "A child counts if they had one or more preventive dental visits (check-ups, cleanings, sealants, or fluoride) in the past 12 months. Children with no dental care of any kind count as not having one.",
    ToothDecay = "A child counts if the parent reports frequent or chronic decayed teeth or cavities in the past 12 months.",
    AdqIns = "A child counts if they were insured for all of the past 12 months and their insurance always or usually covers needed services, lets them see needed providers, and has reasonable out-of-pocket costs (or none). A skipped adequacy question does not disqualify a child.",
    ForgoneHlth = "A child counts if they needed health care in the past 12 months, including dental, vision, or mental health care, but did not get it.",
    Food_0to11 = "A child counts if the household could always afford good, nutritious meals; the rest had enough to eat but not always the right kinds of food, or sometimes or often could not afford enough. Higher values are better.",
    Housing_0to11 = "A child counts if they were ever homeless or lived in a shelter, the family could not pay the rent or mortgage on time in the past 12 months, or the child moved two or more times in the past 12 months. 'Don't know' and skipped questions do not count as instability.",
    HHSmoking = "A child counts if anyone living in the household uses cigarettes, cigars, or pipe tobacco.",
    F11CSHCN = "A child counts if they qualify on the five-question CSHCN screener, or if they have at least one of a list of current health conditions together with at least one functional difficulty.",
    F11ADD = "A child counts if a health care provider ever said they had ADD/ADHD and the parent says they currently have it. Children never diagnosed count as not having ADD/ADHD.",
    F11Autism = "A child counts if a health care provider ever said they had autism or ASD and the parent says they currently have it. Children never diagnosed count as not having autism.",
    K8Q21 = "The parent answers very well, somewhat well, not very well, or not well at all; the measure counts very well or somewhat well."),
  notes = recode(tv_var,
    flrish6to17 = "It matches the 'Meets all 3 flourishing items' category of indicator 2.4 from the Data Resource Center for Child and Adolescent Health (childhealthdata.org).",
    flrish0to5 = "It matches the 'Meets all 4 flourishing items' category of indicator 2.3 from the Data Resource Center for Child and Adolescent Health (childhealthdata.org).",
    ACE2 = "Unfair treatment because of a health condition or disability was added in 2021, so 2016-2020 counts are out of nine experiences instead of ten.",
    WellVisit_12to17 = "Telehealth visits are included from 2021.",
    Housing_0to11 = "The moves question changed format in 2024.",
    F11CSHCN = "Not every listed condition is asked every year; a condition not asked that year is not counted.",
    K8Q21 = "Not a Title V measure: reported directly from a single survey question.",
    .default = ""),
  years = recode(tv_var,
    flrish6to17 = "2018 onward", flrish0to5 = "2018 onward",
    AreBullied = "2018 onward", BullyOthers = "2018 onward",
    Housing_0to11 = "2022 onward",
    WellVisit_12to17 = "2016-2017 and 2019 onward (not reported for 2018)",
    .default = "2016 onward")
)
stopifnot(!anyNA(measure_dict$description), all(startsWith(measure_dict$description, "Percent of ")),
          !any(measure_dict$coding == measure_dict$tv_var))

# Also rebuild when the standard files' columns no longer match the measure
# list (e.g. a measure was added or removed)
expected_cols <- c("geography", "time", "age", as.vector(t(outer(measure_dict$col,
  c("", "_lcl", "_ucl", "_n", "_suppressed", "_unreliable", "_not_asked"), paste0))))
data_changed <- data_changed || !file.exists("standard/data_age.csv.gz") ||
  !identical(names(vroom::vroom("standard/data_age.csv.gz", n_max = 0, show_col_types = FALSE,
                                col_types = vroom::cols(.default = "c"))), expected_cols)

if (data_changed) {

  # ---------------------------------------------------------------------------
  # 4. Title V coding (port of Title _V_Code_SAS_2016-2025.sas)
  #    Codes are strings: "1"/"2"/... for responses, ".M"/".L"/".N"/"." missing.
  #    SAS sequential `if ... then x = ...;` statements become successive
  #    in-place assignments, so later statements override earlier ones as in SAS.
  # ---------------------------------------------------------------------------
  title_v <- function(d, year) {
    n <- nrow(d)
    v <- function(name) if (name %in% names(d)) d[[name]] else rep(".", n)
    na_vec <- function() rep(".", n)
    age <- as.numeric(d$SC_AGE_YEARS)
    lt6 <- v("SC_AGE_LT6") == "1"
    out <- list()

    if (year >= 2018) {
      # Flourishing, ages 6-17: flrish6to17 = 3 ("Meets all 3 flourishing
      # items"); an unanswered item counts as not met. The Title V NOM
      # (FlrshChild), which ignores unanswered items, is not reported.
      it <- list(curious = v("K6Q71_R"), finishes = v("K7Q84_R"), resil = v("K7Q85_R"))
      it <- lapply(it, function(x) { x[age < 6] <- ".N"; x })
      ct   <- Reduce(`+`, lapply(it, function(x) x %in% c("1", "2")))
      allM <- Reduce(`&`, lapply(it, function(x) x == ".M"))
      x <- ifelse(ct == 3, "1", "2"); x[allM] <- ".M"; x[age < 6] <- ".N"
      out$flrish6to17 <- x

      # Flourishing, ages 6 months-5 years: flrish0to5 = 3 ("Meets all 4
      # flourishing items"). The Title V NOM (FlrshYoung) is not reported.
      it <- list(tender = v("K6Q70_R"), resil = v("K6Q73_R"), curious = v("K6Q71_R"), smile = v("K6Q72_R"))
      it <- lapply(it, function(x) { x[lt6] <- ".L"; x[age > 5] <- ".N"; x })
      ct   <- Reduce(`+`, lapply(it, function(x) x %in% c("1", "2")))
      allM <- Reduce(`&`, lapply(it, function(x) x == ".M"))
      x <- ifelse(ct == 4, "1", "2"); x[allM & !lt6 & age <= 5] <- ".M"; x[lt6] <- ".L"; x[age > 5] <- ".N"
      out$flrish0to5 <- x
    } else {
      out$flrish6to17 <- out$flrish0to5 <- na_vec()
    }

    # NOM_Adolescent Depression/Anxiety and its components
    current <- function(src) {
      x <- na_vec(); x[src == "1"] <- "1"; x[src %in% c(".L", "2")] <- "2"; x[src == ".M"] <- ".M"; x
    }
    anx <- current(v("K2Q33B")); anx[age < 12] <- ".L"
    dep <- current(v("K2Q32B")); dep[age < 12] <- ".L"
    x <- na_vec()
    x[anx == "1" | dep == "1"] <- "1"
    x[anx %in% c("2", ".M") & dep %in% c("2", ".M")] <- "2"
    x[anx == ".M" & dep == ".M"] <- ".M"
    x[age < 12] <- ".L"
    out$DepAnx_12to17 <- x; out$anxiety <- anx; out$depress <- dep

    # NOM_Behavioral/Conduct Problem
    x <- current(v("K2Q34B")); x[age < 6 | age > 11] <- ".L"; out$BehConduct_6to11 <- x

    # NPM_Mental Health Tx
    k <- v("K4Q22_R"); x <- na_vec()
    x[k %in% c("1", "2")] <- k[k %in% c("1", "2")]; x[k == "3"] <- ".L"; x[k == ".M"] <- ".M"
    x[age < 12] <- ".L"; out$MHTx_12to17 <- x

    # NPM_Adult Mentor
    x <- v("K9Q96"); x[age < 12] <- ".N"; out$Mentor_12to17 <- x

    # NPM_Bullying (2018 onward; variables absent earlier)
    bully_code <- function(src) {
      x <- na_vec(); x[src %in% c("2", "3", "4", "5")] <- "1"; x[src == "1"] <- "2"; x[src == ".M"] <- ".M"
      x[age < 12] <- ".L"; x
    }
    out$BullyOthers <- bully_code(v("BULLY"))
    out$AreBullied  <- bully_code(v("BULLIED_R"))

    # NOM_Adverse Childhood Experiences
    ace_other <- paste0("ACE", 3:11)
    ct <- (v("ACE1") %in% c("3", "4")) + Reduce(`+`, lapply(ace_other, function(a) v(a) == "1"))
    allM <- Reduce(`&`, lapply(c("ACE1", paste0("ACE", 3:10)), function(a) v(a) == ".M")) &
      v("ACE11") %in% c(".", ".M")
    x <- ifelse(ct > 1, "1", "2"); x[allM] <- ".M"; out$ACE2 <- x

    # NOM_Children's Health Status
    k <- v("K2Q01"); x <- na_vec()
    x[k %in% c("1", "2")] <- "1"; x[k %in% c("3", "4", "5")] <- "2"; x[k == ".M"] <- ".M"
    out$HealthStatus <- x

    # NOM_Child-Adolescent Obesity
    k <- v("BMICLASS"); x <- na_vec()
    x[k == "4"] <- "1"; x[k %in% c("1", "2", "3")] <- "2"; x[k == ".M"] <- ".M"; x[age < 6] <- ".L"
    out$ChAdolBMI <- x

    # NPM_Physical Activity (6-11) and SM_Adolescent Physical Activity (12-17)
    k <- v("PHYSACTIV")
    x <- na_vec(); x[k == "4"] <- "1"; x[k %in% c("1", "2", "3")] <- "2"; x[age < 6 | age > 11] <- ".L"
    out$Physactiv_6to11 <- x
    x <- na_vec(); x[k == "4"] <- "1"; x[k %in% c("1", "2", "3")] <- "2"; x[age < 12] <- ".L"
    out$PhysActiv_12to17 <- x

    # NPM_Adolescent Well-Visit (not computed for 2018)
    k <- v("K4Q20R"); prevmed <- na_vec()
    prevmed[k %in% c("2", "3")] <- "1"; prevmed[k == "1"] <- "2"; prevmed[k == ".M"] <- ".M"
    prevmed[v("S4Q01") == "2"] <- "2"
    if (year != 2018) { x <- prevmed; x[age < 12] <- ".L" } else x <- na_vec()
    out$WellVisit_12to17 <- x

    # NPM_Preventive Dental Visit
    k <- v("DENTISTVISIT"); x <- na_vec()
    x[k %in% c("2", "3")] <- "1"; x[k == "1"] <- "2"; x[k == ".M"] <- ".M"
    x[v("K4Q30_R") == "3" | v("K4Q30_R_3") == "1"] <- "2"
    x[age < 1] <- ".L"
    out$PrevDent_1to17 <- x

    # NOM_Tooth Decay/Cavities
    x <- v("CAVITIES"); x[age < 1] <- ".L"; out$ToothDecay <- x

    # SM_Adequate Insurance
    currins <- v("CURRINS")
    adequacy <- function(src) {
      x <- na_vec(); x[src == "1"] <- "1"; x[src == "2"] <- "2"; x[src %in% c("3", "4")] <- "3"
      x[src == ".M"] <- ".M"; x
    }
    benefits <- adequacy(v("K3Q20")); benefits[currins == ".M"] <- ".M"; benefits[currins == "2"] <- ".L"
    allows   <- adequacy(v("K3Q22")); allows[currins == ".M"] <- ".M";   allows[currins == "2"] <- ".L"
    expense  <- adequacy(v("K3Q21B")); expense[v("HOWMUCH") == "1"] <- "4"
    expense[currins == ".M"] <- ".M"; expense[currins == "2"] <- ".L"
    insadeq <- na_vec()
    insadeq[benefits %in% c("1", "2", ".M") & allows %in% c("1", "2", ".M") &
              expense %in% c("1", "2", "4", ".M")] <- "1"
    insadeq[benefits == "3" | allows == "3" | expense == "3"] <- "2"
    insadeq[benefits == ".M" & allows == ".M" & expense == ".M"] <- ".M"
    insadeq[currins == "2"] <- ".L"
    k <- v("INSGAP"); gapins <- na_vec()
    gapins[k == "1"] <- "1"; gapins[k %in% c("2", "3")] <- "2"; gapins[currins == "2"] <- "2"
    gapins[k == ".M"] <- ".M"
    x <- na_vec()
    x[gapins %in% c("1", ".M") & insadeq %in% c("1", ".M")] <- "1"
    x[currins == "2" | gapins == "2" | insadeq == "2"] <- "2"
    x[gapins == ".M" & insadeq == ".M"] <- ".M"
    out$AdqIns <- x

    # SM_Forgone Health Care
    out$ForgoneHlth <- v("K4Q27")

    # NPM_Food Sufficiency
    k <- v("FOODSIT"); x <- na_vec()
    x[k == "1"] <- "1"; x[k %in% c("2", "3", "4")] <- "2"; x[k == ".M"] <- ".M"; x[age > 11] <- ".L"
    out$Food_0to11 <- x

    # NPM_Housing Instability (2022 forward; variables absent earlier)
    eh <- v("EVERHOMELESS"); mm <- v("MISSMORTGAGE"); pl <- v("PLACESLIVED"); mv <- v("MOVES")
    x <- na_vec()
    x[eh == "1" | mm == "1" | (pl == "2" | mv == "3")] <- "1"
    x[eh %in% c("2", "3", ".M") & mm %in% c("2", "3", ".M") &
        (pl %in% c("1", ".M") | mv %in% c("1", "2", ".M"))] <- "2"
    x[eh %in% c("3", ".M") & mm %in% c("3", ".M") & (pl == ".M" | mv == ".M")] <- ".M"
    x[age > 11] <- ".N"
    out$Housing_0to11 <- x

    # SM_Smoking, Household
    out$HHSmoking <- v("K9Q40")

    # FORM 11: CSHCN (expanded definition)
    diffcnt <- Reduce(`+`, lapply(DIFFICULTY_VARS, function(a) v(a) == "1"))
    condcnt <- Reduce(`+`, lapply(CONDITION_VARS, function(a) v(a) == "1"))
    out$F11CSHCN <- ifelse(v("SC_CSHCN") == "1" | (condcnt >= 1 & diffcnt >= 1), "1", "2")

    # FORM 11: Current ADD/ADHD and Autism, age 3-17
    x <- current(v("K2Q31B")); x[age < 3] <- ".L"; out$F11ADD <- x
    x <- current(v("K2Q35B")); x[age < 3] <- ".L"; out$F11Autism <- x

    # --- Survey items reported directly (not Title V measures) ---------------
    # K8Q21: top two response categories ("Very well", "Somewhat well") vs. the other two
    k <- v("K8Q21"); x <- na_vec()
    x[k %in% c("1", "2")] <- "1"; x[k %in% c("3", "4")] <- "2"; x[k == ".M"] <- ".M"; x[age < 6] <- ".N"
    out$K8Q21 <- x

    as_tibble(out)
  }

  # ---------------------------------------------------------------------------
  # 5. Survey estimation
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips <- all_fips %>%
    filter(nchar(geography) == 2, geography_name %in% c(state.name, "District of Columbia")) %>%
    pull(geography)

  # Title V race_eth: Hispanic, then non-Hispanic single races and 2+ races
  race_label <- c("3" = "AI/AN", "4" = "Asian", "2" = "Black", "5" = "NH/PI",
                  "1" = "White", "7" = "Multiple")

  # Sufficient statistics per stratum x cell x measure, so any domain (a union
  # of cells) can be estimated by summing them.
  year_cells <- function(d, year) {
    codes <- title_v(d, year)
    stopifnot(setequal(names(codes), measure_dict$tv_var))
    age_num <- as.numeric(d$SC_AGE_YEARS)
    base <- tibble(
      geography = sprintf("%02d", as.integer(d$FIPSST)),
      stratum   = paste(d$FIPSST, sub("A$", "", toupper(d$STRATUM))),
      w         = as.numeric(d$FWC),
      age       = case_when(age_num <= 5 ~ "0-5", age_num <= 11 ~ "6-11", TRUE ~ "12-17"),
      sex       = c("1" = "Male", "2" = "Female")[d$SC_SEX],
      race_ethnicity = if_else(d$SC_HISPANIC_R == "1", "Hispanic", unname(race_label[d$SC_RACE_R]))
    )
    stopifnot(all(base$geography %in% state_fips), !anyNA(base$w), !anyNA(base$sex))
    design <- list(
      nh = count(base, stratum, name = "n_h"),
      df = nrow(base) - n_distinct(base$stratum)
    )
    long <- bind_cols(base, codes) %>%
      pivot_longer(all_of(measure_dict$tv_var), names_to = "tv_var", values_to = "code") %>%
      mutate(y = case_when(code == "1" ~ 1, code == "2" ~ 0)) %>%
      filter(!is.na(y))
    cells <- long %>%
      group_by(geography, stratum, age, sex, race_ethnicity, tv_var) %>%
      summarise(n = n(), S0 = sum(w), S1 = sum(w * y), Q0 = sum(w^2), Q1 = sum(w^2 * y),
                .groups = "drop")
    list(cells = cells, design = design)
  }

  # Estimate every domain defined by `keys` (each also rolled up to "Overall")
  # and by geography (each state, plus "00" for the nation).
  estimate <- function(yc, keys) {
    cells <- yc$cells
    rolled <- bind_rows(cells, mutate(cells, geography = "00"))
    for (k in keys) rolled <- bind_rows(rolled, mutate(rolled, !!k := "Overall"))
    rolled %>%
      filter(if_all(all_of(keys), ~ !is.na(.x))) %>%
      group_by(across(all_of(c("geography", keys, "tv_var", "stratum")))) %>%
      summarise(across(c(n, S0, S1, Q0, Q1), sum), .groups = "drop") %>%
      left_join(yc$design$nh, by = "stratum") %>%
      group_by(across(all_of(c("geography", keys, "tv_var")))) %>%
      mutate(W = sum(S0), p = sum(S1) / W) %>%
      summarise(
        n   = sum(n),
        p   = first(p),
        # Sum_h n_h/(n_h-1) * [Sum z^2 - (Sum z)^2 / n_h], z_i = w_i (y_i - p) / W
        var = sum(if_else(n_h > 1, n_h / (n_h - 1) *
                            ((Q1 * (1 - 2 * p) + p^2 * Q0) - (S1 - p * S0)^2 / n_h), 0)) / first(W)^2,
        .groups = "drop"
      ) %>%
      mutate(
        se   = sqrt(pmax(var, 0)),
        tq   = qt(0.975, yc$design$df),
        half = tq * se / (p * (1 - p)),
        # The logit CI is undefined at 0% / 100%: leave it NA (flagged unreliable)
        lcl  = if_else(p > 0 & p < 1, plogis(qlogis(p) - half), NA_real_),
        ucl  = if_else(p > 0 & p < 1, plogis(qlogis(p) + half), NA_real_)
      ) %>%
      select(-var, -tq, -half, -se)
  }

  # Flag, then reshape wide: <col>, _lcl, _ucl, _n, _suppressed, _unreliable,
  # _not_asked for every measure.
  LEVELS <- list(
    age            = c("Overall", "0-5", "6-11", "12-17"),
    sex            = c("Overall", "Male", "Female"),
    race_ethnicity = c("Overall", "Hispanic", unname(race_label))
  )

  finalize <- function(est, keys) {
    # Universe check: a measure is "not asked" for a year x age group when no
    # child in the national sample has a valid response there.
    nat <- est %>% filter(geography == "00")
    for (k in setdiff(keys, "age")) nat <- nat[nat[[k]] == "Overall", ]
    nat <- select(nat, time, age, tv_var, n_nat = n)
    # Complete grid: every geography x year x stratum x measure, so cells with
    # no sampled children appear (as suppressed) instead of being dropped.
    grid <- do.call(tidyr::expand_grid, c(
      list(time = sort(unique(est$time)), geography = c("00", sort(state_fips))),
      LEVELS[keys], list(tv_var = measure_dict$tv_var)))
    out <- grid %>%
      left_join(est, by = c("time", "geography", keys, "tv_var")) %>%
      left_join(nat, by = c("time", "age", "tv_var")) %>%
      mutate(
        n          = coalesce(n, 0L),
        not_asked  = as.integer(coalesce(n_nat, 0L) == 0),
        suppressed = as.integer(not_asked == 0 & n < MIN_N),
        keep       = not_asked == 0 & suppressed == 0,
        value      = if_else(keep, round(100 * p, 2), NA_real_),
        value_lcl  = if_else(keep, round(100 * lcl, 2), NA_real_),
        value_ucl  = if_else(keep, round(100 * ucl, 2), NA_real_),
        unreliable = if_else(keep, as.integer(is.na(value_lcl) | (value_ucl - value_lcl) > CI_MAX_PP |
                                                (value_ucl - value_lcl) > CI_MAX_REL * value), NA_integer_)
      ) %>%
      left_join(select(measure_dict, tv_var, col), by = "tv_var")
    col_order <- as.vector(t(outer(measure_dict$col,
      c("", "_lcl", "_ucl", "_n", "_suppressed", "_unreliable", "_not_asked"), paste0)))
    out %>%
      select(all_of(c("geography", "time", keys)), col, value, value_lcl, value_ucl, n,
             suppressed, unreliable, not_asked) %>%
      tidyr::pivot_wider(names_from = col,
                         values_from = c(value, value_lcl, value_ucl, n, suppressed, unreliable, not_asked),
                         names_glue = "{col}__{.value}") %>%
      # nsch_pct_x__value -> nsch_pct_x; __value_lcl -> _lcl; __n -> _n; ...
      rename_with(~ sub("__", "_", sub("__value", "", .x, fixed = TRUE), fixed = TRUE),
                  -all_of(c("geography", "time", keys))) %>%
      select(all_of(c("geography", "time", keys)), all_of(col_order)) %>%
      arrange(across(all_of(c("geography", "time", keys))))
  }

  SPECS <- list(
    age                = "age",
    age_sex            = c("age", "sex"),
    age_race_ethnicity = c("age", "race_ethnicity")
  )
  est_all <- setNames(vector("list", length(SPECS)), names(SPECS))
  for (y in years) {
    d <- vroom::vroom(extract_path(y), delim = ",", col_types = vroom::cols(.default = "c"),
                      show_col_types = FALSE)
    yc <- year_cells(d, y)
    for (s in names(SPECS)) {
      est_all[[s]] <- bind_rows(est_all[[s]],
                                estimate(yc, SPECS[[s]]) %>% mutate(time = paste0(y, "-12-31")))
    }
    message(sprintf("  estimated %d (%d children)", y, nrow(d)))
  }

  # ---------------------------------------------------------------------------
  # 6. Write standard files
  # ---------------------------------------------------------------------------
  for (s in names(SPECS)) {
    std <- finalize(est_all[[s]], SPECS[[s]])
    vroom::vroom_write(std, sprintf("standard/data_%s.csv.gz", s), delim = ",")
    message(sprintf("Wrote standard/data_%s.csv.gz: %d rows x %d columns", s, nrow(std), ncol(std)))
  }

  # ---------------------------------------------------------------------------
  # 7. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- list(files = as.list(current_files), vars = VARS_SIG)
  dcf::dcf_process_record(updated = process)
}

# ---------------------------------------------------------------------------
# 8. measure_info.json (one entry per output column). Always regenerated, so
#    edits to the descriptions above take effect without new Census data.
# ---------------------------------------------------------------------------
src <- list(list(id = "nsch"))
info <- list()
# Main entry: plain-language sentence, source, definition, years, notes, and
# the 'Overall' universe. Companion columns (CI, n, flags) get short
# descriptions that point back to the main column.
for (i in seq_len(nrow(measure_dict))) {
  m <- measure_dict[i, ]
  main_desc <- paste0(
    m$description, " ",
    "From the National Survey of Children's Health (NSCH); all answers are reported by a parent or caregiver. ",
    "Definition: ", m$coding, " ",
    "Reported for data years ", m$years, ". ",
    if (nzchar(m$notes)) paste0(m$notes, " ") else "",
    "Rows with age = 'Overall' cover the full measure universe (", tolower(m$universe), ")."
  )
  # Filter(length, ...) drops the NULL title_v_variable / nsch_variable field
  entry <- function(id, short, long, sdesc, ldesc, mtype, unit) Filter(length, list(
    id = id, short_name = short, long_name = long, category = m$category,
    short_description = sdesc, long_description = ldesc,
    measure_type = mtype, unit = unit, time_resolution = "Year",
    title_v_variable = if (m$is_title_v) m$tv_var else NULL,
    nsch_variable = if (m$is_title_v) NULL else m$tv_var,
    sources = src
  ))
  long_name <- paste0("NSCH: ", m$short_name)
  what <- paste0("the percent of ", sub("^Percent of ", "", sub("\\.$", "", m$description)))
  info[[m$col]] <- c(entry(m$col, m$short_name, long_name, m$description, main_desc, "Percent", "Percent"),
    list(statement = paste0(sub("^Percent of ", "In {location}, {value} of ", sub("\\.$", "", m$description)), ".")))
  info[[paste0(m$col, "_lcl")]] <- entry(paste0(m$col, "_lcl"), paste0(m$short_name, " - lower 95% CI"),
    paste0(long_name, " - lower bound of 95% confidence interval"),
    paste0("Lower bound of the 95% confidence interval for ", m$col, "."),
    paste0("Lower bound of the 95% confidence interval for ", m$col, ", ", what, ". ",
           "The interval accounts for the NSCH survey design and is computed on the logit scale. ",
           "NA when the estimate is suppressed or not asked, or is exactly 0% or 100% (where a logit interval is undefined)."),
    "Percent", "Percent")
  info[[paste0(m$col, "_ucl")]] <- entry(paste0(m$col, "_ucl"), paste0(m$short_name, " - upper 95% CI"),
    paste0(long_name, " - upper bound of 95% confidence interval"),
    paste0("Upper bound of the 95% confidence interval for ", m$col, "."),
    paste0("Upper bound of the 95% confidence interval for ", m$col, ", ", what, ". ",
           "The interval accounts for the NSCH survey design and is computed on the logit scale. ",
           "NA when the estimate is suppressed or not asked, or is exactly 0% or 100% (where a logit interval is undefined)."),
    "Percent", "Percent")
  info[[paste0(m$col, "_n")]] <- entry(paste0(m$col, "_n"), paste0(m$short_name, " - sample size"),
    paste0(long_name, " - unweighted sample size"),
    paste0("Number of surveyed children behind the estimate for ", m$col, "."),
    paste0("Unweighted number of surveyed children in the denominator of ", m$col,
           " for this geography, year, and demographic group. Estimates based on fewer than 30 children are suppressed (see ",
           m$col, "_suppressed)."),
    "Count", "Count")
  info[[paste0(m$col, "_suppressed")]] <- entry(paste0(m$col, "_suppressed"), paste0(m$short_name, " - suppressed flag"),
    paste0(long_name, " - suppressed flag"),
    paste0("1 if ", m$col, " is suppressed because fewer than 30 children were surveyed; 0 otherwise."),
    paste0("Flag for small samples. 1 means the measure applies to this year and group but fewer than 30 surveyed children are in the denominator, ",
           "so ", m$col, " and its confidence interval are set to NA (not imputed), following the NSCH Comprehensive Analytic Guide data presentation standards; 0 otherwise. ",
           "Distinct from ", m$col, "_not_asked, which marks years and age groups the measure does not cover."),
    "Indicator", "0/1 flag")
  info[[paste0(m$col, "_unreliable")]] <- entry(paste0(m$col, "_unreliable"), paste0(m$short_name, " - unreliable flag"),
    paste0(long_name, " - poor reliability flag"),
    paste0("1 if ", m$col, " is imprecise (95% confidence interval wider than 20 percentage points or 1.2 times the estimate); 0 otherwise."),
    paste0("Flag for imprecise estimates, following the NSCH Comprehensive Analytic Guide data presentation standards. ",
           "1 means the 95% confidence interval of ", m$col, " is wider than 20 percentage points or wider than 1.2 times the estimate; the estimate is still reported but should be interpreted with caution. ",
           "Estimates of exactly 0% or 100% are also flagged 1 because their confidence interval cannot be computed. ",
           "NA when there is no estimate (suppressed or not asked)."),
    "Indicator", "0/1 flag")
  info[[paste0(m$col, "_not_asked")]] <- entry(paste0(m$col, "_not_asked"), paste0(m$short_name, " - not-asked flag"),
    paste0(long_name, " - not asked / not applicable flag"),
    paste0("1 if ", m$col, " does not apply to this year and age group; 0 otherwise."),
    paste0("Flag for years and age groups the measure does not cover. 1 means the age group is outside the measure's universe (",
           tolower(m$universe), ") or the measure is not reported that year (reported for data years ", m$years, "), so ",
           m$col, " is NA; 0 otherwise. Distinct from ", m$col, "_suppressed, which marks small samples."),
    "Indicator", "0/1 flag")
}

info[["_sources"]] <- list(nsch = list(
  name = "National Survey of Children's Health (NSCH)",
  url = "https://www.census.gov/programs-surveys/nsch/data/datasets.html",
  organization = "Health Resources and Services Administration, Maternal and Child Health Bureau (HRSA MCHB); conducted by the U.S. Census Bureau",
  organization_url = "https://mchb.hrsa.gov/data-research/national-survey-childrens-health",
  location = "NSCH public-use topical data files (Stata), 2016 onward, and the Title V measure code (SAS, 2016-2025)",
  location_url = "https://www2.census.gov/programs-surveys/nsch/datasets/",
  description = paste0(
    "The NSCH is an annual, cross-sectional, address-based household survey that produces national and state-level data on the physical and emotional health of children ages 0-17 in the United States. ",
    "A parent or caregiver familiar with the child's health answers by web or mailed paper questionnaire about one randomly selected child in the household. ",
    "It is sponsored by HRSA's Maternal and Child Health Bureau and conducted by the U.S. Census Bureau, and was redesigned in 2016; the Census Bureau advises that estimates from 2016 onward should not be compared with earlier NSCH surveys. ",
    "Survey weights represent the non-institutionalized population of children residing in housing units in each state and DC. ",
    "PopHIVE computes the estimates here from the public-use topical microdata: measures are coded with the Title V measure code that the Census Bureau publishes with the data files (or, for the few measures marked as NSCH survey items, as a single response category of one question), and are weighted single-year percentages with 95% logit confidence intervals that account for the survey design. ",
    "Following the NSCH Comprehensive Analytic Guide, estimates with an unweighted denominator under 30 are suppressed and estimates whose 95% CI is wider than 20 percentage points or 1.2 times the estimate are flagged as unreliable. ",
    "Single-year state estimates for small subgroups (for example, some race/ethnicity groups) are often suppressed or unreliable; the Census Bureau notes that multiple years can be combined for more precision. ",
    "All responses are parent/caregiver reported. Time is the survey (data) year, coded YYYY-12-31; data collection for a survey year can extend into the following calendar year."),
  restrictions = paste0(
    "Public use data. Per the Census Bureau NSCH FAQ: 'Data users are free to produce and publish works based on the NSCH data and documentation; proper attribution to the NSCH source material should accompany these works.' ",
    "Suggested citation: Health Resources and Services Administration, Maternal and Child Health Bureau. [Year] National Survey of Children's Health Public Use Microdata File, STATA. Administered by the U.S. Census Bureau."),
  date_accessed = as.integer(format(Sys.Date(), "%Y"))
))

# Keep the hand-written _catalog block across regeneration
info[["_catalog"]] <- if (file.exists("measure_info.json")) {
  tryCatch(jsonlite::read_json("measure_info.json")[["_catalog"]], error = function(e) NULL)
} else NULL
jsonlite::write_json(info, "measure_info.json", pretty = TRUE, auto_unbox = TRUE, null = "null")
