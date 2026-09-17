# =============================================================================
# Adolescent school immunization coverage
# Source: https://github.com/PopHIVE/school_immunizations
#
# Each state's school-entry immunization assessment is ingested upstream into
# a wide standard file with the state's own grade labels and column set. This
# pulls the states that publish adolescent-grade Tdap, MenACWY, or HPV
# coverage (6th or 7th grade, or Alaska's registry adolescent series), keeps
# their county and state rows, and maps each state's columns onto a common
# set. Rates are proportions upstream and are written here as percent.
#
# Not included: states with kindergarten or all-grade data only, states whose
# adolescent rows carry exemptions but no coverage, and states that publish
# adolescent rows by school only (see README).
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

BASE_URL <- "https://raw.githubusercontent.com/PopHIVE/school_immunizations/main/data/%s/standard/data.csv.gz"

GRADE_MAP <- c(
  "Adolescent" = "Adolescent",
  "7th" = "7th", "7th grade" = "7th", "7th Grade" = "7th", "7" = "7th", "Grade 7" = "7th",
  "6th grade" = "6th", "6th Grade" = "6th", "Grade 6" = "6th"
)

# Per state: grade labels to keep and upstream column -> common measure.
# Colorado is left out (its only school-age label is K-12 combined) and
# Pennsylvania's 7th grade file has no Tdap or MenACWY column.
SPEC <- list(
  AK = list(grades = "Adolescent",
            cols = c(rate_tdap = "tdap", rate_hpv = "hpv", rate_menacwy = "menacwy",
                     rate_complete = "complete")),
  CT = list(grades = "7th",
            cols = c(rate_tdap = "tdap", rate_menacwy = "menacwy",
                     rate_all_required = "complete",
                     rate_all_required_medical_exempt = "medical_exempt",
                     rate_all_required_religious_exempt = "religious_exempt",
                     rate_all_required_full_exempt = "full_exempt")),
  IN = list(grades = c("Grade 6", "Grade 7"),
            cols = c(rate_tdap = "tdap", rate_mcv4 = "menacwy")),
  LA = list(grades = "6th Grade",
            cols = c(rate_tdap = "tdap", rate_mcv4 = "menacwy", rate_complete = "complete",
                     rate_full_exempt = "full_exempt")),
  MA = list(grades = "7th grade",
            cols = c(rate_tdap = "tdap", rate_menacwy = "menacwy",
                     rate_medical_exempt = "medical_exempt",
                     rate_religious_exempt = "religious_exempt",
                     rate_full_exempt = "full_exempt")),
  ND = list(grades = "7",
            cols = c(rate_tdap = "tdap", rate_menacwy = "menacwy",
                     rate_medical_exempt = "medical_exempt",
                     rate_religious_exempt = "religious_exempt",
                     rate_personal_exempt = "personal_exempt")),
  TX = list(grades = "7th grade",
            cols = c(rate_tdap = "tdap", rate_menacwy = "menacwy",
                     rate_conscientious_exemption = "personal_exempt")),
  WA = list(grades = c("6th grade", "7th grade"),
            cols = c(rate_complete = "complete",
                     rate_medical_exempt = "medical_exempt",
                     rate_religious_exempt = "religious_exempt",
                     rate_personal_exempt = "personal_exempt",
                     rate_full_exempt = "full_exempt"))
)

MEASURES <- c("tdap", "menacwy", "hpv", "complete", "medical_exempt",
              "religious_exempt", "personal_exempt", "full_exempt")

# -----------------------------------------------------------------------------
# Download. A failed download keeps the committed copy, which is still hashed
# so the state is neither dropped nor reprocessed.
# -----------------------------------------------------------------------------
current_hashes <- list()
for (st in names(SPEC)) {
  dest <- file.path("raw", paste0(st, ".csv.gz"))
  tmp <- tempfile(fileext = ".csv.gz")
  ok <- tryCatch({
    download.file(sprintf(BASE_URL, st), tmp, mode = "wb", quiet = TRUE)
    TRUE
  }, error = function(e) {
    message("download failed for ", st, ": ", conditionMessage(e))
    FALSE
  })
  if (ok) file.rename(tmp, dest)
  if (file.exists(dest)) current_hashes[[st]] <- unname(tools::md5sum(dest))
}

if (!identical(process$raw_state, current_hashes)) {

  harmonize <- function(st) {
    sp <- SPEC[[st]]
    d <- vroom::vroom(file.path("raw", paste0(st, ".csv.gz")),
                      col_types = vroom::cols(.default = "c"), show_col_types = FALSE)

    keep <- intersect(names(sp$cols), names(d))
    absent <- setdiff(names(sp$cols), names(d))
    if (length(absent) > 0) {
      warning(st, ": upstream columns not found: ", paste(absent, collapse = ", "))
    }
    # Censoring companions (only Texas has them): "suppressed" or "missing"
    flags <- intersect(paste0("flag_", sub("^rate_", "", keep)), names(d))

    d <- d %>% filter(grade %in% sp$grades, !is.na(geography))
    if ("type" %in% names(d)) {
      d <- d %>% filter(is.na(type) | type %in% c("state", "county"))
    }
    if ("school_name" %in% names(d)) {
      d <- d %>% filter(is.na(school_name))
    }
    if (!"N_enrolled" %in% names(d)) d$N_enrolled <- NA_character_

    d <- d %>%
      mutate(
        state = st,
        grade = unname(GRADE_MAP[grade]),
        N_enrolled = suppressWarnings(as.numeric(N_enrolled)),
        suppressed_flag = if (length(flags) > 0) {
          as.integer(rowSums(across(all_of(flags), ~ .x %in% "suppressed")) > 0)
        } else {
          0L
        },
        across(all_of(keep), ~ suppressWarnings(as.numeric(.x)))
      )

    # Upstream rates are proportions; leave a column alone if it already
    # reads as percent.
    for (col in keep) {
      x <- d[[col]]
      if (all(is.na(x)) || max(x, na.rm = TRUE) <= 1.5) x <- x * 100
      d[[col]] <- round(x, 2)
    }

    d %>%
      rename(!!!setNames(keep, paste0("school_adol_pct_", sp$cols[keep]))) %>%
      select(geography, time, state, grade, N_enrolled,
             starts_with("school_adol_pct_"), suppressed_flag)
  }

  out <- bind_rows(lapply(names(SPEC), harmonize))
  for (m in MEASURES) {
    nm <- paste0("school_adol_pct_", m)
    if (!nm %in% names(out)) out[[nm]] <- NA_real_
  }

  out <- out %>%
    mutate(
      time = format(as.Date(time), "%Y-%m-%d"),
      geography = if_else(nchar(geography) %in% c(1, 4), paste0("0", geography), geography)
    ) %>%
    filter(suppressed_flag == 1 | if_any(starts_with("school_adol_pct_"), ~ !is.na(.x))) %>%
    select(geography, time, state, grade, N_enrolled,
           all_of(paste0("school_adol_pct_", MEASURES)), suppressed_flag) %>%
    arrange(state, geography, time, grade)

  n_dup <- sum(duplicated(out[c("geography", "time", "grade")]))
  if (n_dup > 0) stop(n_dup, " duplicate geography-time-grade rows")

  vroom::vroom_write(out, "standard/data.csv.gz", ",")

  process$raw_state <- current_hashes
  dcf::dcf_process_record(updated = process)
}
