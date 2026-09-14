library(tidyverse)
library(survey)
library(arrow)


diabetes_format <- function(){
  b <-
    # Read in parquet of raw survey results (with weights)
    arrow::open_dataset('./raw/survey_responses.parquet', format = 'parquet') %>%
    # Rename/reformat variables
    rename(state = '_STATE',
           LLCPWT = '_LLCPWT',
           agec = '_AGE_G',
           bmi_cat = '_BMI5CAT',
           sex_code = '_SEX',
           race_code = '_RACEGR3') %>%
    mutate(
      agec = as.numeric(agec),
      DIABETE4 = as.numeric(DIABETE4),
      DIABETE3 = as.numeric(DIABETE3),
      LLCPWT = as.numeric(LLCPWT),
      bmi_cat = as.numeric(bmi_cat),
      IYEAR = as.numeric(IYEAR),
      sex_code = as.numeric(sex_code),
      SEX1 = as.numeric(SEX1),
      race_code = as.numeric(race_code),
      `_RACEGR4` = as.numeric(`_RACEGR4`),
      HLTHPLN1 = as.numeric(HLTHPLN1),
      `_HLTHPLN` = as.numeric(`_HLTHPLN`),
      `_HLTHPL1` = as.numeric(`_HLTHPL1`),
      `_HLTHPL2` = as.numeric(`_HLTHPL2`),
      age = if_else(agec == 1, '18-24 Years',
            if_else(agec == 2, '25-34 Years',
            if_else(agec == 3, '35-44 Years',
            if_else(agec == 4, '45-54 Years',
            if_else(agec == 5, '55-64 Years',
            if_else(agec == 6, '65+ Years', NA_character_)))))),
      DIABETE4 = if_else(IYEAR <= 2018, DIABETE3, DIABETE4),
      diab_yes = if_else(DIABETE4 == 1 , 1,
                         if_else(DIABETE4 %in% c(2, 3, 4, 7, 9), 0,
                                 NA_real_)),
      obese_yes = if_else(bmi_cat == 4, 1, 0),
      # _SEX wasn't fielded until 2019; 2018 only has the raw SEX1 question
      sex_code = coalesce(sex_code, SEX1),
      sex = if_else(sex_code == 1, 'Male',
                    if_else(sex_code == 2, 'Female', NA_character_)),
      # 2022 renamed the primary race variable to _RACEGR4; per the 2022
      # BRFSS codebook it uses the identical 1-5/9 coding as _RACEGR3 in every
      # other year/ _RACEGR3 is also populated for a small, non-random leftover subset of 2022
      # respondents (~5%). Both are incorporated but_RACEGR4 takes priority when both are present.
      race_code = coalesce(`_RACEGR4`, race_code),
      race_ethnicity = if_else(race_code == 1, 'White',
                        if_else(race_code == 2, 'Black',
                        if_else(race_code == 3, 'Other',
                        if_else(race_code == 4, 'Multiracial',
                        if_else(race_code == 5, 'Hispanic', NA_character_))))),
      # the "has health coverage" calculated variable was renamed every
      # cycle: HLTHPLN1 (raw, 2018-2020) -> _HLTHPLN (2021-2022) ->
      # _HLTHPL1 (2023) -> _HLTHPL2 (2024), all coded 1 = has coverage, 2 = does not
      hlthpln_code = coalesce(`_HLTHPL2`, `_HLTHPL1`, `_HLTHPLN`, HLTHPLN1),
      insured_yes = if_else(hlthpln_code == 1, 1,
                            if_else(hlthpln_code == 2, 0, NA_real_)),
      time = as.Date(paste0(IYEAR, '-01-01'))
    ) %>%
    rename(STSTR = `_STSTR`,
           PSU = `_PSU`) %>%
    dplyr::select(diab_yes, obese_yes, insured_yes, state, LLCPWT, STSTR, PSU,
                  age, agec, sex, race_ethnicity, time) %>%
    collect()
  
  
  #
  # Prevalence across every subset of {age, sex, race_ethnicity, state}, from
  # the national overall down to the fully-crossed cells. 
  #
  # State-level cells crossed on all three demographic dimensions get thin
  # fast (well under n=30 in smaller states/ race categories) --
  # sample_size_* columns are carried through so a suppression threshold can
  # be applied downstream

  demo_dims <- c('age', 'sex', 'race_ethnicity', 'state')
  demo_defaults <- list(age = 'Total', sex = 'Overall', race_ethnicity = 'Overall', state = '00')

  demo_combos <- c(
    list(character(0)),
    unlist(lapply(seq_along(demo_dims), function(k) combn(demo_dims, k, simplify = FALSE)),
           recursive = FALSE)
  )

  fill_demo_defaults <- function(df, present_dims) {
    for (d in setdiff(demo_dims, present_dims)) df[[d]] <- demo_defaults[[d]]
    df
  }

# Unweighted respondent count across age, sex, race, states, and time
  sample_size_combined <- lapply(demo_combos, function(vars) {
    out <- b %>%
      group_by(across(all_of(c(vars, 'time')))) %>%
      summarize(sample_size_diab = sum(diab_yes, na.rm = TRUE),
                sample_size_obesity = sum(obese_yes, na.rm = TRUE),
                sample_size_insured = sum(insured_yes, na.rm = TRUE),
                .groups = 'drop')
    fill_demo_defaults(out, vars)
  }) %>%
    bind_rows()

  # Parallelizing for speed. For one year, slices b down to specified row and builds a 
  #   svydesign with weight LLCPWT (survey package requires these svydesigns to be prespecified)
  year_svy <- function(year, b, demo_combos, demo_dims, demo_defaults) {
    
    # same as fcn in line 90
    fill_demo_defaults <- function(df, present_dims) {
      # setdiff finds which of the 4 dimensions are NOT in this combo's grouping variables, then for eaach
      # adds a column to df filled with that dimension's default
      for (d in setdiff(demo_dims, present_dims)) df[[d]] <- demo_defaults[[d]]
      df
    }

    # taking only year we want, create svydesign object  
    yr_data <- b[b$time == year, ]
    design_year <- svydesign(
      id = ~1, # no clustering
      weights = ~LLCPWT, #column with per respondent sampling weight
      data = yr_data
    )

    combo_results <- lapply(demo_combos, function(vars) {
      # skip combos where a grouping dimension is entirely NA this year. svyby errors on an all-NA 
      # by-variable 
      if (length(vars) > 0 && any(vapply(vars, function(v) all(is.na(yr_data[[v]])), logical(1)))) {
        return(NULL)
      }
      # national/overall with no other groupings
      out <- if (length(vars) == 0) {
        # svymean computes weighted mean of all outcomes at once over entire design (no grouping)
        # note: not a sum, this is survey package syntax
        est <- svymean(~diab_yes + obese_yes + insured_yes, design_year, na.rm = TRUE)
        # get CIs
        ci <- confint(est)
        # make into tibble
        tibble(
          diab_yes = coef(est)['diab_yes'],
          ci_l.diab_yes = ci['diab_yes', 1], ci_u.diab_yes = ci['diab_yes', 2],
          obese_yes = coef(est)['obese_yes'],
          ci_l.obese_yes = ci['obese_yes', 1], ci_u.obese_yes = ci['obese_yes', 2],
          insured_yes = coef(est)['insured_yes'],
          ci_l.insured_yes = ci['insured_yes', 1], ci_u.insured_yes = ci['insured_yes', 2]
        )
      } else { # if at least one demographic grouping
        svyby(
          ~diab_yes + obese_yes + insured_yes,
          as.formula(paste('~', paste(vars, collapse = '+'))), # transforms our grouping variables into survey syntax
          design = design_year,
          svymean,
          vartype = 'ci',
          na.rm = TRUE
        )
      }
      fill_demo_defaults(out, vars)
    })

# assemble all data from that year into tall dataframe
    bind_rows(combo_results) %>%
      mutate(
        prev_diabetes_survey = diab_yes * 100,
        prev_diabetes_survey_lcl = ci_l.diab_yes * 100,
        prev_diabetes_survey_ucl = ci_u.diab_yes * 100,

        prev_obesity_survey = obese_yes * 100,
        prev_obesity_survey_lcl = ci_l.obese_yes * 100,
        prev_obesity_survey_ucl = ci_u.obese_yes * 100,

        prev_insured_survey = insured_yes * 100,
        prev_insured_survey_lcl = ci_l.insured_yes * 100,
        prev_insured_survey_ucl = ci_u.insured_yes * 100,
        time = year
      ) %>%
      filter(!is.na(age), !is.na(sex), !is.na(race_ethnicity)) %>%
      rename(geography = state) %>%
      dplyr::select(geography, time, age, sex, race_ethnicity,
                    starts_with('prev_diabetes_'), starts_with('prev_obesity'), starts_with('prev_insured'))
  }

  # Each survey year is fully independent, so they're computed on separate
  # worker processes instead of one at a time. min(physical cores, n years)
  # avoids oversubscribing physical cores, since each worker's largest combo
  # (age x sex x race_ethnicity x state) is itself CPU-intensive.
  years <- unique(b$time)
  n_workers <- min(parallel::detectCores(logical = FALSE), length(years))
  cl <- parallel::makeCluster(n_workers)
  on.exit(parallel::stopCluster(cl), add = TRUE)
  parallel::clusterEvalQ(cl, { library(survey); library(dplyr); library(tibble) })

  prevalence_combined <- parallel::parLapply(
    cl, years, year_svy,
    b = b, demo_combos = demo_combos, demo_dims = demo_dims, demo_defaults = demo_defaults
  ) %>%
    bind_rows() %>%
    left_join(sample_size_combined, by = c('age', 'sex', 'race_ethnicity', 'geography' = 'state', 'time'))

  vroom::vroom_write(prevalence_combined, './standard/data_survey.csv.gz', delim = ',')
  write_csv(prevalence_combined, './standard/data_survey.csv')
}

# Validation
# prevalence_combined <-  vroom::vroom( './standard/data_survey.csv.gz' ) %>%
#   # data_survey.csv.gz now also carries sex/race_ethnicity breakdowns; keep
#   # only the Overall/Overall rows so this comparison stays 1:1 by age x
#   # geography x time, as it was before that breakdown was added
#   filter(sex == 'Overall', race_ethnicity == 'Overall')

# #check against data from web
# v1 <- vroom::vroom('./standard/data.csv.gz') %>%
#   dplyr::select(time, age, geography, pct_diabetes_value, pct_diabetes_sample_size) %>%
#   rename(pct_diabetes_precalc = pct_diabetes_value) %>%
#   left_join(prevalence_combined, by=c('geography', 'time', 'age'))


# ##Correlation = 0.9966
# ggplot(v1) +
#   geom_point(aes(x=prev_diabetes_survey, y=pct_diabetes_precalc, color=as.factor(time)))+
#   geom_abline(aes(intercept=0, slope=1))

# v1 %>%
#   filter(age!='Total') %>%
# ggplot() +
#   geom_point(aes(x=sample_size_diab, y=pct_diabetes_sample_size, color=as.factor(time)))+
#   geom_abline(aes(intercept=0, slope=1))

