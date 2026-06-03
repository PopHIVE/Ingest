library(tidyverse)
library(dcf)

# Patched version of dcf::dcf_download_wisqars that fixes the fiveyr1/fiveyr2
# parameters. The dcf package sets fiveyr1=age_min and fiveyr2=age_max, but
# WISQARS 2024 requires fiveyr1=65 and fiveyr2=199 as a fixed "grand total
# display" anchor regardless of the actual age filter (c_age1/c_age2).
# Without this fix, queries for age bands 0-64 return HTTP 500.
dcf_download_wisqars_patched <- function(
    file, fatal_outcome = TRUE, brain_injury_only = FALSE,
    year_start = 2018, year_end = year_start, geography = "00",
    intent = "all", disposition = "all",
    mechanism = if (fatal_outcome) 20810 else 3000,
    group_ages = NULL, age_min = 0, age_max = 199,
    sex = "all", race = "all", race_reporting = "single", ethnicity = "all",
    YPLL = 65, metro = NULL, group_by = NULL, include_total = FALSE,
    verbose = TRUE) {

  intents       <- list(all=0, unintentional=1, violence=8, homicide_legal=4,
                        homicide=3, legal=6, suicide=2, undetermined=5)
  dispositions  <- list(all=0, treated=1, transfered=2, hospitalized=3, observed=4)
  sexes         <- list(all=0, male=1, female=2, unknown=3)
  races         <- list(all=0, white=1, black=2, aa=3, asian=4, pi=5, more=6)
  race_reportings <- list(none=0, bridge=1, single=2, aapi=3)
  ethnicities   <- list(all=0, non_hispanic=1, hispanic=2, unknown=3)

  if (missing(group_ages) && (!missing(age_min) || !missing(age_max))) {
    group_ages <- FALSE
  }

  params <- list(
    TotalLine = if (include_total) "YES" else "NO",
    intent    = if (is.character(intent)) intents[[tolower(intent)]] else 0L,
    mech      = mechanism,
    sex       = paste(vapply(sex,  function(l) if (is.character(l)) sexes[[l]]  else l, 0), collapse=","),
    race      = paste(vapply(race, function(l) if (is.character(l)) sexes[[l]]  else l, 0), collapse=","),
    race_yr   = if (is.character(race_reporting)) race_reportings[[race_reporting]] else race_reporting,
    year1     = year_start,
    year2     = year_end,
    agebuttn  = if (is.null(group_ages)) "ALL" else if (group_ages) "5Yr" else "custom",
    fiveyr1   = 65,    # fixed grand-total display anchor (was age_min in dcf)
    fiveyr2   = 199,   # fixed grand-total display anchor (was age_max in dcf)
    c_age1    = age_min,
    c_age2    = age_max,
    groupby1  = "NONE", groupby2 = "NONE", groupby3 = "NONE",
    groupby4  = "NONE", groupby5 = "NONE", groupby6 = "NONE"
  )

  if (fatal_outcome) {
    params$state   <- geography
    params$ethnicty <- paste(vapply(ethnicity, function(l) if (is.character(l)) ethnicities[[l]] else l, 0), collapse=",")
    params$ypllage <- YPLL
    params$urbrul  <- if (is.null(metro)) 0 else if (metro) 1 else 2
    params$tbi     <- if (brain_injury_only) 1L else 0L
  } else {
    params$groupby1 <- "NONE1"; params$groupby2 <- "NONE2"
    params$groupby3 <- "NONE3"; params$groupby4 <- "NONE4"
    params$groupby5 <- "NONE5"; params$groupby6 <- "NONE6"
    params$outcome  <- "NFI"
    params$racethn  <- 0
    params$disp     <- paste(vapply(disposition, function(l) if (is.character(l)) dispositions[[l]] else l, 0), collapse=",")
  }

  for (group in seq_along(group_by)) {
    params[[paste0("groupby", group)]] <- toupper(group_by[[group]])
  }

  params <- lapply(params, as.character)

  if (fatal_outcome) {
    params$app_id       <- 1002
    params$component_id <- 1000
  }

  if (verbose) {
    url_param_map <- list(year1="y1", year2="y2", tbi="t", disp="d", state="g",
                          ethnicty="e", intent="i", mech="m", sex="s", race="r",
                          agebuttn="a", urbrul="me", race_yr="ry", ypllage="yp",
                          fiveyr1="g1", fiveyr2="g2", c_age1="a1", c_age2="a2",
                          groupby1="r1", groupby2="r2", groupby3="r3",
                          groupby4="r4", groupby5="r5", groupby6="r6")
    url <- paste0("https://wisqars.cdc.gov/reports/?o=", if (fatal_outcome) "MORT" else "NFI")
    for (k in names(params)) {
      url_key <- url_param_map[[k]]
      if (!is.null(url_key))
        for (value in params[[k]]) url <- paste0(url, "&", url_key, "=", value)
    }
    cli::cli_alert_info("requesting report {.url {url}}")
  }

  handler <- curl::new_handle()
  curl::handle_setheaders(handler, `Content-Type` = "application/json")
  curl::handle_setopt(handler, copypostfields = jsonlite::toJSON(list(parameters = params), auto_unbox = TRUE))
  req <- curl::curl_fetch_memory(
    paste0("https://wisqars.cdc.gov/api/cost-", if (fatal_outcome) "fatal" else "nonfatal"),
    handle = handler
  )

  if (req$status_code == 200) {
    dir.create(dirname(file), FALSE, TRUE)
    data <- jsonlite::fromJSON(rawToChar(req$content))
    if (!length(data)) {
      cli::cli_warn("no rows in data, so no file written")
    } else if (grepl("parquet", file)) {
      arrow::write_parquet(data, file, compression = "gzip")
    } else {
      vroom::vroom_write(data, file, ",")
    }
  } else {
    cli::cli_abort("request failed: {req$status_code}")
  }
  invisible(params)
}

#
# Download
#
agegrps <- list(c(0,14),
                c(15,24), 
                c(25,44), 
                c(45,64) ,
                c(65,199)
)

#Use custom age groups
#violence, stratified by age and state

wisqars_downloader <- function(max_year = 2024, min_year = 2001, raw_dir = "raw") {

  dir.create(raw_dir, showWarnings = FALSE, recursive = TRUE)

  safe_download <- function(fname, ...) {
    tryCatch(
      dcf_download_wisqars_patched(fname, ...),
      error = function(e) message("Skipping ", basename(fname), ": ", conditionMessage(e))
    )
  }

  download_intent <- function(intent_name, intent_value) {

    # Pattern 1: state × age
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_state_age_", X[1], "_", X[2], ".csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR"),
        year_start = min_year,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 2: age only (national)
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_age_", X[1], "_", X[2], ".csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "YEAR"),
        year_start = min_year,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 3: state × age × sex
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_state_age_", X[1], "_", X[2], "_sex.csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "SEX"),
        year_start = min_year,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 4: state × age × race (2018+ only)
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_state_age_", X[1], "_", X[2], "_race.csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "RACE"),
        year_start = max(min_year, 2018),
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'single'
      )
    })

    # Pattern 5: state × age × ethnicity
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_state_age_", X[1], "_", X[2], "_ethnicity.csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "ETHNICTY"),
        year_start = min_year,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 6: state × age × sex × ethnicity
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_state_age_", X[1], "_", X[2], "_sex_ethnicity.csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "SEX", "ETHNICTY"),
        year_start = min_year,
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'none'
      )
    })

    # Pattern 7: state × age × sex × race × ethnicity (2018+ only)
    lapply(agegrps, function(X) {
      safe_download(
        file.path(raw_dir, paste0(intent_name, "_state_age_", X[1], "_", X[2], "_sex_race_ethnicity.csv.xz")),
        intent = intent_value,
        group_by = c("MECH", "STATE", "YEAR", "SEX", "RACE", "ETHNICTY"),
        year_start = max(min_year, 2018),
        year_end = max_year,
        age_min = X[1],
        age_max = X[2],
        group_ages = F,
        race_reporting = 'single'
      )
    })

    # Pattern 8: state only (all ages)
    safe_download(
      file.path(raw_dir, paste0(intent_name, "_state.csv.xz")),
      intent = intent_value,
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = min_year,
      year_end = max_year,
      race_reporting = 'none'
    )

    # Pattern 9: national overall (all ages)
    safe_download(
      file.path(raw_dir, paste0(intent_name, ".csv.xz")),
      intent = intent_value,
      group_by = c("MECH", "YEAR"),
      year_start = min_year,
      year_end = max_year,
      race_reporting = 'none'
    )
  }

  download_intent("violence", "violence")
  download_intent("accident", "unintentional")
  download_intent("homicide", "homicide")
  download_intent("suicide", "suicide")
  download_intent("legal", "legal")

  ############################################################
  # Special mechanism-specific downloads (cycling and pedestrian)

  lapply(agegrps, function(X) {
    safe_download(
      file.path(raw_dir, paste0("cycle_accident_state_age_", X[1], "_", X[2], ".csv.xz")),
      mechanism = 20980,
      intent = "unintentional",
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = min_year,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  lapply(agegrps, function(X) {
    safe_download(
      file.path(raw_dir, paste0("cycle_accident_age_", X[1], "_", X[2], ".csv.xz")),
      mechanism = 20980,
      intent = "unintentional",
      group_by = c("MECH", "YEAR"),
      year_start = min_year,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  safe_download(
    file.path(raw_dir, "cycle_accident.csv.xz"),
    mechanism = 20980,
    intent = "unintentional",
    group_by = c("MECH", "YEAR"),
    year_start = min_year,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )

  safe_download(
    file.path(raw_dir, "cycle_accident_state.csv.xz"),
    mechanism = 20980,
    intent = "unintentional",
    group_by = c("MECH", "STATE", "YEAR"),
    year_start = min_year,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )

  ###########################################################
  lapply(agegrps, function(X) {
    safe_download(
      file.path(raw_dir, paste0("ped_accident_state_age_", X[1], "_", X[2], ".csv.xz")),
      mechanism = 21010,
      intent = "unintentional",
      group_by = c("MECH", "STATE", "YEAR"),
      year_start = min_year,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  lapply(agegrps, function(X) {
    safe_download(
      file.path(raw_dir, paste0("ped_accident_age_", X[1], "_", X[2], ".csv.xz")),
      mechanism = 21010,
      intent = "unintentional",
      group_by = c("MECH", "YEAR"),
      year_start = min_year,
      year_end = max_year,
      age_min = X[1],
      age_max = X[2],
      group_ages = F,
      race_reporting = 'none'
    )
  })

  safe_download(
    file.path(raw_dir, "ped_accident.csv.xz"),
    mechanism = 21010,
    intent = "unintentional",
    group_by = c("MECH", "YEAR"),
    year_start = min_year,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )

  safe_download(
    file.path(raw_dir, "ped_accident_state.csv.xz"),
    mechanism = 21010,
    intent = "unintentional",
    group_by = c("MECH", "STATE", "YEAR"),
    year_start = min_year,
    year_end = max_year,
    group_ages = F,
    race_reporting = 'none'
  )
}

# Downloads only new_year from the API and appends it to the existing raw files.
# Much faster than re-scraping all years. Run this when a new year of data is released.
wisqars_append_year <- function(new_year = 2024) {
  tmp_dir <- "raw_tmp"

  wisqars_downloader(max_year = new_year, min_year = new_year, raw_dir = tmp_dir)

  tmp_files <- list.files(tmp_dir, pattern = "\\.csv\\.xz$", full.names = TRUE)
  message("Downloaded ", length(tmp_files), " files for year ", new_year)

  coerce_val_cols <- function(df) {
    dplyr::mutate(df, dplyr::across(where(is.double), as.character))
  }

  for (tmp_file in tmp_files) {
    raw_file <- file.path("raw", basename(tmp_file))
    if (file.exists(raw_file)) {
      existing <- vroom::vroom(raw_file, show_col_types = FALSE)
      new_rows  <- vroom::vroom(tmp_file, show_col_types = FALSE)
      message("  ", basename(raw_file), ": existing rows=", nrow(existing),
              ", new 2024 rows=", nrow(new_rows))
      combined  <- dplyr::bind_rows(
        coerce_val_cols(existing %>% dplyr::filter(as.integer(year) != new_year)),
        coerce_val_cols(new_rows)
      )
      vroom::vroom_write(combined, raw_file, delim = ",")
      message("  Updated: ", raw_file)
    }
    file.remove(tmp_file)
  }
  unlink(tmp_dir, recursive = TRUE)
}



# Add 2024 data without re-scraping everything:
wisqars_append_year(2024)

# Full re-scrape (slow):
#wisqars_downloader(max_year = 2024)
  
#
# Reformat
#

raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv",
  full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  files <- list.files("raw", pattern = "\\.csv\\.xz$", full.names = TRUE)
  
  # read and combine, adding source info
  #test <- vroom::vroom('./raw/ped_accident_state.csv.xz')
  
  data <- files %>%
    set_names() %>%
    map_dfr(~ vroom::vroom(.x, show_col_types = FALSE) %>%
              dplyr::select(-any_of(c("ageadj", "ageadjypll"))) %>%
         mutate(source = basename(.x),
                     deaths = as.character(deaths),
                     ypll = as.character(ypll),
                     CrudeRate = as.character(CrudeRate),
                     CrudeRateypll = as.character(CrudeRateypll)
                    # ageadj = as.character(ageadj),
                    # ageadjypll = as.character(ageadjypll),
                     
            )
            )%>%
      
    mutate(source = str_remove(source, "\\.csv\\.xz$"),
           agegp = if_else(agegp=='<1-Unknown', NA_character_, agegp) ) %>%
    separate_wider_delim(
      source,
      delim = "_",
      names = c("type", "level", "age1", "age2", "age3", "demographic"),
      too_few = "align_start",
      too_many = "merge"
    ) %>%
    {
      # temporary fix to missing columns
      if (!"sex" %in% names(.)) .$sex <- NA
      if (!"race" %in% names(.)) .$race <- NA  
      # WISQARS uses "ethnicty" (typo) as column name
      if ("ethnicty" %in% names(.)) . <- rename(., ethnicity = ethnicty)
      if (!"ethnicity" %in% names(.)) .$ethnicity <- NA
      .
    } %>%
    # combine age columns into one (if present)
    rename(agegrp= agegp) %>%
    mutate(
      
      
      CrudeRate = na_if(gsub("**", "", CrudeRate, fixed = TRUE), "--"),
      deaths    = na_if(gsub("**", "", deaths,    fixed = TRUE), "--"),
      CrudeRate = as.numeric(CrudeRate),
      deaths    = as.numeric(deaths),
      state = replace_na(state, "00"),
      agegrp = replace_na(agegrp, "Total"),
      agegrp = gsub("<1","0", agegrp),
      agegrp = gsub("-Unknown","+", agegrp),
      agegrp = paste0(agegrp, ' Years'),
      agegrp = gsub("Total Years","Total", agegrp),
      sex = case_when(
        demographic == "sex" | grepl("sex", demographic) ~ case_when(
          sex == 1 ~ "Male",
          sex == 2 ~ "Female",
          TRUE ~ "All"
        ),
        TRUE ~ "All"
      ),
      race = case_when(
        demographic == "race" | grepl("race", demographic) ~ case_when(
          race == "01" | race == 1 ~ "White",
          race == "02" | race == 2 ~ "Black",
          race == "03" | race == 3 ~ "American Indian/Alaska Native",
          race == "04" | race == 4 ~ "Asian",
          race == "05" | race == 5 ~ "Native Hawaiian/Pacific Islander",
          race == "06" | race == 6 ~ "More than one race",
          TRUE ~ "All"
        ),
        TRUE ~ "All"
      ),
      ethnicity = case_when(
        demographic == "ethnicity" | grepl("ethnicity", demographic) ~ case_when(
          ethnicity == 1 ~ "Non-Hispanic",
          ethnicity == 2 ~ "Hispanic",
          ethnicity == 3 ~ "Unknown",
          TRUE ~ "All"
        ),
        TRUE ~ "All"
      ),
      
      Mechlbl = str_to_lower(
        str_replace_all(Mechlbl, "[^a-zA-Z0-9]+", "_")
      ),
      Mechlbl = case_when(
        Mechlbl == 'firearm' & type == 'accident'  ~ 'firearm_accident',
        Mechlbl == 'firearm' & type == 'violence'  ~ 'firearm_intentional',
        Mechlbl == 'firearm' & type == 'homicide'  ~ 'firearm_homicide',
        Mechlbl == 'firearm' & type == 'suicide'   ~ 'firearm_suicide',
        Mechlbl == 'firearm' & type == 'legal'     ~ 'firearm_legal_intervention',
        TRUE ~ Mechlbl
      ),
      time=paste0(year, '-01-01')
          ) %>%
    rename(geography = state,
           rate = CrudeRate,
           age= agegrp
           ) %>%
    filter(grepl('firearm',Mechlbl) | type=='accident'|type=='cycle'|type=='ped') %>%
    dplyr::group_by(type,Mechlbl) |>
    dplyr::filter(sum(!is.na(rate)) > 100, age != "Unknown", Mechlbl!='.', Mechlbl!='_') |>
    ungroup()|>
    tidyr::pivot_wider(
      id_cols = c("geography", "time", "age",  "sex", "race", "ethnicity"),
      #names_prefix = "wisqars_",
      names_from = c("Mechlbl"),
      values_from = c("rate", "deaths")
    )
  data <- data %>%
    rename_with(~ paste0("wisqars_", .x),
                .cols = which(grepl("^(rate_|deaths_)", names(data))))
  
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
