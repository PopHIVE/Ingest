library(httr)
library(jsonlite)
library(dplyr)
library(tidyverse)

get_medicaid_data_complete <- function(dataset_id, limit = 1000) {

  # First, get metadata
  metadata_url <- paste0("https://data.medicaid.gov/api/1/metastore/schemas/dataset/items/", dataset_id)
  metadata_response <- GET(metadata_url)

  if (status_code(metadata_response) == 200) {
    metadata <- fromJSON(content(metadata_response, "text", encoding = "UTF-8"))
    cat("Dataset:", metadata$title, "\n")
  }

  # Try to get data with pagination
  all_data <- list()
  offset <- 0

  repeat {
    # Try the datastore endpoint with pagination
    data_url <- paste0("https://data.medicaid.gov/api/1/datastore/query/",
                       dataset_id, "/0?offset=", offset, "&limit=", limit)

    response <- GET(data_url)

    if (status_code(response) != 200) {
      cat("Request failed at offset", offset, "with status:", status_code(response), "\n")
      break
    }

    data_raw <- content(response, "text", encoding = "UTF-8")
    data_list <- fromJSON(data_raw)

    # Extract results
    if ("results" %in% names(data_list) && length(data_list$results) > 0) {
      current_batch <- data_list$results
      all_data[[length(all_data) + 1]] <- current_batch

      cat("Downloaded batch with", nrow(current_batch), "rows (total offset:", offset, ")\n")

      # Check if we got less than the limit (indicating last page)
      if (nrow(current_batch) < limit) {
        break
      }

      offset <- offset + limit
    } else {
      cat("No more results found\n")
      break
    }

    # Add a small delay to be respectful to the API
    Sys.sleep(0.1)
  }

  # Combine all data
  if (length(all_data) > 0) {
    final_data <- do.call(rbind, all_data)
    cat("Final dataset:", nrow(final_data), "rows,", ncol(final_data), "columns\n")
    return(final_data)
  } else {
    cat("No data retrieved\n")
    return(NULL)
  }
}

# Query the Medicaid catalog to discover available Child/Adult Core Set datasets.
# Returns a named character vector: names are years (e.g. "2023"), values are dataset IDs.
# Sorted by year for deterministic hashing. Falls back to the known list if unreachable.
discover_core_set_datasets <- function() {
  known <- c(
    "2025" = "14bc26c3-5584-4032-9175-f3a0399cb206",
    "2024" = "a5023394-ab10-465b-bb4a-7de5ac98d90c",
    "2023" = "e85033c7-367e-467e-9e81-8e85048102b8",
    "2022" = "dfd13757-d763-4f7a-9641-3f06ce21b4c6",
    "2021" = "a058ef78-e18b-4435-94aa-b70ab6ce5904",
    "2020" = "fbbe1734-b448-4e5a-bc94-3f8688534741",
    "2019" = "e36d89c0-f62e-56d5-bc7e-b0adf89262b8",
    "2018" = "229d6279-e614-5353-9226-f6a6f37d06c3",
    "2017" = "c1028fdf-2e43-5d5e-990b-51ed03428625",
    "2016" = "fc3c7c14-4b08-59c2-97db-0726e478dfdf",
    "2015" = "45a28339-17a5-55e6-8e74-e9004fc703d8",
    "2014" = "2b6a0ec0-efe6-5aec-9fe4-e168b8b6f553"
  )

  catalog_url <- "https://data.medicaid.gov/api/1/metastore/schemas/dataset/items"
  response <- tryCatch(GET(catalog_url, timeout(30)), error = function(e) NULL)

  if (is.null(response) || status_code(response) != 200) {
    cat("Could not reach Medicaid catalog; using known dataset IDs\n")
    return(known[order(names(known))])
  }

  catalog <- tryCatch(
    fromJSON(content(response, "text", encoding = "UTF-8"), simplifyDataFrame = TRUE),
    error = function(e) NULL
  )

  if (is.null(catalog) || !is.data.frame(catalog) ||
      !all(c("title", "identifier") %in% names(catalog))) {
    cat("Unexpected catalog format; using known dataset IDs\n")
    return(known[order(names(known))])
  }

  # Match on the portal's actual title text, not "core set" -- these ARE the
  # Core Set reports, but their titles never contain that phrase (matching it
  # is the bug that silently stalled discovery at 2023; don't reintroduce it).
  mask <- grepl("Child and Adult Health Care Quality Measures", catalog$title, ignore.case = TRUE)
  discovered <- catalog[mask, c("identifier", "title")]

  if (nrow(discovered) == 0) {
    cat("No core set datasets found in catalog; using known dataset IDs\n")
    return(known[order(names(known))])
  }

  # Extract 4-digit year from each title (e.g. "FFY 2024" -> "2024")
  discovered$year <- regmatches(
    discovered$title,
    regexpr("\\b20\\d{2}\\b", discovered$title)
  )
  discovered <- discovered[nchar(discovered$year) == 4, ]

  # Build named vector from catalog, then merge with known IDs
  catalog_ids <- setNames(discovered$identifier, discovered$year)
  all_ids <- c(catalog_ids, known[!names(known) %in% names(catalog_ids)])

  new_years <- setdiff(names(catalog_ids), names(known))
  if (length(new_years) > 0) {
    cat("New year(s) found in catalog:", paste(sort(new_years), collapse = ", "), "\n")
  }
  cat("Total core set datasets:", length(all_ids), "\n")

  return(all_ids[order(names(all_ids))])
}

#initialize dcf process
process <- dcf::dcf_process_record()

# Discover available datasets — triggers reprocessing only when new years appear
data_ids <- discover_core_set_datasets()

# raw_state is based solely on the set of available dataset IDs (no timestamp),
# so reprocessing only occurs when new year datasets are published on the portal.
raw_state <- digest::digest(data_ids)

#processing if raw data has changed
if (!identical(process$raw_state, raw_state)) {
  
  df_ls <- lapply(data_ids, get_medicaid_data_complete)
  names(df_ls) <- paste0('year', names(data_ids))
  
  #save the raw files
 lapply(names(df_ls), function(X) {
    vroom::vroom_write(df_ls[[X]], paste0('raw/', X, '.csv.xz'))
  })
  
  #creating standard format
  #renaming columns for consistency across years and ease
  data1 <- bind_rows(df_ls, .id = "year") %>%
    mutate(year = gsub("year", "", year)) %>%
    { if (!"measure_description" %in% names(.)) add_column(., measure_description = NA_character_) else . } %>%
    { if (!"rate_definition" %in% names(.)) add_column(., rate_definition = NA_character_) else . } %>%
    { if (!"bottom_quartile" %in% names(.)) add_column(., bottom_quartile = NA_character_) else . } %>%
    { if (!"top_quartile" %in% names(.)) add_column(., top_quartile = NA_character_) else . } %>%
    { if (!"25th_percentile" %in% names(.)) add_column(., `25th_percentile` = NA_character_) else . } %>%
    { if (!"75th_percentile" %in% names(.)) add_column(., `75th_percentile` = NA_character_) else . } %>%
    { if (!"statespecific_comments" %in% names(.)) add_column(., statespecific_comments = NA_character_) else . } %>%
    { if (!"state_specific_comments" %in% names(.)) add_column(., state_specific_comments = NA_character_) else . } %>%
    { if (!"core_set_year" %in% names(.)) add_column(., core_set_year = NA_character_) else . } %>%
    { if (!"ffy" %in% names(.)) add_column(., ffy = NA_character_) else . } %>%
    # "Higher/Lower rates are better for this measure"; CMS reports it from FFY 2019
    { if (!"measure_type" %in% names(.)) add_column(., measure_type = NA_character_) else . } %>%
    rename(
      measure_abbr = measure_abbreviation,
      value        = state_rate,
      n_states     = number_of_states_reporting,
      pct25        = bottom_quartile,
      pct75        = top_quartile
    ) %>%
    mutate(
      time_year = if_else(is.na(core_set_year) | core_set_year == "",
                          if_else(is.na(ffy) | ffy == "", year, ffy),
                          core_set_year),
      state_comments = if_else(is.na(statespecific_comments),
                               state_specific_comments,
                               statespecific_comments),
      pct25        = if_else(is.na(pct25), `25th_percentile`, pct25),
      pct75        = if_else(is.na(pct75), `75th_percentile`, pct75),
      measure_info = if_else(is.na(measure_description), rate_definition, measure_description)
    ) %>%
    select(
      time_year, state, domain, reporting_program, measure_name,
      measure_abbr, measure_info, population, methodology, measure_type,
      value, n_states, median, pct25, pct75, notes, source, state_comments
    )
  
  # Some measures report more than one rate definition under the same
  # abbreviation and sub_metric (e.g. different age bands or survey questions
  # CMS doesn't expose as separate columns). distinct() below keeps one row
  # per (geography, time, payer, domain, var_name) -- arrange_for_distinct()
  # makes that pick deterministic (a real value over a suppressed/missing one,
  # then the least demographically-qualified population, then alphabetical)
  # instead of depending on the CMS API's pagination order, which is not
  # guaranteed stable across pulls. This does not recover the collapsed rows;
  # it only makes which one survives reproducible. List the affected
  # variable-years in the log instead of dropping them silently.
  warn_collapsed <- function(d) {
    collapsed <- d %>%
      filter(!is.na(val)) %>%
      group_by(geography, time, payer, domain, var_name) %>%
      summarize(n_def = n_distinct(measure_info), .groups = "drop") %>%
      filter(n_def > 1) %>%
      mutate(measure = sub("_(rate|pct_25|pct_75)$", "", var_name),
             year = substr(time, 1, 4)) %>%
      distinct(measure, year) %>%
      group_by(measure) %>%
      summarize(years = paste(sort(year), collapse = " "), .groups = "drop")
    if (nrow(collapsed) > 0) {
      warning("medicaid_quality: ", nrow(collapsed),
              " measures have more than one rate definition in some years; ",
              "keeping the first row:\n",
              paste0("  ", collapsed$measure, ": ", collapsed$years, collapse = "\n"))
    }
    d
  }

  # ---------------------------------------------------------------------------
  # Population hierarchy: match each row to its Core Set's target population
  # ---------------------------------------------------------------------------
  # Rather than ranking population strings by length, each Core Set has a
  # target population and the row closest to it wins:
  #   * Child Core Set (-CH): all Medicaid AND CHIP children
  #       = Title XIX + Title XXI Medicaid-expansion CHIP + separate CHIP
  #         (pre-2024 CMS wrote this simply as "Medicaid & CHIP").
  #   * Adult Core Set (-AD): non-dual Medicaid (Title XIX).
  # Dual eligibles are avoided: Medicare is their primary payer, so much of
  # their care never appears in Medicaid claims. A row that adds duals is only
  # used for an Adult measure when no non-dual Medicaid row exists, and it is
  # flagged. Where a state reports only a narrower population (e.g. Medicaid
  # without CHIP for a Child measure) the row is kept and flagged rather than
  # dropped -- it may be a state whose CHIP program is a Medicaid expansion or
  # that has no separate CHIP, in which case it IS the whole target.
  # A Child measure reported only for CHIP is used as a last resort and
  # flagged "chip_only". Adult rows with no Medicaid population (separate CHIP,
  # duals-only, "Other" only) and "Other"-only Child rows are never used.
  #
  # The CMS population string is parsed into components. Pre-2024 strings
  # ("Medicaid only", "Medicaid & CHIP", "Medicaid & Dual Eligibles", ...) use
  # a generic "CHIP" meaning both CHIP programs; 2024+ strings list
  # "Medicaid (Title XIX)", "Title XXI-funded Medicaid Expansion CHIP",
  # "Separate CHIP (Title XXI)", "Individuals Dually Eligible for Medicare and
  # Medicaid / Separate CHIP" and "Other" separately.
  #
  # Adds:
  #   pop_tier         primary distance from the target (lower = closer;
  #                    NA = cannot represent the target)
  #                      child: number of target components missing (0-2);
  #                             3 = CHIP only (no Medicaid)
  #                      adult: 1 if duals are included, else 0
  #   pop_extra        secondary distance: components outside the target
  #                      child: "Other" / duals present
  #                      adult: count of CHIP / M-CHIP / Other components
  #   population_match flag for the chosen row:
  #                      "target"             matches the target population
  #                      "broader"            target plus a small non-dual
  #                                           addition (e.g. + Other, + CHIP)
  #                      "narrower"           (child) part of the target only,
  #                                           e.g. Medicaid without CHIP
  #                      "chip_only"          (child) CHIP enrollees only;
  #                                           no Medicaid population reported
  #                      "broader_incl_duals" (adult) Medicaid incl. duals
  add_population_match <- function(d) {
    p <- coalesce(d$population, "")
    dual <- grepl("Dual", p, ignore.case = TRUE)
    p <- gsub("Individuals Dually Eligible for Medicare and (Separate CHIP|Medicaid)|Dual Eligibles",
              "", p, ignore.case = TRUE)
    mchip <- grepl("Medicaid Expansion CHIP", p, ignore.case = TRUE)
    p <- gsub("Title XXI-funded Medicaid Expansion CHIP", "", p, ignore.case = TRUE)
    schip <- grepl("Separate CHIP", p, ignore.case = TRUE)
    p <- gsub("Separate CHIP( \\(Title XXI\\))?", "", p, ignore.case = TRUE)
    chip_all <- grepl("CHIP", p)                     # pre-2024 generic "CHIP"
    mcd   <- grepl("Medicaid", p, ignore.case = TRUE)
    other <- grepl("\\bOther\\b", p, ignore.case = TRUE)

    core_set <- case_when(grepl("-CH$", d$measure_abbr) ~ "child",
                          grepl("-AD$", d$measure_abbr) ~ "adult")
    child_missing <- (!mcd) + (!(mchip | chip_all)) + (!(schip | chip_all))

    d %>%
      mutate(
        pop_tier = case_when(
          # Child measures reported only for CHIP: last resort, after
          # Medicaid-only (child_missing <= 2 for any row containing Medicaid)
          core_set == "child" & !mcd & (chip_all | mchip | schip) ~ 3L,
          !mcd                ~ NA_integer_,
          core_set == "child" ~ as.integer(child_missing),
          core_set == "adult" ~ as.integer(dual)
        ),
        pop_extra = case_when(
          is.na(pop_tier)     ~ NA_integer_,
          core_set == "child" ~ as.integer(other + dual),
          core_set == "adult" ~ as.integer(mchip + schip + chip_all + other)
        ),
        population_match = case_when(
          is.na(pop_tier)                        ~ NA_character_,
          core_set == "child" & !mcd             ~ "chip_only",
          core_set == "adult" & pop_tier == 1    ~ "broader_incl_duals",
          core_set == "child" & pop_tier > 0     ~ "narrower",
          pop_extra > 0                          ~ "broader",
          TRUE                                   ~ "target"
        )
      )
  }

  # Deterministic pick for the distinct() in widen_measures(): prefer a real
  # value over a suppressed/missing one, then the population closest to the
  # Core Set target (pop_tier, then pop_extra; see add_population_match()).
  # (Rows that cannot represent the target are filtered out before this; the
  # remaining fallback keys -- fewest dual/"Other" qualifiers, then the most
  # inclusive string -- only matter between rows of the same tier.) Ties are
  # broken alphabetically by population and (for measures whose sub-components
  # share one population, e.g. AIS-AD's per-vaccine rows) by rate definition,
  # so re-running the ingest always keeps the same row regardless of the CMS
  # API's response order. Expects one row per source row (rate stat only) with
  # `rate_missing` (see widen_measures()).
  arrange_for_distinct <- function(d) {
    d %>%
      mutate(
        population_priority = if_else(
          grepl("Dual(ly)? Eligible", population, ignore.case = TRUE) |
            grepl("(^|, )Other(,|$)", population, ignore.case = TRUE),
          1L, 0L
        )
      ) %>%
      arrange(rate_missing, pop_tier, pop_extra,        # NA tiers sort last
              population_priority, desc(nchar(population)), population,
              measure_info) %>%
      select(-population_priority)
  }

  # Many CMS measures report several distinct rates under ONE abbreviation in
  # the same year (different age bands, sub-measures, survey items, ...). Each
  # distinct rate_definition must land in its own column, otherwise the
  # one-row-per-state-year collapse below silently keeps an arbitrary one.
  # sub_metric / sub_extra below label them; the unlabeled ("") column is the
  # headline / total rate. After labeling, warn_collapsed() should report
  # nothing -- if it does, a new definition needs a rule here.
  mi_has <- function(x, pattern) grepl(pattern, coalesce(x, ""), ignore.case = TRUE)

  # IET-AD: initiation (14 days) / engagement (34 days) by substance. The
  # subject is read from the text before "who ..." / "with Engagement|Initiation"
  # because every definition also mentions "Alcohol or Other Drug Treatment".
  iet_label <- function(x) {
    subj <- sub("( who | with (Engagement|Initiation)).*$", "", coalesce(x, ""),
                ignore.case = TRUE, perl = TRUE)
    phase <- if_else(grepl("Engage", x, ignore.case = TRUE), "engage", "init")
    sub_ <- case_when(
      grepl("Opioid", subj, ignore.case = TRUE)                                     ~ "opioid",
      grepl("Alcohol or Other Drug|Alcohol or Drug|Total SUD", subj, ignore.case = TRUE) ~ "total",
      grepl("Alcohol", subj, ignore.case = TRUE)                                    ~ "alc",
      grepl("Other", subj, ignore.case = TRUE)                                      ~ "other",
      TRUE                                                                          ~ "total"
    )
    paste(phase, sub_, sep = "_")
  }
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2, geography != "00") %>%
    mutate(state_name = coalesce(
      geography_name,
      c(GU = "Guam", PR = "Puerto Rico", VI = "U.S. Virgin Islands")[state]
    )) %>%
    select(geography, state_name)

  #creating wide format
  data_long <- data1 %>%
    # state names -> FIPS (all_fips has no names for the territories, so fill
    # them in from the abbreviation; CMS also uses "Dist. of Col." in some years)
    mutate(state = case_when(
      state == "Dist. of Col."  ~ "District of Columbia",
      state == "Virgin Islands" ~ "U.S. Virgin Islands",
      TRUE ~ state
    )) %>%
    left_join(state_fips_lookup, by = c("state" = "state_name")) %>%
    mutate(
      time            = paste0(time_year, "-12-31"),   # annual: YYYY-12-31
      geography_level = "s",
      age             = "Overall",
      sex             = "Overall",
      race_ethnicity  = "Overall",
      # payer comes from the CMS population string: Medicaid only, CHIP only,
      # both ("Medicaid & CHIP"), or neither ("Other"). The 2024+ dual-eligible
      # breakouts ("Individuals Dually Eligible for Medicare and Medicaid" /
      # "... and Separate CHIP") name a program only to describe Medicare
      # overlap, so they are blanked before testing for Medicaid / CHIP.
      pop_payer = gsub("Individuals Dually Eligible for Medicare and (Separate CHIP|Medicaid)",
                       "", population, ignore.case = TRUE),
      payer = case_when(
        grepl("Medicaid", pop_payer, ignore.case = TRUE) &
          grepl("CHIP", pop_payer, ignore.case = TRUE)   ~ "Medicaid & CHIP",
        grepl("Medicaid", pop_payer, ignore.case = TRUE) ~ "Medicaid",
        grepl("CHIP", pop_payer, ignore.case = TRUE)     ~ "CHIP",
        TRUE                                              ~ "Other"
      ),
      row_id = row_number(),
      # Labels for definitions that share one abbreviation (see mi_has above).
      # IMA carries two definitions from 2017 (Combination 1 = MenACWY + Tdap,
      # and HPV series completion); WCV carries four age bands from 2021. Keep
      # the base column as Combination 1 / ages 3-21 and split the rest out.
      # AMR reports every year ages 5-11, 12-18 and 5-18 (child) and 19-50,
      # 51-64 and 19-64 (adult) under one abbreviation; the base column is the
      # overall 5-18 / 19-64 rate and the narrower bands are split out.
      sub_metric = case_when(
        measure_abbr %in% c("AMR-CH", "AMR-AD") &
          mi_has(measure_info, "Ages 5( to |-)11\\s*$")                          ~ "5_11",
        measure_abbr %in% c("AMR-CH", "AMR-AD") &
          mi_has(measure_info, "Ages 12( to |-)18\\s*$")                         ~ "12_18",
        measure_abbr %in% c("AMR-CH", "AMR-AD") &
          mi_has(measure_info, "Ages 19( to |-)50\\s*$")                         ~ "19_50",
        measure_abbr %in% c("AMR-CH", "AMR-AD") &
          mi_has(measure_info, "Ages 51( to |-)64\\s*$")                         ~ "51_64",
        measure_abbr %in% c("AMR-CH", "AMR-AD")                                  ~ "",
        measure_abbr == "IMA-CH" &
          grepl("Papillomavirus|HPV", coalesce(measure_info, ""))               ~ "hpv",
        measure_abbr == "IMA-CH"                                                 ~ "",
        measure_abbr == "WCV-CH" &
          grepl("Ages 3 to 11$", coalesce(measure_info, ""))                     ~ "3_11",
        measure_abbr == "WCV-CH" &
          grepl("Ages 12 to 17$", coalesce(measure_info, ""))                    ~ "12_17",
        measure_abbr == "WCV-CH" &
          grepl("Ages 18 to 21$", coalesce(measure_info, ""))                    ~ "18_21",
        measure_abbr == "WCV-CH"                                                 ~ "",
        # vaccines (AIS-AD, 2025 only) and pregnancy vaccines (PRS)
        measure_abbr == "AIS-AD" & mi_has(measure_info, "Influenza")             ~ "flu",
        measure_abbr == "AIS-AD" & mi_has(measure_info, "Td/Tdap")               ~ "tdap",
        measure_abbr == "AIS-AD" & mi_has(measure_info, "Zoster")                ~ "zoster",
        measure_abbr %in% c("PRS-AD", "PRS-CH") &
          mi_has(measure_info, "Recommended Immunizations")                      ~ "both",
        measure_abbr %in% c("PRS-AD", "PRS-CH") & mi_has(measure_info, "Tdap")   ~ "tdap",
        measure_abbr %in% c("PRS-AD", "PRS-CH") & mi_has(measure_info, "Influenza") ~ "flu",
        # antidepressant management: base = acute phase (12 weeks)
        measure_abbr == "AMM-AD" & mi_has(measure_info, "6 Months")              ~ "6mo",
        measure_abbr == "AMM-AD"                                                 ~ "",
        # CAP-CH: four age groups; base = ages 12-19 (the column historically kept)
        measure_abbr == "CAP-CH" & mi_has(measure_info, "Past Year: (Ages )?12")         ~ "12_24m",
        measure_abbr == "CAP-CH" & mi_has(measure_info, "Past Year: (Ages )?25")         ~ "25m_6y",
        measure_abbr == "CAP-CH" & mi_has(measure_info, "Past Two Years: (Ages )?7")     ~ "7_11",
        measure_abbr == "CAP-CH"                                                 ~ "",
        # contraceptive care: LARC vs most/moderately effective, 3 days vs 60/90 days
        measure_abbr %in% c("CCP-AD", "CCP-CH") & mi_has(measure_info, "Long-Acting") &
          mi_has(measure_info, "Within 3 Days")                                  ~ "",
        measure_abbr %in% c("CCP-AD", "CCP-CH") & mi_has(measure_info, "Long-Acting") ~ "larc_late",
        measure_abbr %in% c("CCP-AD", "CCP-CH") & mi_has(measure_info, "Within 3 Days") ~ "mod_3d",
        measure_abbr %in% c("CCP-AD", "CCP-CH")                                  ~ "mod_late",
        measure_abbr %in% c("CCW-AD", "CCW-CH") & mi_has(measure_info, "Most Effective") ~ "mod",
        measure_abbr %in% c("CCW-AD", "CCW-CH")                                  ~ "",
        # childhood immunization status: base = Combination 3
        measure_abbr == "CIS-CH" & mi_has(measure_info, "Combination 10")        ~ "combo10",
        measure_abbr == "CIS-CH" & mi_has(measure_info, "Measles|MMR")           ~ "mmr",
        measure_abbr == "CIS-CH" & mi_has(measure_info, "Flu")                   ~ "flu",
        measure_abbr == "CIS-CH"                                                 ~ "",
        # colorectal screening: base = younger age band (46-49, then 46-50)
        measure_abbr == "COL-AD" & mi_has(measure_info, "Ages 46 to")            ~ "",
        measure_abbr == "COL-AD"                                                 ~ "older",
        # CAHPS survey items (CPA-AD adult, CPC-CH child)
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Informed")                                       ~ "doctor_informed",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Getting Care Quickly|Got Care Quickly")          ~ "care_quickly",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Getting Needed Care|Got Needed Care")            ~ "needed_care",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Customer Service")                               ~ "customer_service",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Communicate")                                    ~ "doctor_communication",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Rating of Health Care|Rated Their Health Care")  ~ "rating_care",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Rating of Health Plan|Rated Their Health Plan")  ~ "rating_plan",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Rating of Personal Doctor|Rated Their Personal Doctor") ~ "rating_doctor",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "Rating of Specialist|Rated Their Specialist")    ~ "rating_specialist",
        # glycemic status / HbA1c control
        measure_abbr %in% c("GSD-AD", "HBD-AD") & mi_has(measure_info, "<8")     ~ "lt8",
        measure_abbr %in% c("GSD-AD", "HBD-AD") & mi_has(measure_info, ">9")     ~ "gt9",
        # initiation / engagement of SUD treatment by substance
        measure_abbr == "IET-AD"                                                 ~ iet_label(measure_info),
        # asthma medication management (MMA-CH): base = total ages 5-20
        measure_abbr == "MMA-CH" & mi_has(measure_info, "5-11")                  ~ "5_11",
        measure_abbr == "MMA-CH" & mi_has(measure_info, "12-18")                 ~ "12_18",
        measure_abbr == "MMA-CH" & mi_has(measure_info, "19-20")                 ~ "19_20",
        measure_abbr == "MMA-CH"                                                 ~ "",
        # tobacco cessation: base = advised to quit
        measure_abbr == "MSC-AD" & mi_has(measure_info, "Medication")            ~ "meds",
        measure_abbr == "MSC-AD" & mi_has(measure_info, "Strateg|Methods")       ~ "strategies",
        measure_abbr == "MSC-AD"                                                 ~ "",
        # NCI-IDD adult consumer survey (renamed NCIIDD-AD in 2024)
        measure_abbr %in% c("NCIDDS-AD", "NCIIDD-AD") &
          mi_has(measure_info, "way to get places")                              ~ "transport",
        measure_abbr %in% c("NCIDDS-AD", "NCIIDD-AD") &
          mi_has(measure_info, "life decisions")                                 ~ "life_decisions",
        measure_abbr %in% c("NCIDDS-AD", "NCIIDD-AD") &
          mi_has(measure_info, "daily schedule|everyday lives")                  ~ "daily_choices",
        # opioid use disorder pharmacotherapy: base = any medication (total rate)
        measure_abbr == "OUD-AD" & mi_has(measure_info, "Buprenorphine")         ~ "bup",
        measure_abbr == "OUD-AD" & mi_has(measure_info, "Injectable Naltrexone") ~ "ntx_inj",
        measure_abbr == "OUD-AD" & mi_has(measure_info, "Methadone")             ~ "methadone",
        measure_abbr == "OUD-AD" & mi_has(measure_info, "Oral Naltrexone")       ~ "ntx_oral",
        measure_abbr == "OUD-AD"                                                 ~ "",
        # PPC2: postpartum visit vs timeliness of prenatal care
        measure_abbr %in% c("PPC2-AD", "PPC2-CH") &
          mi_has(measure_info, "Postpartum Care Visit")                          ~ "postpartum",
        measure_abbr %in% c("PPC2-AD", "PPC2-CH") &
          mi_has(measure_info, "Prenatal Care Visit")                            ~ "prenatal",
        measure_abbr == "SFM-CH" & mi_has(measure_info, "All Four")              ~ "all4",
        measure_abbr == "SFM-CH"                                                 ~ "any",
        # well-care for children: BMI (base), nutrition counseling, activity counseling
        measure_abbr == "WCC-CH" & mi_has(measure_info, "Nutrition")             ~ "nutrition",
        measure_abbr == "WCC-CH" & mi_has(measure_info, "Physical Activity")     ~ "activity",
        measure_abbr == "WCC-CH"                                                 ~ "",
        grepl("Within 7 Days|7-Day|7 Days", measure_info, ignore.case = TRUE)    ~ "7d",
        grepl("Within 30 Days|30-Day|30 Days", measure_info, ignore.case = TRUE) ~ "30d",
        grepl("Initiation Phase", measure_info, ignore.case = TRUE)               ~ "init",
        grepl("Continuation|Maintenance", measure_info, ignore.case = TRUE)       ~ "cont",
        grepl("Blood Glucose and Cholesterol", measure_info, ignore.case = TRUE)  ~ "gluc_chol",
        grepl("Blood Glucose", measure_info, ignore.case = TRUE)                  ~ "gluc",
        grepl("Cholesterol", measure_info, ignore.case = TRUE)                    ~ "chol",
        TRUE ~ ""
      ),
      # Second label: age band / population / threshold within a measure.
      # FFY 2025 adds "Age 65 and Older" rows next to "Ages 18 to 64" for the
      # FUA/FUH/FUM adult measures; those must not share the 18-64 column.
      sub_age = case_when(
        measure_abbr %in% c("APM-CH", "APP-CH") &
          mi_has(measure_info, "Ages 1 to 11\\s*$")                              ~ "1_11",
        measure_abbr %in% c("APM-CH", "APP-CH") &
          mi_has(measure_info, "Ages 12 to 17\\s*$")                             ~ "12_17",
        measure_abbr %in% c("FUA-AD", "FUH-AD", "FUM-AD") &
          mi_has(measure_info, "Age 65 and Older")                               ~ "65p",
        measure_abbr == "OEV-CH" & mi_has(measure_info, "Age < 3")               ~ "lt3",
        measure_abbr == "OEV-CH" & mi_has(measure_info, "Ages 3 to 5")           ~ "3_5",
        measure_abbr == "OEV-CH" & mi_has(measure_info, "Ages 6 to 14")          ~ "6_14",
        measure_abbr == "OEV-CH" & mi_has(measure_info, "Ages 15 to 20")         ~ "15_20",
        measure_abbr == "TFL-CH" & mi_has(measure_info, "Ages 1 to 2")           ~ "1_2",
        measure_abbr == "TFL-CH" & mi_has(measure_info, "Ages 3 to 5")           ~ "3_5",
        measure_abbr == "TFL-CH" & mi_has(measure_info, "Ages 6 to 14")          ~ "6_14",
        measure_abbr == "TFL-CH" & mi_has(measure_info, "Ages 15 to 20")         ~ "15_20",
        measure_abbr == "CPC-CH" & mi_has(measure_info, "Chronic Conditions")    ~ "ccc",
        measure_abbr == "CPC-CH" & mi_has(measure_info, "General Child")         ~ "gc",
        TRUE ~ ""
      ),
      # CAHPS FFY 2025 reports a broader top-box next to the stricter one
      # ("Usually or Always" next to "Always"; 8-10 next to 9-10 ratings).
      # FFY 2023 reported CAHPS as composite / rating scores ("Getting Care
      # Quickly Composite", "Rating of Health Care"); from FFY 2024 CMS reports
      # top-box percentages ("Percentage who Selected ... Always"). Different
      # statistics, so the 2023 definitions get their own "composite" column.
      sub_thr = case_when(
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          !mi_has(measure_info, "^Percentage")                                  ~ "composite",
        measure_abbr %in% c("CPA-AD", "CPC-CH") &
          mi_has(measure_info, "\"Usually\" or \"Always|\"8\", \"9\" or \"10\"") ~ "broad",
        TRUE ~ ""),
      sub_extra = gsub("^_+|_+$", "", gsub("__+", "_", paste(sub_age, sub_thr, sep = "_"))),
      # Measures CMS renamed / re-abbreviated mid-series continue their column:
      # FUA/FUM-AD (FFY 2018 combined measure) -> FUA-AD / FUM-AD, and the
      # first-15-months well-child measure that moved under W30-CH in FFY 2021,
      # and the NCI-IDD adult survey, re-abbreviated NCIDDS-AD -> NCIIDD-AD in
      # FFY 2024 with word-for-word identical rate definitions.
      # (Not joined: HBD-AD -> GSD-AD, renamed when glycemic status replaced
      # HbA1c; PPC-AD -> PPC2-AD, which is restricted to ages 21+.)
      measure_clean = case_when(
        measure_abbr == "NCIDDS-AD"                                              ~ "nciidd_ad",
        measure_abbr == "FUA/FUM-AD" & mi_has(measure_info, "Alcohol")           ~ "fua_ad",
        measure_abbr == "FUA/FUM-AD" & mi_has(measure_info, "Mental Illness")    ~ "fum_ad",
        measure_abbr == "W30-CH" & mi_has(measure_info, "First 15 Months")       ~ "w15_ch",
        TRUE ~ tolower(gsub("[^A-Za-z0-9]+", "_", measure_abbr))
      ),
      # CMS non-numeric rate codes, all -> NA (rate not available):
      #   DS  = suppressed under CMS cell-size policy (counts 1-10)
      #   #   = not reported, denominator < 30 (or CAHPS < 100 respondents)
      #   NR  = state did not report the measure for this population
      #   NPR = CMS does not publicly report state rates for this measure
      # (a blank rate = state reported a denominator of zero). Any other code
      # still triggers an as.numeric() coercion warning so it gets noticed.
      value = as.numeric(if_else(value %in% c("DS", "#", "NR", "NPR"), NA_character_, value)),
      pct25 = as.numeric(if_else(pct25 == "DS", NA_character_, pct25)),
      pct75 = as.numeric(if_else(pct75 == "DS", NA_character_, pct75)),
      # From FFY 2016 CMS orders the quartiles by performance, so for
      # lower-is-better measures (PCR, PQIs, AMB, LBW, ...) the "bottom
      # quartile" / "25th percentile" is the HIGHER value; FFY 2014-2015 order
      # them by value. Normalize so pct_25 / pct_75 are always the 25th / 75th
      # percentiles of the state rate distribution (pct_25 <= pct_75).
      # (kept before normalizing: from FFY 2016 an inverted pair marks a
      # lower-is-better measure; used for measures CMS never typed, see below)
      pct_inverted = pct25 > pct75,
      pct_lo = pmin(pct25, pct75),
      pct75  = pmax(pct25, pct75),
      pct25  = pct_lo
    ) %>%
    select(-pct_lo) %>%
    add_population_match() %>%
    select(geography, geography_level, time, age, sex, race_ethnicity,
           payer, domain, measure_abbr, measure_info, measure_clean, sub_metric, sub_extra,
           population, pop_tier, pop_extra, population_match,
           measure_type, pct_inverted,
           row_id, value, pct25, pct75) %>%
    pivot_longer(cols = c(value, pct25, pct75), names_to = "stat", values_to = "val") %>%
    mutate(
      stat = case_when(
        stat == "value" ~ "rate",
        stat == "pct25" ~ "pct_25",
        stat == "pct75" ~ "pct_75"
      ),
      var_name = paste("medicaid", measure_clean, sub_metric, sub_extra, stat, sep = "_"),
      var_name = gsub("_+$", "", gsub("__+", "_", var_name))
    ) %>%
    select(-measure_clean, -sub_metric, -sub_extra, -stat)

  # collapse to one row per (keys, measure column) and widen.
  # The winning source row is chosen once per (keys, measure column) using the
  # rate, and the rate / pct_25 / pct_75 are then all taken from that same row
  # (same population), never mixed across rows. `keys` has no payer, so the
  # pick is made across all populations a state reported.
  # flag_match = TRUE adds a <measure>_population_match column per measure
  # recording how the chosen row relates to the target population (the Core
  # Set files then pivot these wide columns into long format).
  widen_measures <- function(d, keys = c("geography", "time", "domain"),
                             flag_match = FALSE, label = "") {
    d <- warn_collapsed(d) %>%
      mutate(base_var = sub("_(rate|pct_25|pct_75)$", "", var_name)) %>%
      group_by(row_id) %>%
      mutate(rate_missing = any(grepl("_rate$", var_name) & is.na(val))) %>%
      ungroup()
    winners <- d %>%
      filter(grepl("_rate$", var_name)) %>%
      arrange_for_distinct() %>%
      distinct(across(all_of(c(keys, "base_var"))), .keep_all = TRUE)
    wide <- d %>%
      filter(row_id %in% winners$row_id) %>%
      select(-measure_abbr, -measure_info, -population, -row_id,
             -base_var, -rate_missing, -pop_tier, -pop_extra, -population_match,
             -measure_type, -pct_inverted,
             -any_of(setdiff("payer", keys))) %>%
      pivot_wider(names_from = var_name, values_from = val) %>%
      mutate(across(starts_with("medicaid_"), as.numeric))
    if (flag_match) {
      flags <- winners %>%
        transmute(across(all_of(keys)),
                  flag_name = paste0(base_var, "_population_match"),
                  population_match) %>%
        pivot_wider(names_from = flag_name, values_from = population_match)
      wide <- wide %>%
        left_join(flags, by = keys) %>%
        # keep each measure's rate / pct_25 / pct_75 / flag together
        select(!starts_with("medicaid_"), sort(grep("^medicaid_", names(.), value = TRUE)))
      tiers <- winners %>% count(population_match, name = "n")
      message("medicaid_quality ", label, ": state-year-measure picks by population match: ",
              paste0(tiers$population_match, "=", tiers$n, collapse = ", "))
    }
    wide
  }

  # One file per Core Set: one value per state-year-measure from the row
  # closest to that Core Set's target population (Child: Medicaid + CHIP;
  # Adult: non-dual Medicaid), with a <measure>_population_match flag. Rows
  # that cannot represent the target (adult CHIP-only, duals-only, Other-only)
  # are excluded. See add_population_match().
  # Written in long format -- one row per state, year and measure -- because
  # the Core Set changes every year and a wide file is mostly empty cells.
  # `measure` is a measure id (e.g. medicaid_bcs_ad, documented by the
  # medicaid_bcs_ad_rate entry in measure_info.json) and
  # `value` is the state rate. Only state-year-measures with a reported value
  # are kept (suppressed / not reported / not public = no row), so measures
  # CMS never publishes by state (e.g. CDF) do not appear. age / sex /
  # race_ethnicity are always "Overall" and geography_level always "s", so
  # they are omitted.
  core_set_file <- function(suffix, label) {
    data_long %>%
      filter(!is.na(pop_tier), grepl(suffix, measure_abbr)) %>%
      widen_measures(keys = c("geography", "time", "domain"), flag_match = TRUE,
                     label = label) %>%
      pivot_longer(starts_with("medicaid_"),
                   names_to = c("measure", ".value"),
                   names_pattern = "^(.*)_(rate|pct_25|pct_75|population_match)$") %>%
      filter(!is.na(rate)) %>%
      # In FFY 2014 CMS used domain labels it later dropped (e.g. Behavioral
      # Health vs Behavioral Health Care from 2015); label every year with the
      # measure's most recent domain
      # (the most common label in its latest year) so each measure has one.
      group_by(measure) %>%
      mutate(domain = names(which.max(table(domain[time == max(time)])))) %>%
      ungroup() %>%
      left_join(measure_direction, by = "measure") %>%
      select(geography, time, domain, measure, measure_type, value = rate,
             pct_25, pct_75, population_match) %>%
      arrange(geography, time, measure)
  }

  # Whether higher or lower rates are better, per measure (one value for all
  # years). CMS's measure_type (FFY 2019 on) is used where available; it is
  # constant for every rate definition. For measures CMS never typed (those
  # that ended before FFY 2019), it is inferred from CMS's quartiles: from FFY
  # 2016 CMS orders them by performance, so a bottom quartile above the top
  # quartile marks a lower-is-better measure (this matches CMS's measure_type
  # in every row where both exist). Measures with neither stay NA.
  measure_direction <- data_long %>%
    filter(grepl("_rate$", var_name)) %>%
    mutate(measure = sub("_rate$", "", var_name),
           cms = sub(" for this measure$", "", measure_type),
           yr = as.integer(substr(time, 1, 4))) %>%
    group_by(measure) %>%
    summarize(
      cms = first(na.omit(cms)),
      inferred = case_when(
        !any(yr >= 2016 & !is.na(pct_inverted)) ~ NA_character_,
        mean(pct_inverted[yr >= 2016], na.rm = TRUE) > 0.5 ~ "Lower rates are better",
        TRUE ~ "Higher rates are better"),
      .groups = "drop") %>%
    transmute(measure, measure_type = coalesce(cms, inferred))
  data_child <- core_set_file("-CH$", "data_child_core_set")
  data_adult <- core_set_file("-AD$", "data_adult_core_set")

  #writing standard files (only the two Core Set files are published)
  vroom::vroom_write(data_child, "standard/data_child_core_set.csv.gz", delim = ",")
  vroom::vroom_write(data_adult, "standard/data_adult_core_set.csv.gz", delim = ",")
  
  #record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}