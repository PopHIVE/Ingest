# =============================================================================
# U.S. Census Bureau Data Ingestion
# Source: https://www.census.gov (PEP, SAIPE, SAHIE, 2020 Decennial OQM)
# Ingests four Census programs through a single ingest.R, each with its own key
# in the process record. See README.md for the program table and conventions.
#
# ACS 5-year estimates ("2024 American Community Survey 5-Year Estimates,
# Powered by Metopio") and the urban/rural classification were split out to
# data/ACS_estimates/ingest.R, since both are written into their own
# data_county.csv.gz there, separate from this folder's remaining programs.
# =============================================================================

#to edit API key:
#library("usethis")
#edit_r_environ()
##add
#CENSUS_API_KEY="XXXXXXXXXX"

library(dplyr)
library(vroom)
library(censusapi)
library(readxl)

# -----------------------------------------------------------------------------
# Percent scale
# -----------------------------------------------------------------------------
# PopHIVE's standard is 0-100: a percent measure stores 18.44, not 0.1844.
# The Census API returns counts, so every share below is derived as a
# proportion; each derived block is converted once, where it is built.
#
# The measure list is read from measure_info.json rather than hard-coded, so it
# cannot drift from the declaration it implements. Adding a percent measure to
# measure_info.json is therefore enough; nothing here needs editing.
`%||%` <- function(a, b) if (is.null(a)) b else a

PERCENT_MEASURES <- local({
  mi <- jsonlite::fromJSON("measure_info.json", simplifyVector = FALSE)
  mi[["_sources"]] <- NULL
  is_pct <- vapply(mi, function(e) {
    identical(tolower(as.character(e[["measure_type"]] %||% "")), "percent")
  }, logical(1))
  names(mi)[is_pct]
})

# Guarded per column, so this is a no-op on values that are already 0-100.
# That matters if the Census API ever starts publishing a share as a percentage,
# or if a derivation below is rewritten to produce one: without the guard those
# values would be multiplied a second time and silently land 100x too large.
#
# The threshold is 2, not 1, because a proportion legitimately reaches 1.0
# (100%). Its blind spot is a genuine percentage whose maximum never exceeds 2 --
# that would be read as a proportion and scaled. No census percent measure is
# close: the lowest column maximum is 8.88 (acs_INL), so there is 4x headroom.
# Re-check this if a sub-2% percent measure is ever added here.
PERCENT_SCALE_CEILING <- 2

to_percent_scale <- function(df) {
  for (cl in intersect(names(df), PERCENT_MEASURES)) {
    x  <- suppressWarnings(as.numeric(df[[cl]]))
    mx <- suppressWarnings(max(x, na.rm = TRUE))
    if (is.finite(mx) && mx > PERCENT_SCALE_CEILING) next
    df[[cl]] <- x * 100
  }
  df
}

# -----------------------------------------------------------------------------
# Read Census API key
# -----------------------------------------------------------------------------

api_key <- Sys.getenv("CENSUS_API_KEY")

# -----------------------------------------------------------------------------
# Initialize process record
# -----------------------------------------------------------------------------
# process.json is created by dcf::dcf_add_source()
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# Forced rebuilds
# -----------------------------------------------------------------------------
# Each block is guarded on its upstream vintage, so a changed *derivation* in
# this script won't propagate on its own. dcf's `force` only decides whether
# this script runs, not what it does once running. Set CENSUS_FORCE_REBUILD to
# "all" or any of pep,saipe,oqm,sahie to bypass those guards for one run.
FORCE_BLOCKS <- trimws(strsplit(
  tolower(Sys.getenv("CENSUS_FORCE_REBUILD", "")), ",", fixed = TRUE
)[[1]])

force_block <- function(name) {
  forced <- "all" %in% FORCE_BLOCKS || name %in% FORCE_BLOCKS
  if (forced) message("[FORCE] rebuilding block: ", name)
  forced
}

if (length(FORCE_BLOCKS) && any(nzchar(FORCE_BLOCKS))) {
  message("CENSUS_FORCE_REBUILD=", paste(FORCE_BLOCKS, collapse = ","))
}

# =============================================================================
# Population Estimates Program (PEP) — annual county-level population,
# age (65+, under 18), sex, and race/Hispanic-origin composition.
# Source: U.S. Census Bureau PEP, "pep/charv" API.
# Written to its own standard/data_pep.csv.gz (different vintage cadence
# than ACS). Feeds chr_population, chr_65_and_older, chr_female, and the
# chr_ race/Hispanic measures in PopHIVE/us-rates.
# =============================================================================

PEP_ENDPOINT <- "pep/charv"

# Race-alone codes; no HISP breakdown published at the county level.
PEP_RACE_ALONE <- c(aian = "006", asian = "012", nhpi = "050")

# POPGROUP 451/453 ("White/Black alone, not Hispanic") return no data at
# county or state level. Use the plain race code (002/004) crossed with
# HISP=1 instead.
PEP_RACE_NOT_HISPANIC <- c(nh_white = "002", nh_black = "004")

latest_pep_vintage <- tryCatch({
  censusapi::listCensusApis() %>%
    filter(name == PEP_ENDPOINT) %>%
    pull(vintage) %>%
    as.integer() %>%
    max(na.rm = TRUE)
}, error = function(e) {
  message("[WARN] Could not fetch PEP API metadata: ", conditionMessage(e))
  if (!is.null(process$pep_vintage_year)) as.integer(process$pep_vintage_year) else NA_integer_
})

if (!is.na(latest_pep_vintage) &&
    (force_block("pep") || is.null(process$pep_vintage_year) ||
     process$pep_vintage_year < latest_pep_vintage)) {

  message("PEP latest vintage year: ", latest_pep_vintage)
  pep_dataset <- paste0(latest_pep_vintage, "/", PEP_ENDPOINT)
  safe_div <- function(num, denom) if_else(denom <= 0, NA_real_, num / denom)

  # Each vintage bundles several reference dates (April 2020 Census Day,
  # then a July estimate per year); keep only the most recent one.
  latest_period <- function(df) {
    df %>%
      mutate(YEAR = as.integer(YEAR), MONTH = as.integer(MONTH)) %>%
      filter(YEAR == max(YEAR)) %>%
      filter(MONTH == max(MONTH))
  }

  # AGE "0" = all ages, matched on the returned string rather than passed
  # as a query predicate (AGE="0" 404s as a predicate; AGE="0000" doesn't).
  #
  # PEP's "charv" endpoint supports county, state, and national ("us:*")
  # geography natively -- the whole fetch is parameterized by region_str/
  # join_cols/build_geography and run three times rather than duplicated,
  # then mixed into one file by geography length like every other
  # multi-level source here. National rows come back with a "us" column
  # instead of "state"/"county".
  fetch_pep_level <- function(region_str, join_cols, build_geography) {
    fetch_pep_race_alone <- function(popgroup_code) {
      tryCatch({
        censusapi::getCensus(
          name = pep_dataset, vars = c("POP", "AGE", "YEAR", "MONTH"),
          region = region_str, POPGROUP = popgroup_code, key = api_key
        ) %>%
          latest_period() %>%
          filter(AGE == "0") %>%
          transmute(across(all_of(join_cols)), pop = as.numeric(POP))
      }, error = function(e) {
        message("  [WARN] PEP fetch failed (POPGROUP=", popgroup_code, ", region=", region_str, "): ", conditionMessage(e))
        NULL
      })
    }

    fetch_pep_race_not_hispanic <- function(popgroup_code) {
      tryCatch({
        censusapi::getCensus(
          name = pep_dataset, vars = c("POP", "AGE", "YEAR", "MONTH"),
          region = region_str, POPGROUP = popgroup_code, HISP = "1", key = api_key
        ) %>%
          latest_period() %>%
          filter(AGE == "0") %>%
          transmute(across(all_of(join_cols)), pop = as.numeric(POP))
      }, error = function(e) {
        message("  [WARN] PEP fetch failed (POPGROUP=", popgroup_code, ", HISP=1, region=", region_str, "): ", conditionMessage(e))
        NULL
      })
    }

    pep_race <- Filter(Negate(is.null), c(
      lapply(names(PEP_RACE_ALONE), function(nm) {
        df <- fetch_pep_race_alone(PEP_RACE_ALONE[[nm]])
        if (!is.null(df)) rename(df, !!nm := pop) else NULL
      }),
      lapply(names(PEP_RACE_NOT_HISPANIC), function(nm) {
        df <- fetch_pep_race_not_hispanic(PEP_RACE_NOT_HISPANIC[[nm]])
        if (!is.null(df)) rename(df, !!nm := pop) else NULL
      })
    ))

    # Hispanic origin, any race: HISP=2. Omitting HISP (as below) defaults
    # to its "Total" category, not a full breakdown.
    pep_hispanic <- tryCatch({
      censusapi::getCensus(
        name = pep_dataset, vars = c("POP", "AGE", "YEAR", "MONTH"),
        region = region_str, POPGROUP = "001", HISP = "2", key = api_key
      ) %>%
        latest_period() %>%
        filter(AGE == "0") %>%
        transmute(across(all_of(join_cols)), hispanic = as.numeric(POP))
    }, error = function(e) {
      message("  [WARN] PEP Hispanic-origin fetch failed (region=", region_str, "): ", conditionMessage(e))
      NULL
    })

    # Total population and the 65+/18+ pre-aggregated age codes, all races
    # and Hispanic origins combined. Under-18 = total - 18+.
    pep_age <- tryCatch({
      df <- censusapi::getCensus(
        name = pep_dataset, vars = c("POP", "AGE", "YEAR", "MONTH"),
        region = region_str, POPGROUP = "001", key = api_key
      ) %>% latest_period() %>% mutate(POP = as.numeric(POP))

      df %>% filter(AGE == "0") %>% transmute(across(all_of(join_cols)), total = POP) %>%
        left_join(df %>% filter(AGE == "6599") %>% transmute(across(all_of(join_cols)), age_65_plus = POP),
                  by = join_cols) %>%
        left_join(df %>% filter(AGE == "1899") %>% transmute(across(all_of(join_cols)), age_18_plus = POP),
                  by = join_cols)
    }, error = function(e) {
      message("  [WARN] PEP age fetch failed (region=", region_str, "): ", conditionMessage(e))
      NULL
    })

    # Female, all races/ages/Hispanic origins combined: SEX=2.
    pep_female <- tryCatch({
      censusapi::getCensus(
        name = pep_dataset, vars = c("POP", "AGE", "YEAR", "MONTH"),
        region = region_str, POPGROUP = "001", SEX = "2", key = api_key
      ) %>%
        latest_period() %>%
        filter(AGE == "0") %>%
        transmute(across(all_of(join_cols)), female = as.numeric(POP))
    }, error = function(e) {
      message("  [WARN] PEP female fetch failed (region=", region_str, "): ", conditionMessage(e))
      NULL
    })

    pep_blocks <- Filter(Negate(is.null), c(pep_race, list(pep_hispanic, pep_age, pep_female)))
    if (length(pep_blocks) == 0) return(NULL)

    Reduce(function(a, b) left_join(a, b, by = join_cols), pep_blocks) %>%
      mutate(
        geography        = build_geography(.),
        time             = paste0(latest_pep_vintage, "-12-31"),
        pep_population   = total,
        pep_pct_65_older = safe_div(age_65_plus, total),
        pep_pct_under_18 = safe_div(total - age_18_plus, total),
        pep_pct_female   = safe_div(female, total),
        pep_pct_aian     = safe_div(aian, total),
        pep_pct_asian    = safe_div(asian, total),
        pep_pct_nhpi     = safe_div(nhpi, total),
        pep_pct_nh_black = safe_div(nh_black, total),
        pep_pct_nh_white = safe_div(nh_white, total),
        pep_pct_hispanic = safe_div(hispanic, total)
      ) %>%
      select(geography, time, starts_with("pep_"))
  }

  pep_result <- bind_rows(
    fetch_pep_level(
      "county:*", c("state", "county"),
      function(df) paste0(sprintf("%02d", as.integer(df$state)), sprintf("%03d", as.integer(df$county)))
    ),
    fetch_pep_level("state:*", c("state"), function(df) sprintf("%02d", as.integer(df$state))),
    fetch_pep_level("us:*", c("us"), function(df) "00")
  )

  pep_result <- to_percent_scale(pep_result)
  if (!is.null(pep_result) && nrow(pep_result) > 0) {
    vroom::vroom_write(pep_result, "standard/data_pep.csv.gz", delim = ",")
    process$pep_vintage_year <- latest_pep_vintage
    dcf::dcf_process_record(updated = process)
    message("PEP data written for vintage ", latest_pep_vintage, " (", nrow(pep_result), " rows, county+state+national)")
  }
} else {
  message("PEP data is up to date (last vintage: ", process$pep_vintage_year, ")")
}

# =============================================================================
# SAIPE (Small Area Income and Poverty Estimates) — annual county-level
# child poverty rate and median household income.
# Source: U.S. Census Bureau SAIPE, "timeseries/poverty/saipe" API.
# SAEPOVRT0_17_PT is a 0-100 percent; rescaled to this file's 0-1 convention.
# Feeds chr_children_in_poverty and chr_median_household_income in us-rates.
# =============================================================================

SAIPE_ENDPOINT <- "timeseries/poverty/saipe"

# SAIPE is a "time"-predicate timeseries dataset, not vintage-in-path like
# ACS/PEP, so there's no vintage to discover from listCensusApis(). It's
# released ~once/year; probe the current year and the two preceding it.
latest_saipe_year <- tryCatch({
  candidate_years <- as.integer(format(Sys.Date(), "%Y"))
  candidate_years <- candidate_years:(candidate_years - 2L)
  found <- NA_integer_
  for (yr in candidate_years) {
    test <- tryCatch(
      censusapi::getCensus(
        name = SAIPE_ENDPOINT, vars = "SAEMHI_PT",
        region = "state:01", time = as.character(yr), key = api_key
      ),
      error = function(e) NULL
    )
    if (!is.null(test) && nrow(test) > 0) { found <- yr; break }
  }
  found
}, error = function(e) {
  message("[WARN] Could not probe SAIPE API: ", conditionMessage(e))
  NA_integer_
})

if (!is.na(latest_saipe_year) &&
    (force_block("saipe") || is.null(process$saipe_year) ||
     process$saipe_year < latest_saipe_year)) {

  message("SAIPE latest year: ", latest_saipe_year)

  # SAIPE supports county, state, and national ("us:*") geography natively
  # -- no aggregation needed, just three fetches mixed into one file by
  # geography length, matching every other multi-level source in this
  # pipeline. National rows come back with a "us" column instead of
  # "state"/"county", so geography is built per level rather than from one
  # shared column set.
  fetch_saipe_level <- function(region_str, build_geography) {
    tryCatch({
      censusapi::getCensus(
        name   = SAIPE_ENDPOINT,
        vars   = c("SAEPOVRT0_17_PT", "SAEMHI_PT"),
        region = region_str,
        time   = as.character(latest_saipe_year),
        key    = api_key
      ) %>%
        mutate(
          geography                     = build_geography(.),
          time                          = paste0(latest_saipe_year, "-12-31"),
          # SAIPE publishes SAEPOVRT as a percentage. Divide to a proportion
          # so the single to_percent_scale() call below is the only place a
          # scale is applied; the two cancel and the stored value is 0-100.
          saipe_pct_children_poverty    = as.numeric(SAEPOVRT0_17_PT) / 100,
          saipe_median_household_income = as.numeric(SAEMHI_PT)
        ) %>%
        select(geography, time, saipe_pct_children_poverty, saipe_median_household_income)
    }, error = function(e) {
      message("  [WARN] SAIPE fetch failed (region=", region_str, "): ", conditionMessage(e))
      NULL
    })
  }

  saipe_result <- bind_rows(
    fetch_saipe_level("county:*", function(df) paste0(sprintf("%02d", as.integer(df$state)), sprintf("%03d", as.integer(df$county)))),
    fetch_saipe_level("state:*", function(df) sprintf("%02d", as.integer(df$state))),
    fetch_saipe_level("us:*", function(df) "00")
  )

  saipe_result <- to_percent_scale(saipe_result)
  if (!is.null(saipe_result) && nrow(saipe_result) > 0) {
    vroom::vroom_write(saipe_result, "standard/data_saipe.csv.gz", delim = ",")
    process$saipe_year <- latest_saipe_year
    dcf::dcf_process_record(updated = process)
    message("SAIPE data written for year ", latest_saipe_year, " (", nrow(saipe_result), " rows, county+state+national)")
  }
} else {
  message("SAIPE data is up to date (last year: ", process$saipe_year, ")")
}

# =============================================================================
# 2020 Census Operational Quality Metrics (OQM) — county-level self-response
# rate. Static, one-time dataset tied to the 2020 Census; won't refresh
# again until the 2030 Census releases its own OQM data. Feeds
# chr_census_participation in us-rates.
# Source: Release 4 (Oct 2022) county file. National total is already
# present as State="00"/County="000".
# =============================================================================

oqm_url      <- "https://www2.census.gov/programs-surveys/decennial/2020/data/operational-quality-metrics/census-operational-quality-metrics-release_4.xlsx"
oqm_raw_path <- "raw/census-operational-quality-metrics-release_4.xlsx"

if (!file.exists(oqm_raw_path)) {
  # This ~6MB file occasionally exceeds R's 60s default download timeout.
  old_timeout <- getOption("timeout")
  options(timeout = 120)
  download.file(oqm_url, oqm_raw_path, mode = "wb")
  options(timeout = old_timeout)
}
oqm_hash <- unname(tools::md5sum(oqm_raw_path))

if (force_block("oqm") || !identical(process$oqm_state, list(hash = oqm_hash)) ||
    !file.exists("standard/data_oqm.csv.gz")) {

  oqm_raw <- readxl::read_excel(oqm_raw_path, sheet = "County Metrics", skip = 1)

  oqm_result <- oqm_raw %>%
    rename(state = State, county = County, self_response = `Self-Response`) %>%
    filter(!is.na(state), !is.na(county)) %>%  # drops one blank trailing row in the source sheet
    mutate(
      geography = if_else(state == "00" & county == "000", "00", paste0(state, county)),
      time      = "2020-12-31",
      # Source uses "-" for counties where self-response wasn't computed
      # (e.g. Lake and Peninsula Borough, AK); coerces to NA as intended.
      oqm_self_response_rate = suppressWarnings(as.numeric(self_response))
    ) %>%
    select(geography, time, oqm_self_response_rate)

  oqm_result <- to_percent_scale(oqm_result)
  vroom::vroom_write(oqm_result, "standard/data_oqm.csv.gz", delim = ",")

  process$oqm_state <- list(hash = oqm_hash)
  dcf::dcf_process_record(updated = process)
  message("OQM data written (2020 Census, static)")
}

# =============================================================================
# SAHIE (Small Area Health Insurance Estimates) — annual county-level
# uninsured rate, overall and for two age subgroups.
# Source: U.S. Census Bureau SAHIE, "timeseries/healthins/sahie" API.
# AGECAT/IPRCAT/RACECAT/SEXCAT codes verified against this endpoint's own
# variable metadata (values enumerated directly, unlike PEP's AGE/SEX/HISP):
#   AGECAT 0 = Under 65, 1 = 18-64, 4 = Under 19
#   IPRCAT/RACECAT/SEXCAT 0 = All Incomes/Races/Sexes
# PCTUI_PT is a 0-100 percent; rescaled to this file's 0-1 convention.
# Feeds chr_uninsured, chr_uninsured_adults, chr_uninsured_children in
# us-rates.
# =============================================================================

SAHIE_ENDPOINT <- "timeseries/healthins/sahie"
SAHIE_AGECATS  <- c(sahie_pct_uninsured = "0", sahie_pct_uninsured_adults = "1",
                    sahie_pct_uninsured_children = "4")

# Same reasoning as SAIPE: a "time"-predicate timeseries dataset with no
# vintage to discover from listCensusApis(), released ~once/year.
latest_sahie_year <- tryCatch({
  candidate_years <- as.integer(format(Sys.Date(), "%Y"))
  candidate_years <- candidate_years:(candidate_years - 2L)
  found <- NA_integer_
  for (yr in candidate_years) {
    test <- tryCatch(
      censusapi::getCensus(
        name = SAHIE_ENDPOINT, vars = "PCTUI_PT",
        region = "state:01", time = as.character(yr),
        AGECAT = "0", IPRCAT = "0", RACECAT = "0", SEXCAT = "0", key = api_key
      ),
      error = function(e) NULL
    )
    if (!is.null(test) && nrow(test) > 0) { found <- yr; break }
  }
  found
}, error = function(e) {
  message("[WARN] Could not probe SAHIE API: ", conditionMessage(e))
  NA_integer_
})

if (!is.na(latest_sahie_year) &&
    (force_block("sahie") || is.null(process$sahie_year) ||
     process$sahie_year < latest_sahie_year)) {

  message("SAHIE latest year: ", latest_sahie_year)

  # SAHIE supports county, state, and national ("us:*") geography natively.
  # Each geography level is fetched and joined separately (join_cols/
  # build_geography differ per level -- national rows come back with a "us"
  # column, not "state"/"county"), then mixed into one file by geography
  # length like every other multi-level source here.
  fetch_sahie_level <- function(region_str, join_cols, build_geography) {
    fetch_sahie_agecat <- function(agecat_code) {
      tryCatch({
        censusapi::getCensus(
          name = SAHIE_ENDPOINT, vars = "PCTUI_PT",
          region = region_str, time = as.character(latest_sahie_year),
          AGECAT = agecat_code, IPRCAT = "0", RACECAT = "0", SEXCAT = "0", key = api_key
        ) %>%
          # PCTUI is published as a percentage; divided to a proportion here
          # for the same reason as SAIPE above, then restored by
          # to_percent_scale(). Net effect: stored on 0-100.
          transmute(across(all_of(join_cols)), value = as.numeric(PCTUI_PT) / 100)
      }, error = function(e) {
        message("  [WARN] SAHIE fetch failed (AGECAT=", agecat_code, ", region=", region_str, "): ", conditionMessage(e))
        NULL
      })
    }

    blocks <- Filter(Negate(is.null), lapply(names(SAHIE_AGECATS), function(nm) {
      df <- fetch_sahie_agecat(SAHIE_AGECATS[[nm]])
      if (!is.null(df)) rename(df, !!nm := value) else NULL
    }))
    if (length(blocks) == 0) return(NULL)

    Reduce(function(a, b) left_join(a, b, by = join_cols), blocks) %>%
      mutate(
        geography = build_geography(.),
        time      = paste0(latest_sahie_year, "-12-31")
      ) %>%
      select(geography, time, starts_with("sahie_"))
  }

  sahie_result <- bind_rows(
    fetch_sahie_level(
      "county:*", c("state", "county"),
      function(df) paste0(sprintf("%02d", as.integer(df$state)), sprintf("%03d", as.integer(df$county)))
    ),
    fetch_sahie_level("state:*", c("state"), function(df) sprintf("%02d", as.integer(df$state))),
    fetch_sahie_level("us:*", c("us"), function(df) "00")
  )

  sahie_result <- to_percent_scale(sahie_result)
  if (!is.null(sahie_result) && nrow(sahie_result) > 0) {
    vroom::vroom_write(sahie_result, "standard/data_sahie.csv.gz", delim = ",")
    process$sahie_year <- latest_sahie_year
    dcf::dcf_process_record(updated = process)
    message("SAHIE data written for year ", latest_sahie_year, " (", nrow(sahie_result), " rows, county+state+national)")
  }
} else {
  message("SAHIE data is up to date (last year: ", process$sahie_year, ")")
}

