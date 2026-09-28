# =============================================================================
# ACS 5-Year Estimates Data Ingestion
# Source: https://www.census.gov (ACS 5-year via Census API; urban/rural via
# the 2020 Census Urban Area to County Allocation File)
#
# Split out of data/census/ingest.R: this covers the "2024 American Community
# Survey 5-Year Estimates, Powered by Metopio" dataset (acs_* measures) plus
# the urban/rural classification (census_ur_* measures), which is appended to
# the same data_county.csv.gz file the ACS block writes. See README.md.
# ACS indicators adapted from the Metopio SDOH framework, code courtesy of
# Heather Blonsky.
#
# Variable legend (ACS computed variables, all carrying an "acs_" prefix)
#   Race/Ethnicity: W=Non-Hispanic White, B=Non-Hispanic Black, A=Asian,
#                   H=Hispanic/Latino, P1=Pacific Islander/Native Hawaiian,
#                   P=Native American, Q=Two or more races
#   Sex:            F=Female, M=Male
#   Age:            I=Infants 0-4, J=Juveniles 5-17, Y=Young Adults 18-39,
#                   O=Middle-Aged 40-64, S=Seniors 65+
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
# Converting at the point of DERIVATION and not at the point of writing is
# deliberate. The urban-rural block further down re-reads
# standard/data_county.csv.gz, swaps three columns and writes it back, and the
# ACS county file is written before that runs -- a write-time hook would
# multiply the already-converted ACS columns by 100 again on every such run.
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
# "all" or any of sdoh,ur to bypass those guards for one run.
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

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ACS5         <- "acs/acs5"
ACS5_SUBJECT <- "acs/acs5/subject"

FIRST_YEAR <- 2019L  # schema was different before 2019

# Census ACS tables (medians, aggregates) return these as literal values when
# an estimate could not be computed (e.g. sample too small), not as a true
# missing value. Left uncleaned, -666666666 would be read as a real acs_VAL,
# acs_AGE, acs_INC, etc. observation downstream.
ACS_NA_CODES <- c(-666666666, -999999999, -222222222, -333333333, -555555555, -888888888)

# Discover the latest available ACS 5-year vintage from the Census API
# discovery endpoint (api.census.gov/data.json) — no probing loop needed.
# Falls back to process$last_vintage_year, then FIRST_YEAR, if the call fails.
latest_vintage <- tryCatch({
  censusapi::listCensusApis() %>%
    filter(name == "acs/acs5") %>%
    pull(vintage) %>%
    as.integer() %>%
    max(na.rm = TRUE)
}, error = function(e) {
  message("[WARN] Could not fetch Census API metadata: ", conditionMessage(e))
  if (!is.null(process$last_vintage_year)) as.integer(process$last_vintage_year) else FIRST_YEAR
})
message("ACS latest vintage year: ", latest_vintage)

YEARS <- FIRST_YEAR:latest_vintage

# =============================================================================
# Core ingestion function: fetch all SDOH variables for one year + geo level
# Returns a data.frame with geography, time, and all computed indicators,
# or NULL if all API calls failed.
# =============================================================================
fetch_sdoh_year <- function(vintage_year, geo_level, api_key) {
  message("  Fetching ", geo_level, " ", vintage_year, "...")

  region <- switch(geo_level,
    "state"    = "state:*",
    "county"   = "county:*",
    "national" = "us:*"
  )
  id_cols <- switch(geo_level,
    "state"    = "state",
    "county"   = c("state", "county"),
    "national" = "us"
  )

  # Helper: call Census API, return NULL on error.
  safe_fetch <- function(endpoint, vars) {
    df <- tryCatch(
      censusapi::getCensus(
        name    = endpoint,
        vars    = vars,
        vintage = vintage_year,
        region  = region,
        key     = api_key
      ),
      error = function(e) {
        message("    [WARN] Failed (", endpoint, "): ", conditionMessage(e))
        NULL
      }
    )
    if (!is.null(df)) {
      value_cols <- intersect(vars, names(df))
      df[value_cols] <- lapply(df[value_cols], function(x) {
        x <- suppressWarnings(as.numeric(x))
        replace(x, x %in% ACS_NA_CODES, NA_real_)
      })
    }
    df
  }

  # Helper: safe division — returns NA instead of Inf/NaN when the denominator
  # is non-positive. Most denominators here are counts, where <0 is impossible,
  # but acs_OWS divides by B19081_001E (mean income of the lowest quintile),
  # which the ACS can report as negative where business losses dominate. A
  # negative denominator there yields a negative "inequality ratio", which is
  # meaningless, so it must be NA rather than a plausible-looking number.
  safe_div <- function(num, denom) if_else(denom <= 0, NA_real_, num / denom)

  # Helper: keep id columns + requested computed columns, drop NAME if present
  keep_cols <- function(df, computed) {
    df %>%
      select(any_of(c(id_cols, "NAME")), any_of(computed)) %>%
      select(-any_of("NAME"))
  }

  # ---------------------------------------------------------------------------
  # Block 1: Median age (AGE), broadband (BDB), birth rate (BTH),
  #          opportunity youth (DCY), HS graduation (EDB), higher ed (EDC),
  #          Gini index (GNI), group quarters (GRP),
  #          severe housing cost burden (HBS), housing cost burden (HBU)
  # ---------------------------------------------------------------------------
  raw1 <- safe_fetch(ACS5, c(
    "B01002_001E",                                                    # median age
    "B28002_001E", "B28002_004E",                                     # broadband
    "B13002_001E", "B13002_002E",                                     # birth rate
    "B14005_001E", "B14005_010E", "B14005_011E", "B14005_014E",       # opportunity youth
    "B14005_015E", "B14005_024E", "B14005_025E", "B14005_028E",
    "B14005_029E",
    "B15002_001E", "B15002_011E", "B15002_012E", "B15002_013E",       # educational attainment
    "B15002_014E", "B15002_015E", "B15002_016E", "B15002_017E",
    "B15002_018E", "B15002_028E", "B15002_029E", "B15002_030E",
    "B15002_031E", "B15002_032E", "B15002_033E", "B15002_034E",
    "B15002_035E",
    "B19083_001E",                                                    # Gini index
    "B26001_001E", "B01001_001E",                                     # group quarters
    "B25091_001E", "B25091_008E", "B25091_009E", "B25091_010E",       # owner housing costs (30%+)
    "B25091_011E", "B25091_019E", "B25091_020E", "B25091_021E",
    "B25091_022E",
    "B25070_001E", "B25070_007E", "B25070_008E", "B25070_009E",       # renter housing costs (30%+)
    "B25070_010E"
  ))

  b1 <- if (!is.null(raw1)) {
    raw1 %>%
      mutate(
        acs_AGE = B01002_001E,
        acs_BDB = safe_div(B28002_004E, B28002_001E),
        acs_BTH = safe_div(B13002_002E, B13002_001E),
        acs_DCY = safe_div(B14005_010E + B14005_011E + B14005_014E + B14005_015E +
                 B14005_024E + B14005_025E + B14005_028E + B14005_029E, B14005_001E),
        acs_EDB = safe_div(B15002_011E + B15002_012E + B15002_013E + B15002_014E +
                 B15002_015E + B15002_016E + B15002_017E + B15002_018E +
                 B15002_028E + B15002_029E + B15002_030E + B15002_031E +
                 B15002_032E + B15002_033E + B15002_034E + B15002_035E, B15002_001E),
        acs_EDC = safe_div(B15002_012E + B15002_013E + B15002_014E + B15002_015E +
                 B15002_016E + B15002_017E + B15002_018E + B15002_029E +
                 B15002_030E + B15002_031E + B15002_032E + B15002_033E +
                 B15002_034E + B15002_035E, B15002_001E),
        acs_GNI = B19083_001E,
        acs_GRP = safe_div(B26001_001E, B01001_001E),
        # Severe burden (50%+): renters + mortgaged owners + non-mortgaged owners
        acs_HBS = safe_div(B25070_010E + B25091_011E + B25091_022E,
              B25070_001E + B25091_001E),
        # Any burden (30%+): renters + mortgaged owners + non-mortgaged owners
        # Note: B25091_019E-022E = non-mortgaged owner 30%+ bands
        acs_HBU = safe_div(B25070_007E + B25070_008E + B25070_009E + B25070_010E +
                 B25091_008E + B25091_009E + B25091_010E + B25091_011E +
                 B25091_019E + B25091_020E + B25091_021E + B25091_022E,
              B25070_001E + B25091_001E)
      ) %>%
      keep_cols(c("acs_AGE", "acs_BDB", "acs_BTH", "acs_DCY", "acs_EDB", "acs_EDC",
                  "acs_GNI", "acs_GRP", "acs_HBS", "acs_HBU"))
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 2: Population by sex (POP_M/F) and age group (POP_I/J/Y/O/S),
  #          sex/age share (PCT_*), age dependency ratio (DEP)
  # ---------------------------------------------------------------------------
  raw2 <- safe_fetch(ACS5, c(
    "B01001_001E",                                                    # total
    "B01001_002E", "B01001_026E",                                     # male, female
    "B01001_003E", "B01001_027E",                                     # infants 0-4
    "B01001_004E", "B01001_005E", "B01001_006E",                      # juveniles 5-17
    "B01001_028E", "B01001_029E", "B01001_030E",
    "B01001_007E", "B01001_008E", "B01001_009E", "B01001_010E",       # young adults 18-39
    "B01001_011E", "B01001_012E", "B01001_013E",
    "B01001_031E", "B01001_032E", "B01001_033E", "B01001_034E",
    "B01001_035E", "B01001_036E", "B01001_037E",
    "B01001_014E", "B01001_015E", "B01001_016E", "B01001_017E",       # middle-aged 40-64
    "B01001_018E", "B01001_019E",
    "B01001_038E", "B01001_039E", "B01001_040E", "B01001_041E",
    "B01001_042E", "B01001_043E",
    "B01001_020E", "B01001_021E", "B01001_022E", "B01001_023E",       # seniors 65+
    "B01001_024E", "B01001_025E",
    "B01001_044E", "B01001_045E", "B01001_046E", "B01001_047E",
    "B01001_048E", "B01001_049E"
  ))

  b2 <- if (!is.null(raw2)) {
    raw2 %>%
      mutate(
        acs_POP   = B01001_001E,
        acs_POP_M = B01001_002E,
        acs_POP_F = B01001_026E,
        acs_POP_I = B01001_003E + B01001_027E,
        acs_POP_J = B01001_004E + B01001_005E + B01001_006E +
                B01001_028E + B01001_029E + B01001_030E,
        acs_POP_Y = B01001_007E + B01001_008E + B01001_009E + B01001_010E +
                B01001_011E + B01001_012E + B01001_013E +
                B01001_031E + B01001_032E + B01001_033E + B01001_034E +
                B01001_035E + B01001_036E + B01001_037E,
        acs_POP_O = B01001_014E + B01001_015E + B01001_016E + B01001_017E +
                B01001_018E + B01001_019E +
                B01001_038E + B01001_039E + B01001_040E + B01001_041E +
                B01001_042E + B01001_043E,
        acs_POP_S = B01001_020E + B01001_021E + B01001_022E + B01001_023E +
                B01001_024E + B01001_025E +
                B01001_044E + B01001_045E + B01001_046E + B01001_047E +
                B01001_048E + B01001_049E
      ) %>%
      mutate(
        acs_PCT_M = safe_div(acs_POP_M, acs_POP),
        acs_PCT_F = safe_div(acs_POP_F, acs_POP),
        acs_PCT_I = safe_div(acs_POP_I, acs_POP),
        acs_PCT_J = safe_div(acs_POP_J, acs_POP),
        acs_PCT_Y = safe_div(acs_POP_Y, acs_POP),
        acs_PCT_O = safe_div(acs_POP_O, acs_POP),
        acs_PCT_S = safe_div(acs_POP_S, acs_POP),
        acs_DEP   = safe_div(acs_POP_I + acs_POP_J + acs_POP_S, acs_POP_Y + acs_POP_O)
      ) %>%
      keep_cols(c("acs_POP", "acs_POP_M", "acs_POP_F", "acs_POP_I", "acs_POP_J", "acs_POP_Y", "acs_POP_O", "acs_POP_S",
                  "acs_PCT_M", "acs_PCT_F", "acs_PCT_I", "acs_PCT_J", "acs_PCT_Y", "acs_PCT_O", "acs_PCT_S", "acs_DEP"))
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 3: Population by race/ethnicity (POP_W/B/A/H/P/P1/Q),
  #          race shares (PCT_*), diversity index (REX)
  # ---------------------------------------------------------------------------
  raw3 <- safe_fetch(ACS5, c(
    "B03002_001E",   # total
    "B03002_003E",   # Non-Hispanic White
    "B03002_004E",   # Non-Hispanic Black
    "B03002_005E",   # Native American
    "B03002_006E",   # Asian
    "B03002_007E",   # Pacific Islander / Native Hawaiian
    "B03002_009E",   # Two or more races
    "B03002_012E"    # Hispanic or Latino
  ))

  b3 <- if (!is.null(raw3)) {
    raw3 %>%
      mutate(
        acs_POP_W  = B03002_003E,
        acs_POP_B  = B03002_004E,
        acs_POP_P  = B03002_005E,
        acs_POP_A  = B03002_006E,
        acs_POP_P1 = B03002_007E,
        acs_POP_Q  = B03002_009E,
        acs_POP_H  = B03002_012E
      ) %>%
      mutate(
        acs_PCT_W  = safe_div(acs_POP_W,  B03002_001E),
        acs_PCT_B  = safe_div(acs_POP_B,  B03002_001E),
        acs_PCT_P  = safe_div(acs_POP_P,  B03002_001E),
        acs_PCT_A  = safe_div(acs_POP_A,  B03002_001E),
        acs_PCT_P1 = safe_div(acs_POP_P1, B03002_001E),
        acs_PCT_Q  = safe_div(acs_POP_Q,  B03002_001E),
        acs_PCT_H  = safe_div(acs_POP_H,  B03002_001E),
        # Herfindahl-based diversity index (1 = maximally diverse)
        acs_REX = 1 - (acs_PCT_W^2 + acs_PCT_B^2 + acs_PCT_P^2 + acs_PCT_A^2 +
                   acs_PCT_P1^2 + acs_PCT_Q^2 + acs_PCT_H^2)
      ) %>%
      keep_cols(c("acs_POP_W", "acs_POP_B", "acs_POP_P", "acs_POP_A", "acs_POP_P1", "acs_POP_Q", "acs_POP_H",
                  "acs_PCT_W", "acs_PCT_B", "acs_PCT_P", "acs_PCT_A", "acs_PCT_P1", "acs_PCT_Q", "acs_PCT_H",
                  "acs_REX"))
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 4: Housing, poverty, income indicators
  #   HTA=single-parent HH, HTJ=crowded housing, HUF=lacking plumbing,
  #   HUG=no telephone, HUN=mobile home, HUO=owner-occupied,
  #   POV=poverty rate, PUB=public transit to work,
  #   PVA=deep poverty (<50% FPL), PVB=<150% FPL, PVC=<200% FPL,
  #   SNP=SNAP/food stamps, VAL=median home value, WWN=no internet,
  #   INB=median worker earnings, INC=median HH income, PCI=per capita income,
  #   INL-INQ=income quintile shares, OWS=S80/S20 quintile ratio
  # ---------------------------------------------------------------------------
  raw4 <- safe_fetch(ACS5, c(
    "B11012_001E", "B11012_010E", "B11012_015E",                      # single-parent HH
    "B25014_001E", "B25014_005E", "B25014_006E", "B25014_007E",       # crowded housing
    "B25014_011E", "B25014_012E", "B25014_013E",
    "B25048_001E", "B25048_003E",                                     # lacking plumbing
    "B25043_001E", "B25043_007E", "B25043_016E",                      # no telephone
    "B25024_001E", "B25024_010E", "B25024_011E",                      # mobile homes
    "B25003_001E", "B25003_002E",                                     # owner-occupied
    "B25077_001E",                                                    # median home value
    "B22003_001E", "B22003_002E",                                     # SNAP / food stamps
    "B28002_001E", "B28002_013E",                                     # no internet
    "B08301_001E", "B08301_010E",                                     # public transit
    "B17001_001E", "B17001_002E",                                     # poverty
    "C17002_001E", "C17002_002E", "C17002_003E", "C17002_004E",       # poverty thresholds
    "C17002_005E", "C17002_006E", "C17002_007E",
    "B20017_001E",                                                    # median worker earnings
    "B19013_001E",                                                    # median HH income
    "B19301_001E",                                                    # per capita income
    "B19082_001E", "B19082_002E", "B19082_003E",                      # income quintile shares
    "B19082_004E", "B19082_005E", "B19082_006E",
    "B19081_001E", "B19081_005E"                                      # S80/S20 ratio
  ))

  b4 <- if (!is.null(raw4)) {
    raw4 %>%
      mutate(
        acs_HTA = safe_div(B11012_010E + B11012_015E, B11012_001E),
        acs_HTJ = safe_div(B25014_005E + B25014_006E + B25014_007E +
                 B25014_011E + B25014_012E + B25014_013E, B25014_001E),
        acs_HUF = safe_div(B25048_003E, B25048_001E),
        acs_HUG = safe_div(B25043_007E + B25043_016E, B25043_001E),
        acs_HUN = safe_div(B25024_010E + B25024_011E, B25024_001E),
        acs_HUO = safe_div(B25003_002E, B25003_001E),
        acs_POV = safe_div(B17001_002E, B17001_001E),
        acs_PUB = safe_div(B08301_010E, B08301_001E),
        acs_PVA = safe_div(C17002_002E, C17002_001E),
        acs_PVB = safe_div(C17002_002E + C17002_003E + C17002_004E + C17002_005E, C17002_001E),
        acs_PVC = safe_div(C17002_002E + C17002_003E + C17002_004E + C17002_005E +
                 C17002_006E + C17002_007E, C17002_001E),
        acs_SNP = safe_div(B22003_002E, B22003_001E),
        acs_VAL = B25077_001E,
        acs_WWN = safe_div(B28002_013E, B28002_001E),
        acs_INB = B20017_001E,
        acs_INC = B19013_001E,
        acs_PCI = B19301_001E,
        # B19082 reports the aggregate income share held by each household
        # quintile as a percentage (0-100), unlike every other rate in this
        # script, which safe_div() leaves as a 0-1 proportion. Divide by 100 to
        # put it on the same internal footing as its siblings, so the single
        # to_percent_scale() call below is the ONE place a scale is applied.
        # The two operations cancel, which is intended: these six measures end
        # up back on the 0-100 scale B19082 publishes them on.
        #
        # An earlier version of this comment justified the division by "the 0-1
        # convention PopHIVE uses for all Percent measures". That convention did
        # not exist -- measured across Ingest, 169 percent measures stored 0-100
        # against 126 on 0-1. The standard is 0-100. Do not cite the old claim.
        acs_INL = B19082_001E / 100,
        acs_INM = B19082_002E / 100,
        acs_INN = B19082_003E / 100,
        acs_INO = B19082_004E / 100,
        acs_INP = B19082_005E / 100,
        acs_INQ = B19082_006E / 100,
        acs_OWS = safe_div(B19081_005E, B19081_001E)
      ) %>%
      keep_cols(c("acs_HTA", "acs_HTJ", "acs_HUF", "acs_HUG", "acs_HUN", "acs_HUO", "acs_POV", "acs_PUB",
                  "acs_PVA", "acs_PVB", "acs_PVC", "acs_SNP", "acs_VAL", "acs_WWN",
                  "acs_INB", "acs_INC", "acs_PCI", "acs_INL", "acs_INM", "acs_INN", "acs_INO", "acs_INP", "acs_INQ", "acs_OWS"))
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 5: Limited English proficiency (LEQ)
  # ---------------------------------------------------------------------------
  raw5 <- safe_fetch(ACS5, c(
    "B16004_001E",
    "B16004_006E",  "B16004_007E",  "B16004_008E",  "B16004_011E",
    "B16004_012E",  "B16004_013E",  "B16004_016E",  "B16004_017E",
    "B16004_018E",  "B16004_021E",  "B16004_022E",  "B16004_023E",
    "B16004_028E",  "B16004_029E",  "B16004_030E",  "B16004_033E",
    "B16004_034E",  "B16004_035E",  "B16004_038E",  "B16004_039E",
    "B16004_040E",  "B16004_043E",  "B16004_044E",  "B16004_045E",
    "B16004_050E",  "B16004_051E",  "B16004_052E",  "B16004_055E",
    "B16004_056E",  "B16004_057E",  "B16004_060E",  "B16004_061E",
    "B16004_062E",  "B16004_065E",  "B16004_066E",  "B16004_067E"
  ))

  b5 <- if (!is.null(raw5)) {
    raw5 %>%
      mutate(
        acs_LEQ = safe_div(B16004_006E + B16004_007E + B16004_008E + B16004_011E +
                 B16004_012E + B16004_013E + B16004_016E + B16004_017E +
                 B16004_018E + B16004_021E + B16004_022E + B16004_023E +
                 B16004_028E + B16004_029E + B16004_030E + B16004_033E +
                 B16004_034E + B16004_035E + B16004_038E + B16004_039E +
                 B16004_040E + B16004_043E + B16004_044E + B16004_045E +
                 B16004_050E + B16004_051E + B16004_052E + B16004_055E +
                 B16004_056E + B16004_057E + B16004_060E + B16004_061E +
                 B16004_062E + B16004_065E + B16004_066E + B16004_067E, B16004_001E)
      ) %>%
      keep_cols("acs_LEQ")
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 6: Uninsured rate (UNS)
  # ---------------------------------------------------------------------------
  raw6 <- safe_fetch(ACS5, c(
    "B27001_001E",
    "B27001_005E",  "B27001_008E",  "B27001_011E",  "B27001_014E",
    "B27001_017E",  "B27001_020E",  "B27001_023E",  "B27001_026E",
    "B27001_029E",  "B27001_033E",  "B27001_036E",  "B27001_039E",
    "B27001_042E",  "B27001_045E",  "B27001_048E",  "B27001_051E",
    "B27001_054E",  "B27001_057E"
  ))

  b6 <- if (!is.null(raw6)) {
    raw6 %>%
      mutate(
        acs_UNS = safe_div(B27001_005E + B27001_008E + B27001_011E + B27001_014E +
                 B27001_017E + B27001_020E + B27001_023E + B27001_026E +
                 B27001_029E + B27001_033E + B27001_036E + B27001_039E +
                 B27001_042E + B27001_045E + B27001_048E + B27001_051E +
                 B27001_054E + B27001_057E, B27001_001E)
      ) %>%
      keep_cols("acs_UNS")
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 7+8: Unemployment rate (UMP)
  #   Two API calls: unemployed civilians / civilians in labor force
  # ---------------------------------------------------------------------------
  raw7 <- safe_fetch(ACS5, c(
    "B23001_008E",  "B23001_015E",  "B23001_022E",  "B23001_029E",
    "B23001_036E",  "B23001_043E",  "B23001_050E",  "B23001_057E",
    "B23001_064E",  "B23001_071E",  "B23001_076E",  "B23001_081E",
    "B23001_086E",  "B23001_094E",  "B23001_101E",  "B23001_108E",
    "B23001_115E",  "B23001_122E",  "B23001_129E",  "B23001_136E",
    "B23001_143E",  "B23001_150E",  "B23001_157E",  "B23001_162E",
    "B23001_167E",  "B23001_172E"
  ))

  raw8 <- safe_fetch(ACS5, c(
    "B23001_006E",  "B23001_013E",  "B23001_020E",  "B23001_027E",
    "B23001_034E",  "B23001_041E",  "B23001_048E",  "B23001_055E",
    "B23001_062E",  "B23001_069E",  "B23001_074E",  "B23001_079E",
    "B23001_084E",  "B23001_092E",  "B23001_099E",  "B23001_106E",
    "B23001_113E",  "B23001_120E",  "B23001_127E",  "B23001_134E",
    "B23001_141E",  "B23001_148E",  "B23001_155E",  "B23001_160E",
    "B23001_165E",  "B23001_170E"
  ))

  b7 <- if (!is.null(raw7) && !is.null(raw8)) {
    ump_n <- raw7 %>%
      mutate(UMP_n = B23001_008E + B23001_015E + B23001_022E + B23001_029E +
                     B23001_036E + B23001_043E + B23001_050E + B23001_057E +
                     B23001_064E + B23001_071E + B23001_076E + B23001_081E +
                     B23001_086E + B23001_094E + B23001_101E + B23001_108E +
                     B23001_115E + B23001_122E + B23001_129E + B23001_136E +
                     B23001_143E + B23001_150E + B23001_157E + B23001_162E +
                     B23001_167E + B23001_172E) %>%
      select(any_of(id_cols), UMP_n)

    ump_d <- raw8 %>%
      mutate(UMP_d = B23001_006E + B23001_013E + B23001_020E + B23001_027E +
                     B23001_034E + B23001_041E + B23001_048E + B23001_055E +
                     B23001_062E + B23001_069E + B23001_074E + B23001_079E +
                     B23001_084E + B23001_092E + B23001_099E + B23001_106E +
                     B23001_113E + B23001_120E + B23001_127E + B23001_134E +
                     B23001_141E + B23001_148E + B23001_155E + B23001_160E +
                     B23001_165E + B23001_170E) %>%
      select(any_of(id_cols), UMP_d)

    left_join(ump_n, ump_d, by = intersect(names(ump_n), id_cols)) %>%
      mutate(acs_UMP = safe_div(UMP_n, UMP_d)) %>%
      select(-UMP_n, -UMP_d)
  } else NULL

  # ---------------------------------------------------------------------------
  # Block 9: Disability (DIS), Medicaid (MCD), Medicare (MCR)
  #   Subject table — may be unavailable for early years; fails silently
  # ---------------------------------------------------------------------------
  raw9 <- safe_fetch(ACS5_SUBJECT, c(
    "S1810_C01_001E", "S1810_C02_001E",      # disability
    "S2704_C01_001E", "S2704_C02_002E", "S2704_C02_006E"  # Medicare / Medicaid
  ))

  b9 <- if (!is.null(raw9)) {
    raw9 %>%
      mutate(
        acs_DIS = safe_div(S1810_C02_001E, S1810_C01_001E),
        acs_MCR = safe_div(S2704_C02_002E, S2704_C01_001E),
        acs_MCD = safe_div(S2704_C02_006E, S2704_C01_001E)
      ) %>%
      keep_cols(c("acs_DIS", "acs_MCR", "acs_MCD"))
  } else NULL

  # ---------------------------------------------------------------------------
  # Join all blocks on identifier columns
  # ---------------------------------------------------------------------------
  blocks <- Filter(Negate(is.null), list(b1, b2, b3, b4, b5, b6, b7, b9))
  if (length(blocks) == 0) {
    message("  [WARN] All API calls failed for ", geo_level, " ", vintage_year)
    return(NULL)
  }

  result <- Reduce(
    function(a, b) left_join(a, b, by = intersect(names(a), id_cols)),
    blocks
  )

  # ---------------------------------------------------------------------------
  # Standardize geography column to FIPS code
  # ---------------------------------------------------------------------------
  if (geo_level == "state") {
    result <- result %>%
      mutate(geography = sprintf("%02d", as.integer(state))) %>%
      select(-state)
  } else if (geo_level == "county") {
    result <- result %>%
      mutate(geography = paste0(
        sprintf("%02d", as.integer(state)),
        sprintf("%03d", as.integer(county))
      )) %>%
      select(-state, -county)
  } else if (geo_level == "national") {
    result <- result %>%
      mutate(geography = "00") %>%
      select(-any_of("us"))
  }

  # Annual time standard: YYYY-12-31
  result %>%
    mutate(time = paste0(vintage_year, "-12-31")) %>%
    select(geography, time, everything())
}

# =============================================================================
# Fetch all vintage years for one geography level
# Adds a geo_level column ("state", "county", or "national") to each row.
# =============================================================================
fetch_all_years <- function(geo_level, api_key, years = YEARS) {
  message("=== ", toupper(geo_level), " ===")
  results <- lapply(years, function(yr) fetch_sdoh_year(yr, geo_level, api_key))
  df <- bind_rows(Filter(Negate(is.null), results))
  if (nrow(df) > 0) df <- mutate(df, geo_level = geo_level)
  df
}

# =============================================================================
# Run ingest when output files are absent or a new vintage year is available
# =============================================================================
output_files  <- c("standard/data_state.csv.gz",
                   "standard/data_county.csv.gz")
output_exists <- all(file.exists(output_files))
last_vintage  <- process$last_vintage_year

if (force_block("sdoh") || !output_exists || is.null(last_vintage) ||
    last_vintage < latest_vintage) {

  # Fetch all geographies and combine into a single data frame
  data_all <- bind_rows(
    fetch_all_years("state",    api_key),
    fetch_all_years("national", api_key),
    fetch_all_years("county",   api_key)
  )

  if (nrow(data_all) > 0) {
    # Split combined file by geo_level into two separate files.
    # "national" rows (single geography "00") are folded into the state
    # file, not written separately -- same convention as
    # county_health_rankings and bls_laus's data_state.csv.gz.
    data_state  <- data_all %>%
                    filter(geo_level %in% c("state", "national")) %>%
                    select(-geo_level) %>%
                    to_percent_scale()

    data_county <- data_all %>%
                    filter(geo_level == "county") %>%
                    select(-geo_level) %>%
                    to_percent_scale()

   if (nrow(data_state) > 0) vroom::vroom_write(data_state, "standard/data_state.csv.gz", delim = ",")
   if (nrow(data_county) > 0) vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")
  }

  process$last_vintage_year <- latest_vintage
  dcf::dcf_process_record(updated = process)

} else {
  message("ACS SDOH data is up to date (last vintage: ", last_vintage, ")")
}

# =============================================================================
# Urban-Rural Classification (2020 Census UA-to-County allocation file)
# Source: https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx
# Appended as new columns to standard/data_county.csv.gz.
# =============================================================================

ur_url      <- "https://www2.census.gov/geo/docs/reference/ua/2020_UA_COUNTY.xlsx"
ur_raw_path <- "raw/2020_UA_COUNTY.xlsx"

if (!file.exists(ur_raw_path)) {
  download.file(ur_url, ur_raw_path, mode = "wb")
}
ur_hash <- unname(tools::md5sum(ur_raw_path))

county_file     <- "standard/data_county.csv.gz"
ur_cols_present <- file.exists(county_file) &&
  "census_ur_pct_urban_pop" %in% names(
    vroom::vroom(county_file, n_max = 1, show_col_types = FALSE)
  )

if (force_block("ur") || !identical(process$ur_state, list(hash = ur_hash)) ||
    !ur_cols_present) {
  ur_raw <- readxl::read_excel(ur_raw_path)

  # One row per county; STATE + COUNTY give the 5-digit FIPS. Values are
  # already in 0-1 proportion scale (fully rural counties are present with 0).
  ur_county <- ur_raw %>%
    mutate(geography = paste0(STATE, COUNTY)) %>%
    select(geography,
      census_ur_pct_urban_pop  = POPPCT_URB,
      census_ur_pct_urban_land = ALAND_PCT_URB,
      census_ur_pct_urban_hu   = HOUPCT_URB
    ) %>%
    to_percent_scale()

  if (file.exists(county_file)) {
    data_county <- vroom::vroom(county_file, show_col_types = FALSE) %>%
      select(-any_of(c("census_ur_pct_urban_pop", "census_ur_pct_urban_land", "census_ur_pct_urban_hu"))) %>%
      left_join(ur_county, by = "geography")
    vroom::vroom_write(data_county, county_file, delim = ",")
    message("Urban-rural classification joined to ", county_file)
  }

  process$ur_state <- list(hash = ur_hash)
  dcf::dcf_process_record(updated = process)
}
