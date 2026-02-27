# =============================================================================
# Census ACS 5-Year SDOH Data Ingestion
# Source: U.S. Census Bureau American Community Survey 5-Year Estimates
# Indicators adapted from the Metopio SDOH framework, with code courtesy of Heather Blonsky
#Due to the large size of the ZCTA-level files, the standard file is saved in a parquet directory.
#It can be read into memory using arrow::open_dataset("standard/data_zcta") %>% collect(). You can insert a filter as needed prior to collect()
#
# Outputs:
#   standard/data.csv.gz         -- combined all geographies, vintage years 2009-2024
#   standard/data_state.csv.gz   -- 2-digit FIPS, vintage years 2009-2024
#   standard/data_county.csv.gz  -- 5-digit FIPS, vintage years 2009-2024
#   standard/data_zcta.csv.gz    -- 5-digit ZCTA, vintage years 2009-2024
#
# Variable legend (all computed variables carry a "acs_" prefix)
#   Race/Ethnicity: W=Non-Hispanic White, B=Non-Hispanic Black, A=Asian,
#                   H=Hispanic/Latino, P1=Pacific Islander/Native Hawaiian,
#                   P=Native American, Q=Two or more races
#   Sex:            F=Female, M=Male
#   Age:            I=Infants 0-4, J=Juveniles 5-17, Y=Young Adults 18-39,
#                   O=Middle-Aged 40-64, S=Seniors 65+
#   Example:        acs_POP, acs_PCT_W, acs_AGE, acs_REX, etc.
# =============================================================================

#to edit API key:
#library("usethis")
#edit_r_environ()
##add
#CENSUS_API_KEY="XXXXXXXXXX"

library(dplyr)
library(vroom)
library(censusapi)

# -----------------------------------------------------------------------------
# Read Census API key
# -----------------------------------------------------------------------------

api_key <- Sys.getenv("CENSUS_API_KEY")

# -----------------------------------------------------------------------------
# Initialize process record
# -----------------------------------------------------------------------------
if (!file.exists("process.json")) {
  process <- list(last_vintage_year = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
ACS5         <- "acs/acs5"
ACS5_SUBJECT <- "acs/acs5/subject"

# ACS 5-year vintage years (each vintage = 5-year period ending that year)
# 2009 is the first available; 2024 is the most recent as of early 2026; schema was different before 2019
YEARS <- 2019:2024

# =============================================================================
# Core ingestion function: fetch all SDOH variables for one year + geo level
# Returns a data.frame with geography, time, and all computed indicators,
# or NULL if all API calls failed.
# =============================================================================
fetch_sdoh_year <- function(vintage_year, geo_level, api_key) {
  message("  Fetching ", geo_level, " ", vintage_year, "...")

  region <- switch(geo_level,
    "state"  = "state:*",
    "county" = "county:*",
    "zcta"   = "zip code tabulation area:*"
  )
  id_cols <- switch(geo_level,
    "state"  = "state",
    "county" = c("state", "county"),
    "zcta"   = "zcta"          # normalised name; Census returns "zip code tabulation area"
  )

  # Helper: call Census API, return NULL on error.
  # For ZCTA, immediately renames "zip code tabulation area" -> "zcta" so that
  # column names with spaces never reach dplyr join operations (which would
  # silently produce a cross-join when intersect() fails to match them).
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
    if (!is.null(df) && geo_level == "zcta") {
      # censusapi 0.9.0 returns "zip_code_tabulation_area" (underscores);
      # older versions may use spaces or dots.
      zcta_variants <- c("zip_code_tabulation_area",
                         "zip code tabulation area",
                         "zip.code.tabulation.area")
      hit <- zcta_variants[zcta_variants %in% names(df)]
      if (length(hit) > 0) names(df)[names(df) == hit[1]] <- "zcta"
    }
    df
  }

  # Helper: safe division — returns NA instead of Inf/NaN when denominator is 0
  safe_div <- function(num, denom) if_else(denom == 0, NA_real_, num / denom)

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
        acs_INL = B19082_001E,
        acs_INM = B19082_002E,
        acs_INN = B19082_003E,
        acs_INO = B19082_004E,
        acs_INP = B19082_005E,
        acs_INQ = B19082_006E,
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
  #   Subject table — may be unavailable for ZCTA or early years; fails silently
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
  # Standardize geography column to FIPS / ZCTA code
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
  } else if (geo_level == "zcta") {
    result <- result %>%
      rename(geography = zcta) %>%
      select(-any_of("state"))
  }

  # Annual time standard: YYYY-12-31
  result %>%
    mutate(time = paste0(vintage_year, "-12-31")) %>%
    select(geography, time, everything())
}

# =============================================================================
# Fetch all vintage years for one geography level
# Adds a geo_level column ("state", "county", or "zcta") to each row.
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
output_files  <- c("standard/data.csv.gz",
                   "standard/data_state.csv.gz",
                   "standard/data_county.csv.gz",
                   "standard/data_zcta.csv.gz")
output_exists <- all(file.exists(output_files))
last_vintage  <- process$last_vintage_year
current_max   <- max(YEARS)

if (!output_exists || is.null(last_vintage) || last_vintage < current_max) {

  # Fetch all geographies and combine into a single data frame
  data_all <- bind_rows(
    fetch_all_years("state",  api_key),
    fetch_all_years("county", api_key),
    fetch_all_years("zcta",   api_key)
  )

  if (nrow(data_all) > 0) {
    # Write combined file
    # Split combined file by geo_level into three separate files
    data_state  <- data_all %>%
                    filter(geo_level == "state") %>%
                    select(-geo_level)

    data_county <- data_all %>%
                    filter(geo_level == "county") %>%
                    select(-geo_level)

    data_zcta  <- data_all %>%
                    filter(geo_level == "zcta") %>%
                    select(-geo_level) %>%
                    rename(geography_zcta=geography)

      data_zcta_2019_2020  <- data_all %>%
                    filter(geo_level == "zcta" & time %in% c("2019-12-31", "2020-12-31")) %>%
                    select(-geo_level) %>%
                    rename(geography_zcta=geography)
      data_zcta_2021_2022  <- data_all %>%
                    filter(geo_level == "zcta" & time %in% c("2021-12-31", "2022-12-31")) %>%
                    select(-geo_level) %>%
                    rename(geography_zcta=geography)
      data_zcta_2023_2024  <- data_all %>%
                    filter(geo_level == "zcta" & time %in% c("2023-12-31", "2024-12-31")) %>%
                    select(-geo_level) %>%
                    rename(geography_zcta=geography)

   if (nrow(data_zcta_2019_2020) > 0) vroom::vroom_write(data_zcta_2019_2020, './standard/data_zcta_2019_2020.csv.gz')
   if (nrow(data_zcta_2021_2022) > 0) vroom::vroom_write(data_zcta_2021_2022, './standard/data_zcta_2021_2022.csv.gz')
   if (nrow(data_zcta_2023_2024) > 0) vroom::vroom_write(data_zcta_2023_2024, './standard/data_zcta_2023_2024.csv.gz')
   
   if (nrow(data_state) > 0) vroom::vroom_write(data_state, "standard/data_state.csv.gz", delim = ",")
   if (nrow(data_county) > 0) vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")
  }

  process$last_vintage_year <- current_max
  dcf::dcf_process_record(updated = process)

} else {
  message("Census SDOH data is up to date (last vintage: ", last_vintage, ")")
}
