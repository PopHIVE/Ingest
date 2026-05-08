# ==============================================================================
# ABCs Census Population Estimates: 1998–2008 (Pre-ACS era)
#
# Extends abcs_census_age_stratified_pop.csv back to 1998 using:
#
#   2000–2009: Census Bureau intercensal county/state estimates
#              (2000–2010 intercensal series, downloaded as CSV per state)
#              URL: https://www2.census.gov/programs-surveys/popest/datasets/
#                   2000-2010/intercensal/county/co-est00int-agesex-{ss}.csv
#
#   1998–1999: 2000 decennial census (SF1 table P012: Sex by Age) used as a
#              close approximation. Population at 1998/1999 differs from 2000
#              by <3% for most counties; acceptable for surveillance denominators.
#              For higher precision, provide 1990 decennial data and uncomment
#              the interpolation block near the bottom of this script.
#
# Age group mapping from 5-year Census bins:
#   0-4:   AGEGRP=1
#   5-17:  AGEGRP=2 (5-9) + AGEGRP=3 (10-14) + 3/5 * AGEGRP=4 (15-19)
#   18-49: 2/5 * AGEGRP=4 + AGEGRP=5..10 (20-49)
#   50-64: AGEGRP=11..13 (50-64)
#   65+:   AGEGRP=14..18
# Note: the 15-19 split uses a uniform-within-group assumption (3 of 5 years
# fall in 15-17, 2 of 5 in 18-19). Decennial data used for 1998-1999 has an
# exact 15-17 / 18-19 split from table P012 (no approximation needed there).
#
# Run from: data/abcs/
# Output:   raw/abcs_census_age_stratified_pop_pre2009.csv
# Combine with raw/abcs_census_age_stratified_pop.csv for a full 1998–2022 series.
# ==============================================================================

library(dplyr)
library(tidyr)
library(purrr)
library(vroom)
library(tidycensus)

# ==============================================================================
# ABCs surveillance areas (identical to census_pop_validation.R)
# ==============================================================================

make_rows <- function(state, years, fips_vec, ag) {
  if (length(ag) == 1) ag <- rep(ag, length(fips_vec))
  crossing(tibble(state = state, year = years),
           tibble(county_fips = fips_vec, age_group = ag))
}

abcs_areas <- bind_rows(
  ## California
  make_rows("CA", 1994,      c("06075","06013","06001"), "total"),
  make_rows("CA", 1996:1999, "06075",                   "total"),
  make_rows("CA", 2000:2010, "06075",                   "total"),
  make_rows("CA", 2000:2010, c("06013","06001"),        "under5"),
  make_rows("CA", 2011:2014, "06075",                   "total"),
  make_rows("CA", 2011:2014, c("06013","06001"),        "under18"),
  make_rows("CA", 2015:2016, "06075",                   "total"),
  make_rows("CA", 2015:2016, c("06013","06001"),        "under18_65plus"),
  make_rows("CA", 2017:2023, c("06075","06013","06001"), "total"),

  ## Colorado
  make_rows("CO", 2000:2023,
            c("08001","08005","08031","08035","08059"), "total"),

  ## Connecticut (statewide)
  make_rows("CT", 1995:2023, "09", "total"),

  ## Georgia
  make_rows("GA", 1997:2023,
            c("13013","13015","13045","13057","13063",
              "13067","13077","13089","13097","13113",
              "13117","13121","13135","13151","13217",
              "13223","13227","13247","13255","13297"), "total"),

  ## Maryland
  make_rows("MD", 1995:2023,
            c("24003","24005","24510","24013","24025","24027"), "total"),

  ## Minnesota
  make_rows("MN", 1995:2001,
            c("27003","27019","27037","27053","27123","27139","27163"), "total"),
  make_rows("MN", 2002:2023, "27", "total"),

  ## New Mexico (statewide)
  make_rows("NM", 2004:2023, "35", "total"),

  ## New York
  make_rows("NY", 1997:1998,
            c("36037","36051","36055","36069","36073","36117","36123"), "total"),
  make_rows("NY", 1999:2004,
            c("36001","36021","36037","36039","36051","36055","36057",
              "36069","36073","36083","36091","36093","36095","36117","36123"), "total"),
  make_rows("NY", 2005:2023,
            c("36001","36021","36037","36039","36051","36055","36057",
              "36069","36073","36083","36091","36093","36095","36117","36123"), "total"),
  make_rows("NY", 2005:2023, "36029", "under5"),

  ## Oklahoma (historical only)
  make_rows("OK", 1989:1994, "40", "total"),

  ## Oregon
  make_rows("OR", 2004:2023, c("41005","41051","41067"), "total"),

  ## Tennessee
  make_rows("TN", 1989:1994,
            c("47037","47065","47093","47157"), "total"),
  make_rows("TN", 1995:1998,
            c("47037","47065","47093","47157","47187"), "total"),
  make_rows("TN", 1999:2009,
            c("47021","47037","47043","47065","47093",
              "47147","47149","47157","47165","47187","47189"), "total"),
  make_rows("TN", 2010:2023,
            c("47001","47009","47021","47037","47043","47057","47065","47089",
              "47093","47105","47113","47145","47147","47149","47155","47157",
              "47165","47173","47187","47189"), "total"),

  ## Texas (historical only)
  make_rows("TX", 1995:1996, "48029", "total")
)

HIST_YEARS <- 1998:2008

# ==============================================================================
# SECTION A: 2000–2009 via Census intercensal estimates
# ==============================================================================

# Identify needed geographies
needed_2000_2008 <- abcs_areas %>%
  filter(year %in% 2000:2008) %>%
  distinct(county_fips) %>%
  mutate(
    geo_type   = if_else(nchar(county_fips) == 2, "state", "county"),
    state_fips = substr(county_fips, 1, 2)
  )

# Age mapping: Census 5-year AGEGRP -> standard groups
# AGEGRP=4 (15-19) is split: 3/5 to "5-17", 2/5 to "18-49"
age_mapping_intercensal <- bind_rows(
  tibble(AGEGRP = 1L,        age = "0-4",   weight = 1),
  tibble(AGEGRP = 2L,        age = "5-17",  weight = 1),   # 5-9
  tibble(AGEGRP = 3L,        age = "5-17",  weight = 1),   # 10-14
  tibble(AGEGRP = 4L,        age = "5-17",  weight = 3/5), # 15-17 portion of 15-19
  tibble(AGEGRP = 4L,        age = "18-49", weight = 2/5), # 18-19 portion of 15-19
  tibble(AGEGRP = 5L:10L,    age = "18-49", weight = 1),   # 20-49
  tibble(AGEGRP = 11L:13L,   age = "50-64", weight = 1),   # 50-64
  tibble(AGEGRP = 14L:18L,   age = "65+",   weight = 1)    # 65+
)

# Helper: parse the wide intercensal format into long county_fips/year/AGEGRP/pop
# The national county file co-est00int-agesex-5yr.csv contains both county
# records (SUMLEV=50) and state summary records (SUMLEV=40); geo_type selects which.
parse_intercensal_wide <- function(raw, geo_type) {
  if (geo_type == "county") {
    raw <- raw %>%
      filter(SEX == 0, AGEGRP > 0, SUMLEV == 50) %>%
      mutate(county_fips = paste0(
        formatC(STATE,  width = 2, flag = "0"),
        formatC(COUNTY, width = 3, flag = "0")
      ))
  } else {
    raw <- raw %>%
      filter(SEX == 0, AGEGRP > 0, SUMLEV == 40) %>%
      mutate(county_fips = formatC(STATE, width = 2, flag = "0"))
  }

  raw %>%
    select(county_fips, AGEGRP, starts_with("POPESTIMATE")) %>%
    pivot_longer(
      cols         = starts_with("POPESTIMATE"),
      names_to     = "year",
      names_prefix = "POPESTIMATE",
      values_to    = "pop"
    ) %>%
    mutate(year = as.integer(year), AGEGRP = as.integer(AGEGRP)) %>%
    filter(year %in% 2000:2008)
}

# Download the national county intercensal file once (SUMLEV=50 records only).
# State-level totals are derived by aggregating all counties within each state.
needed_county_fips <- needed_2000_2008 %>%
  filter(geo_type == "county") %>%
  pull(county_fips) %>%
  unique()

statewide_fips <- needed_2000_2008 %>%
  filter(geo_type == "state") %>%
  pull(county_fips) %>%
  unique()

cat("Downloading 2000-2008 intercensal estimates (national county file)...\n")
raw_intercensal <- tryCatch({
  url <- paste0(
    "https://www2.census.gov/programs-surveys/popest/datasets/",
    "2000-2010/intercensal/county/co-est00int-agesex-5yr.csv"
  )
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  download.file(url, tmp, mode = "wb", method = "libcurl", quiet = TRUE)
  read.csv(tmp, check.names = FALSE, fileEncoding = "latin1")
}, error = function(e) {
  message("  [intercensal download]: ", conditionMessage(e))
  NULL
})

# Parse all county records once; reuse for both county and state aggregation
raw_counties_all <- if (!is.null(raw_intercensal)) {
  parse_intercensal_wide(raw_intercensal, "county")
} else tibble()

intercensal_county <- raw_counties_all %>%
  filter(county_fips %in% needed_county_fips)
cat("  County rows pulled:", nrow(intercensal_county), "\n")

# State-level: aggregate every county in the state (the county file has no SUMLEV=40)
intercensal_state <- if (length(statewide_fips) > 0) {
  raw_counties_all %>%
    mutate(state_fips2 = substr(county_fips, 1, 2)) %>%
    filter(state_fips2 %in% statewide_fips) %>%
    group_by(county_fips = state_fips2, AGEGRP, year) %>%
    summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")
} else tibble()
cat("  State rows pulled:", nrow(intercensal_state), "\n")

intercensal_all <- bind_rows(intercensal_county, intercensal_state)

# Map to standard age groups
intercensal_ages <- intercensal_all %>%
  left_join(age_mapping_intercensal, by = "AGEGRP", relationship = "many-to-many") %>%
  mutate(pop = pop * weight) %>%
  group_by(county_fips, year, age) %>%
  summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")

# Apply abcs_areas restrictions and aggregate to state/year/age
hist_2000_2008 <- abcs_areas %>%
  filter(year %in% 2000:2008) %>%
  left_join(intercensal_ages, by = c("county_fips", "year"),
            relationship = "many-to-many") %>%
  filter(
    (age_group == "total") |
    (age_group == "under5"         & age == "0-4") |
    (age_group == "under18"        & age %in% c("0-4","5-17")) |
    (age_group == "under18_65plus" & age %in% c("0-4","5-17","65+"))
  ) %>%
  group_by(state, year, age) %>%
  summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")

cat("\n2000-2008 state/year/age combinations:", nrow(hist_2000_2008), "\n")

# ==============================================================================
# SECTION B: 1998–1999 via 2000 decennial census (P012: Sex by Age)
#
# The 2000 decennial census provides an exact 15-17 / 18-19 split so no
# fractional allocation is needed for the 5-17 / 18-49 boundary.
#
# Population at 1998 and 1999 is approximated as the 2000 census count.
# Typical error: <3% for most counties over a 1-2 year window, which is
# acceptable for surveillance-area denominator estimation.
#
# To improve precision: pull 1990 decennial (table P011 in SF1) and replace
# the assignment below with linear interpolation:
#   pop_1998 = pop1990 + 0.8 * (pop2000 - pop1990)
#   pop_1999 = pop1990 + 0.9 * (pop2000 - pop1990)
# ==============================================================================

needed_1998_1999 <- abcs_areas %>%
  filter(year %in% 1998:1999) %>%
  distinct(county_fips) %>%
  mutate(
    geo_type   = if_else(nchar(county_fips) == 2, "state", "county"),
    state_fips = substr(county_fips, 1, 2)
  )

# P012 variable mapping (exact 15-17 and 18-19 boundaries in decennial SF1)
p012_map <- bind_rows(
  # 0-4
  tibble(variable = c("P012003", "P012027"), age = "0-4"),
  # 5-17: 5-9, 10-14, 15-17
  tibble(variable = c("P012004","P012005","P012006",
                      "P012028","P012029","P012030"), age = "5-17"),
  # 18-49: 18-19, 20, 21, 22-24, 25-29, 30-34, 35-39, 40-44, 45-49
  tibble(variable = c(paste0("P012", formatC(7:15,  width = 3, flag = "0")),
                      paste0("P012", formatC(31:39, width = 3, flag = "0"))),
         age = "18-49"),
  # 50-64: 50-54, 55-59, 60-61, 62-64
  tibble(variable = c(paste0("P012", formatC(16:19, width = 3, flag = "0")),
                      paste0("P012", formatC(40:43, width = 3, flag = "0"))),
         age = "50-64"),
  # 65+: 65-66 through 85+
  tibble(variable = c(paste0("P012", formatC(20:25, width = 3, flag = "0")),
                      paste0("P012", formatC(44:49, width = 3, flag = "0"))),
         age = "65+")
)
all_p012_vars <- p012_map$variable

pull_decennial_age <- function(geo_type, st_fips, fips_codes) {
  tryCatch({
    if (geo_type == "state") {
      raw <- get_decennial(geography = "state", variables = all_p012_vars,
                           year = 2000, sumfile = "sf1") %>%
        filter(GEOID %in% fips_codes)
    } else {
      raw <- get_decennial(geography = "county", state = st_fips,
                           variables = all_p012_vars,
                           year = 2000, sumfile = "sf1") %>%
        filter(GEOID %in% fips_codes)
    }
    raw %>%
      left_join(p012_map, by = "variable") %>%
      group_by(county_fips = GEOID, age) %>%
      summarise(pop = sum(value, na.rm = TRUE), .groups = "drop")
  }, error = function(e) {
    message("  [Decennial 2000] state=", st_fips, ": ", conditionMessage(e))
    NULL
  })
}

cat("\nPulling 2000 decennial census for 1998-1999 approximation...\n")

county_tasks_dec <- needed_1998_1999 %>%
  filter(geo_type == "county") %>%
  group_by(state_fips) %>%
  summarise(fips = list(county_fips), .groups = "drop")

dec2000_county <- pmap_dfr(county_tasks_dec,
  function(state_fips, fips) pull_decennial_age("county", state_fips, fips))

state_tasks_dec <- needed_1998_1999 %>%
  filter(geo_type == "state") %>%
  group_by(state_fips) %>%
  summarise(fips = list(county_fips), .groups = "drop")

dec2000_state <- pmap_dfr(state_tasks_dec,
  function(state_fips, fips) pull_decennial_age("state", state_fips, fips))

dec2000 <- bind_rows(dec2000_county, dec2000_state)
cat("  Decennial rows pulled:", nrow(dec2000), "\n")

# Replicate for 1998 and 1999
dec_1998_1999 <- bind_rows(
  dec2000 %>% mutate(year = 1998L),
  dec2000 %>% mutate(year = 1999L)
)

hist_1998_1999 <- abcs_areas %>%
  filter(year %in% 1998:1999) %>%
  left_join(dec_1998_1999, by = c("county_fips", "year"),
            relationship = "many-to-many") %>%
  filter(
    (age_group == "total") |
    (age_group == "under5"         & age == "0-4") |
    (age_group == "under18"        & age %in% c("0-4","5-17")) |
    (age_group == "under18_65plus" & age %in% c("0-4","5-17","65+"))
  ) %>%
  group_by(state, year, age) %>%
  summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop")

cat("1998-1999 state/year/age combinations:", nrow(hist_1998_1999), "\n")

# ==============================================================================
# Combine and write
# ==============================================================================

age_strat_pop_hist <- bind_rows(hist_1998_1999, hist_2000_2008) %>%
  arrange(state, year, age)

vroom::vroom_write(age_strat_pop_hist, "raw/abcs_census_age_stratified_pop_pre2009.csv",
                   delim = ",")
cat("\nWritten: raw/abcs_census_age_stratified_pop_pre2009.csv\n")
cat("Total rows:", nrow(age_strat_pop_hist), "\n\n")
cat("Preview (first 30 rows):\n")
print(head(age_strat_pop_hist, 30))

# ==============================================================================
# Optional: combine with ACS 2009-2022 data for a full series
# ==============================================================================
# acs_data <- vroom::vroom("raw/abcs_census_age_stratified_pop.csv",
#                          show_col_types = FALSE)
# full_series <- bind_rows(age_strat_pop_hist, acs_data) %>%
#   arrange(state, year, age)
# vroom::vroom_write(full_series, "raw/abcs_census_age_stratified_pop_full.csv",
#                    delim = ",")
# cat("Full 1998-2022 series written:", nrow(full_series), "rows\n")
