# ==============================================================================
# ABCs Denominator Census Validation
#
# 1. Defines ABCs surveillance areas (counties + age restrictions) per state/year
# 2. Pulls Census Bureau total population estimates (2000-2022) for each area
# 3. Compares with spn_surveillance_population to validate alignment
# 4. Extracts ACS 5-year age-stratified population data (2009-2022) for the
#    same areas, using standard age groups: 0-4, 5-17, 18-49, 50-64, 65+
#
# Requirements:
#   - Census API key: register at https://api.census.gov/data/key_signup.html
#     then run once: tidycensus::census_api_key("YOUR_KEY", install = TRUE)
#
# Run from: data/abcs/
# Outputs:
#   raw/census_validation_comparison.csv   -- ACS-derived vs reported populations
#   raw/abcs_census_age_stratified_pop.csv -- age-stratified pops by state/year
# ==============================================================================

library(dplyr)
library(tidyr)
library(purrr)
library(vroom)
library(tidycensus)

# Load reported denominators
denom <- vroom("raw/abcs_denominators.csv", show_col_types = FALSE) %>%
  filter(!is.na(spn_surveillance_population))

# ==============================================================================
# SECTION 1: Define ABCs surveillance areas by state, year, county, age group
#
# county_fips: 5-digit for county, 2-digit state code for statewide entries
# age_group:   "total"          = all ages (use total county population)
#              "under5"         = children < 5 only
#              "under18"        = persons < 18 only
#              "under18_65plus" = persons < 18 OR >= 65
#
# Where a county list applies only to certain age groups (e.g. CA 2000-2010,
# where SF is all-ages but Contra Costa + Alameda are under-5 only), separate
# rows with different age_group values are used.
# ==============================================================================

make_rows <- function(state, years, fips_vec, ag) {
  if (length(ag) == 1) ag <- rep(ag, length(fips_vec))
  crossing(tibble(state = state, year = years),
           tibble(county_fips = fips_vec, age_group = ag))
}

abcs_areas <- bind_rows(

  ## ---- California (state FIPS 06) ----
  # 1994: SF + Contra Costa + Alameda, all ages
  make_rows("CA", 1994,      c("06075","06013","06001"), "total"),
  # 1996-1999: SF only, all ages
  make_rows("CA", 1996:1999, "06075",                   "total"),
  # 2000-2010: SF all ages; CC + Alameda under-5 only
  make_rows("CA", 2000:2010, "06075",                   "total"),
  make_rows("CA", 2000:2010, c("06013","06001"),        "under5"),
  # 2011-2014: SF all ages; CC + Alameda under-18
  make_rows("CA", 2011:2014, "06075",                   "total"),
  make_rows("CA", 2011:2014, c("06013","06001"),        "under18"),
  # 2015-2016: SF all ages; CC + Alameda under-18 and 65+
  make_rows("CA", 2015:2016, "06075",                   "total"),
  make_rows("CA", 2015:2016, c("06013","06001"),        "under18_65plus"),
  # 2017+: All three counties, all ages
  make_rows("CA", 2017:2023, c("06075","06013","06001"), "total"),

  ## ---- Colorado (state FIPS 08) ----
  # Adams, Arapahoe, Denver, Douglas, Jefferson — all ages throughout
  make_rows("CO", 2000:2023,
            c("08001","08005","08031","08035","08059"), "total"),

  ## ---- Connecticut (state FIPS 09) — statewide ----
  make_rows("CT", 1995:2023, "09", "total"),

  ## ---- Georgia (state FIPS 13) — 20-county metro, all ages ----
  # Barrow, Bartow, Carroll, Cherokee, Clayton, Cobb, Coweta, DeKalb,
  # Douglas, Fayette, Forsyth, Fulton, Gwinnett, Henry, Newton,
  # Paulding, Pickens, Rockdale, Spalding, Walton
  make_rows("GA", 1997:2023,
            c("13013","13015","13045","13057","13063",
              "13067","13077","13089","13097","13113",
              "13117","13121","13135","13151","13217",
              "13223","13227","13247","13255","13297"), "total"),

  ## ---- Maryland (state FIPS 24) ----
  # Anne Arundel, Baltimore County, Baltimore City, Carroll, Harford, Howard
  # Note: Baltimore City (24510) is an independent city, separate from
  # Baltimore County (24005)
  make_rows("MD", 1995:2023,
            c("24003","24005","24510","24013","24025","24027"), "total"),

  ## ---- Minnesota (state FIPS 27) ----
  # 1995-2001: 7-county metro (Anoka, Carver, Dakota, Hennepin, Ramsey, Scott, Washington)
  make_rows("MN", 1995:2001,
            c("27003","27019","27037","27053","27123","27139","27163"), "total"),
  # 2002+: statewide
  make_rows("MN", 2002:2023, "27", "total"),

  ## ---- New Mexico (state FIPS 35) — statewide ----
  make_rows("NM", 2004:2023, "35", "total"),

  ## ---- New York (state FIPS 36) ----
  # 1997-1998: 7-county (Genesee, Livingston, Monroe, Ontario, Orleans, Wayne, Yates)
  make_rows("NY", 1997:1998,
            c("36037","36051","36055","36069","36073","36117","36123"), "total"),
  # 1999-2004: 15-county (add Albany, Columbia, Greene, Montgomery,
  #            Rensselaer, Saratoga, Schenectady, Schoharie)
  make_rows("NY", 1999:2004,
            c("36001","36021","36037","36039","36051","36055","36057",
              "36069","36073","36083","36091","36093","36095","36117","36123"), "total"),
  # 2005-2023: 15-county all ages + Erie (36029) under-5 only
  make_rows("NY", 2005:2023,
            c("36001","36021","36037","36039","36051","36055","36057",
              "36069","36073","36083","36091","36093","36095","36117","36123"), "total"),
  make_rows("NY", 2005:2023, "36029", "under5"),

  ## ---- Oklahoma (state FIPS 40) — statewide 1989-1994 ----
  make_rows("OK", 1989:1994, "40", "total"),

  ## ---- Oregon (state FIPS 41) ----
  # Clackamas, Multnomah, Washington
  make_rows("OR", 2004:2023, c("41005","41051","41067"), "total"),

  ## ---- Tennessee (state FIPS 47) ----
  # 1989-1994: 4-county (Davidson, Hamilton, Knox, Shelby)
  make_rows("TN", 1989:1994,
            c("47037","47065","47093","47157"), "total"),
  # 1995-1998: 5-county (add Williamson)
  make_rows("TN", 1995:1998,
            c("47037","47065","47093","47157","47187"), "total"),
  # 1999-2009: 11-county (add Cheatham, Dickson, Robertson, Rutherford, Sumner, Wilson)
  make_rows("TN", 1999:2009,
            c("47021","47037","47043","47065","47093",
              "47147","47149","47157","47165","47187","47189"), "total"),
  # 2010-2023: 20-county (add Anderson, Blount, Grainger, Jefferson,
  #            Loudon, Madison, Roane, Sevier, Union)
  make_rows("TN", 2010:2023,
            c("47001","47009","47021","47037","47043","47057","47065","47089",
              "47093","47105","47113","47145","47147","47149","47155","47157",
              "47165","47173","47187","47189"), "total"),

  ## ---- Texas (state FIPS 48) ----
  # Bexar County only, 1995-1996
  make_rows("TX", 1995:1996, "48029", "total")
)

# ==============================================================================
# SECTION 2: Pull ACS 5-year age-stratified estimates (2009-2022)
#
# Table B01001 (Sex by Age) gives precise 5-year age bins plus 15-17, 18-19
# enabling exact computation of 0-4, 5-17, 18-49, 50-64, 65+ groups.
#
# Male variables:   B01001_003 to B01001_025
# Female variables: B01001_027 to B01001_049
# ==============================================================================

# Variable-to-age-group mapping for B01001
b01001_map <- bind_rows(
  #  0-4
  tibble(variable = c("B01001_003","B01001_027"), age_grp = "0-4"),
  #  5-17
  tibble(variable = paste0("B01001_0", c("04","05","06","28","29","30")), age_grp = "5-17"),
  # 18-49
  tibble(variable = paste0("B01001_0", c("07","08","09","10","11","12","13","14","15",
                                         "31","32","33","34","35","36","37","38","39")), age_grp = "18-49"),
  # 50-64
  tibble(variable = paste0("B01001_0", c("16","17","18","19","40","41","42","43")), age_grp = "50-64"),
  # 65+
  tibble(variable = paste0("B01001_0", c("20","21","22","23","24","25",
                                         "44","45","46","47","48","49")), age_grp = "65+")
)
all_b01001_vars <- b01001_map$variable

cat("Pulling ACS 5-year age-stratified estimates (2009-2022)...\n")
cat("This will take several minutes (many API calls).\n\n")

ACS_YEARS <- 2009:2022

needed_acs <- abcs_areas %>%
  filter(year %in% ACS_YEARS) %>%
  distinct(county_fips, year) %>%
  mutate(
    geo_type   = if_else(nchar(county_fips) == 2, "state", "county"),
    state_fips = substr(county_fips, 1, 2)
  )

# Helper: pull ACS 5-year B01001 for a set of geographies in one state/year
pull_acs_age <- function(yr, geo_type, st_fips, fips_codes) {
  tryCatch({
    if (geo_type == "state") {
      raw <- get_acs(geography = "state", variables = all_b01001_vars,
                     year = yr, survey = "acs5") %>%
        filter(GEOID %in% fips_codes)
    } else {
      raw <- get_acs(geography = "county", state = st_fips,
                     variables = all_b01001_vars, year = yr, survey = "acs5") %>%
        filter(GEOID %in% fips_codes)
    }
    raw %>%
      left_join(b01001_map, by = "variable") %>%
      group_by(county_fips = GEOID, age_grp) %>%
      summarise(pop = sum(estimate, na.rm = TRUE), .groups = "drop") %>%
      mutate(year = yr)
  }, error = function(e) {
    message("  [ACS age] state=", st_fips, " year=", yr, ": ", conditionMessage(e))
    NULL
  })
}

# Pull ACS state-level age data
acs_state_tasks <- needed_acs %>%
  filter(geo_type == "state") %>%
  group_by(year, state_fips) %>%
  summarise(fips = list(county_fips), .groups = "drop")

acs_state_age <- pmap_dfr(acs_state_tasks,
  function(year, state_fips, fips) pull_acs_age(year, "state", state_fips, fips))

# Pull ACS county-level age data (grouped by year × state)
acs_county_tasks <- needed_acs %>%
  filter(geo_type == "county") %>%
  group_by(year, state_fips) %>%
  summarise(fips = list(county_fips), .groups = "drop")

acs_county_age <- pmap_dfr(acs_county_tasks,
  function(year, state_fips, fips) pull_acs_age(year, "county", state_fips, fips))

acs_age_data <- bind_rows(acs_state_age, acs_county_age)
cat("Retrieved ACS age data for", nrow(acs_age_data), "geography-year-age group combinations.\n\n")

# ==============================================================================
# SECTION 3: Compute ACS-derived total surveillance population for comparison
#
# Sums ACS age-group populations across relevant counties, respecting the age
# restrictions defined in abcs_areas, then compares with spn_surveillance_population.
# ==============================================================================

surveillance_pop <- abcs_areas %>%
  filter(year %in% 2009:2022) %>%  # ACS years only for consistency
  left_join(acs_age_data, by = c("county_fips", "year"),
            relationship = "many-to-many") %>%
  filter(
    (age_group == "total") |
    (age_group == "under5"         & age_grp == "0-4") |
    (age_group == "under18"        & age_grp %in% c("0-4","5-17")) |
    (age_group == "under18_65plus" & age_grp %in% c("0-4","5-17","65+"))
  ) %>%
  group_by(state, year) %>%
  summarise(census_derived_pop = sum(pop, na.rm = TRUE), .groups = "drop")

# Compare with reported populations
validation <- denom %>%
  select(state, year, spn_surveillance_population) %>%
  mutate(year = as.integer(year)) %>%
  inner_join(surveillance_pop, by = c("state","year")) %>%
  mutate(
    ratio    = census_derived_pop / spn_surveillance_population,
    pct_diff = (census_derived_pop - spn_surveillance_population) /
               spn_surveillance_population * 100
  ) %>%
  arrange(state, year)

cat("=== VALIDATION: Census-derived vs Reported Populations (2009-2022) ===\n")
print(validation %>%
        select(state, year, spn_reported = spn_surveillance_population,
               census_derived = census_derived_pop, ratio, pct_diff),
      n = Inf)

cat("\n--- Percent difference summary ---\n")
print(summary(validation$pct_diff))

vroom::vroom_write(validation, "raw/census_validation_comparison.csv", delim = ",")
cat("\nWritten: raw/census_validation_comparison.csv\n\n")

# ==============================================================================
# SECTION 4: Age-stratified surveillance area populations (ACS 5-year, 2009-2022)
#
# For each state/year, sums ACS age-group populations across relevant counties,
# respecting the age restrictions defined in abcs_areas.
#
# Standard output age groups: 0-4, 5-17, 18-49, 50-64, 65+
# For restricted areas:
#   - "total" counties contribute to all age groups
#   - "under5" counties contribute only to 0-4
#   - "under18" counties contribute to 0-4 and 5-17
#   - "under18_65plus" counties contribute to 0-4, 5-17, and 65+
# ==============================================================================

age_strat_pop <- abcs_areas %>%
  filter(year %in% ACS_YEARS) %>%
  left_join(acs_age_data, by = c("county_fips", "year"),
            relationship = "many-to-many") %>%
  filter(
    (age_group == "total") |
    (age_group == "under5"         & age_grp == "0-4") |
    (age_group == "under18"        & age_grp %in% c("0-4","5-17")) |
    (age_group == "under18_65plus" & age_grp %in% c("0-4","5-17","65+"))
  ) %>%
  group_by(state, year, age = age_grp) %>%
  summarise(pop = sum(pop, na.rm = TRUE), .groups = "drop") %>%
  arrange(state, year, age)

vroom::vroom_write(age_strat_pop, "raw/abcs_census_age_stratified_pop.csv", delim = ",")
cat("Written: raw/abcs_census_age_stratified_pop.csv\n")
cat("Age-stratified rows:", nrow(age_strat_pop), "\n")
cat("\nPreview (first 30 rows):\n")
print(head(age_strat_pop, 30))
