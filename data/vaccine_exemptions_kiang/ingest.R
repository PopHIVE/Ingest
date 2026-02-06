# =============================================================================
# Vaccine Exemptions Data Ingestion (Kiang et al.)
# Source: https://github.com/mkiang/vaccine_exemptions
# Paper: https://jamanetwork.com/journals/jama/fullarticle/2843870
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
url <- "https://raw.githubusercontent.com/mkiang/vaccine_exemptions/refs/heads/main/data/analytic_data_rounded.csv"
raw_path <- "raw/exemptions.csv"
download.file(url, raw_path)
unlink(paste0(raw_path, ".xz"))
system2("xz", c("-f", raw_path))

# -----------------------------------------------------------------------------
# 2. Check raw state
# -----------------------------------------------------------------------------
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv",
  recursive = TRUE,
  full.names = TRUE
)))

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 3. Read and transform data
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom("raw/exemptions.csv.xz", delim = ",", show_col_types = FALSE)

  # ---------------------------------------------------------------------------
  # 3a. Create state-level data
  # ---------------------------------------------------------------------------
  data_state <- data_raw %>%
    filter(geography == "state") %>%
    mutate(
      # Use state abbreviation to get FIPS code (convert to 2-digit character string)
      geography = stringr::str_pad(as.character(cdlTools::fips(state_abb, to = "FIPS")), width = 2, pad = "0")
    ) %>%
    # Format time - use September 1 of each year for school-entry data
    mutate(
      time = paste0("09-01-", year),
      time = format(as.Date(time, format = "%m-%d-%Y"), "%m-%d-%Y")
    ) %>%
    # Select and rename columns - keep only essential columns
    select(
      geography,
      time,
      exemption_rate_mmr = pct
    ) %>%
    # Remove rows with missing geography
    filter(!is.na(geography))

  # Load state populations for weighted averaging
  state_pop <- vroom::vroom("../../resources/pop_state.csv.gz", show_col_types = FALSE) %>%
    filter(age_level == "Total") %>%
    select(state_name = geography, population = popsize) %>%
    # Convert state names to FIPS codes
    mutate(
      geography = stringr::str_pad(
        as.character(cdlTools::fips(state_name, to = "FIPS")),
        width = 2, pad = "0"
      )
    ) %>%
    filter(!is.na(geography)) %>%
    select(geography, population)

  # Calculate national average (population-weighted mean of states)
  nat_ave <- data_state %>%
    left_join(state_pop, by = "geography") %>%
    group_by(time) %>%
    summarize(
      exemption_rate_mmr = weighted.mean(exemption_rate_mmr, population, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(geography = '00')

  # Combine state and national data
  data_state_combined <- bind_rows(data_state, nat_ave)

  # ---------------------------------------------------------------------------
  # 3b. Create county-level data
  # ---------------------------------------------------------------------------
  data_county <- data_raw %>%
    filter(geography == "county") %>%
    # Use the IHME FIPS codes (more complete)
    mutate(
      geography = stringr::str_pad(as.character(fips_ihme), width = 5, pad = "0")
    ) %>%
    # Format time - use September 1 of each year for school-entry data
    mutate(
      time = paste0("09-01-", year),
      time = format(as.Date(time, format = "%m-%d-%Y"), "%m-%d-%Y")
    ) %>%
    # Select and rename columns - keep only essential columns
    select(
      geography,
      time,
      exemption_rate_mmr = pct
    ) %>%
    # Remove rows with missing geography
    filter(!is.na(geography))

  # ---------------------------------------------------------------------------
  # 4. Write standardized outputs
  # ---------------------------------------------------------------------------
  # State-level data
  vroom::vroom_write(
    data_state_combined,
    "standard/data_state.csv.gz",
    delim = ","
  )

  # County-level data
  vroom::vroom_write(
    data_county,
    "standard/data_county.csv.gz",
    delim = ","
  )

  # Also write state as main data file (for backwards compatibility)
  vroom::vroom_write(
    data_state_combined,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}