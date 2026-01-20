# =============================================================================
# MMR Vaccine Coverage Estimates (HealthMap) Data Ingestion
# Source: https://github.com/eric-gengzhou/MMR_vaccine_estimates
# Citation: Zhou, E.G., Brownstein, J., Rader, B. (2025). Assessing MMR
#   Vaccination Coverage Gaps in US Children with Digital Participatory
#   Surveillance. Nature Health.
# =============================================================================

library(dplyr)
library(vroom)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Process County-Level Data
# -----------------------------------------------------------------------------

# Read county data
data_county_raw <- vroom::vroom("raw/county_pred_final.csv.gz")

# Transform to standard format
# Note: This is a cross-sectional estimate (no time dimension)
# Using 12-31-2024 as the time point based on when the data was published
data_county <- data_county_raw %>%
  rename(
    geography = county_fips
  ) %>%
  mutate(
    # Ensure geography is character with leading zeros
    geography = sprintf("%05d", as.numeric(geography)),
    # Time point: end of 2024 (when estimates were published)
    time = "12-31-2024",
    # Convert proportion to percentage (0-100 scale)
    value = est_mean * 100,
    # Keep additional columns for context
    risk_level = risk_level,
    local_i = local_i,
    p_value = p_value,
    spatial_cluster = highlow_cat
  ) %>%
  select(geography, time, value, risk_level, local_i, p_value, spatial_cluster) %>%
  arrange(geography)

# Write county-level standardized output
vroom::vroom_write(
  data_county,
  "standard/data_county.csv.gz",
  delim = ","
)

# -----------------------------------------------------------------------------
# 2. Process ZCTA (ZIP Code) Level Data
# -----------------------------------------------------------------------------

# Read ZCTA data
data_zcta_raw <- vroom::vroom("raw/zcta_pred_final.csv.gz")

# Transform to standard format
data_zcta <- data_zcta_raw %>%
  rename(
    geography = zcta5
  ) %>%
  # Filter out uninhabited ZCTAs
  filter(!uninhabited) %>%
  mutate(
    # Ensure geography is character with leading zeros
    geography = sprintf("%05d", as.numeric(geography)),
    # Time point: end of 2024
    time = "12-31-2024",
    # Convert proportion to percentage (0-100 scale)
    value = est_mean * 100,
    # Keep additional columns for context
    risk_level = risk_level,
    spatial_cluster = quad,
    population_sample = n_ps_tot
  ) %>%
  select(geography, time, value, risk_level, spatial_cluster, population_sample) %>%
  arrange(geography)

# Write ZCTA-level standardized output
vroom::vroom_write(
  data_zcta,
  "standard/data_zcta.csv.gz",
  delim = ","
)

# -----------------------------------------------------------------------------
# 3. Create National and State-Level Summaries from County Data
# -----------------------------------------------------------------------------

# Load state populations for weighting (if available)
# For now, calculate simple averages by state
data_state <- data_county_raw %>%
  mutate(
    state_fips = substr(sprintf("%05d", as.numeric(county_fips)), 1, 2),
    value = est_mean * 100
  ) %>%
  group_by(state_fips) %>%
  summarize(
    value = mean(value, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(geography = state_fips) %>%
  mutate(
    time = "12-31-2024"
  ) %>%
  select(geography, time, value) %>%
  arrange(geography)

# Calculate national average
data_national <- data_county_raw %>%
  summarize(
    value = mean(est_mean * 100, na.rm = TRUE)
  ) %>%
  mutate(
    geography = "00",
    time = "12-31-2024"
  ) %>%
  select(geography, time, value)

# Combine state and national
data_state_national <- bind_rows(data_national, data_state)

# Write state-level standardized output
vroom::vroom_write(
  data_state_national,
  "standard/data_state.csv.gz",
  delim = ","
)

# -----------------------------------------------------------------------------
# 4. Record processed state
# -----------------------------------------------------------------------------

# Since this is a static dataset downloaded manually, mark as processed
process$raw_state <- list(
  county_file = "county_pred_final.csv.gz",
  zcta_file = "zcta_pred_final.csv.gz",
  processed_date = Sys.Date()
)

dcf::dcf_process_record(updated = process)
