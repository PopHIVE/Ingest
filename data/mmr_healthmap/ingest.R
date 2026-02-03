# =============================================================================
# MMR Vaccine Coverage Estimates (HealthMap) Data Ingestion
# Source: https://github.com/eric-gengzhou/MMR_vaccine_estimates
# Citation: Zhou, E.G., Brownstein, J., Rader, B. (2025). Assessing MMR
#   Vaccination Coverage Gaps in US Children with Digital Participatory
#   Surveillance. Nature Health.
# =============================================================================

library(dplyr)
library(vroom)
library(httr)
library(jsonlite)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Check GitHub for updates and download if changed
# -----------------------------------------------------------------------------

# GitHub repository info
github_owner <- "eric-gengzhou"
github_repo <- "MMR_vaccine_estimates"
github_branch <- "main"

# Files to track (files are in repository root, not in a data folder)
files_to_download <- c(
  "county_pred_final.csv" = "county_pred_final.csv",
  "zcta_pred_final.csv" = "zcta_pred_final.csv"
)

# Get the latest commit SHA for the repository
api_url <- sprintf(
  "https://api.github.com/repos/%s/%s/commits?sha=%s&per_page=1",
  github_owner, github_repo, github_branch
)

response <- httr::GET(api_url, httr::add_headers("User-Agent" = "PopHIVE-Ingest"))

if (httr::status_code(response) != 200) {
  stop("Failed to fetch GitHub commit info: ", httr::status_code(response))
}

commits <- jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"))
latest_commit_sha <- commits$sha[1]
latest_commit_date <- commits$commit$committer$date[1]

# Check if we need to update (compare commit SHA)
needs_update <- is.null(process$raw_state) ||
  is.null(process$raw_state$commit_sha) ||
  process$raw_state$commit_sha != latest_commit_sha

if (needs_update) {
  message("New data detected on GitHub (commit: ", substr(latest_commit_sha, 1, 7), ")")
  message("Downloading updated files...")

  # Download each file
  for (local_name in names(files_to_download)) {
    github_path <- files_to_download[local_name]
    raw_url <- sprintf(
      "https://raw.githubusercontent.com/%s/%s/%s/%s",
      github_owner, github_repo, github_branch, github_path
    )

    local_path <- file.path("raw", paste0(local_name, ".gz"))
    temp_path <- file.path("raw", local_name)

    message("  Downloading ", local_name, "...")
    download.file(raw_url, temp_path, mode = "wb", quiet = TRUE)

    # Compress the file
    R.utils::gzip(temp_path, destname = local_path, overwrite = TRUE, remove = TRUE)
  }

  message("Download complete.")
} else {
  message("No updates found on GitHub. Skipping download.")
}

# Only process if data has changed
if (!needs_update) {
  message("Data unchanged. Skipping processing.")
  return(invisible(NULL))
}

# -----------------------------------------------------------------------------
# 2. Process County-Level Data
# -----------------------------------------------------------------------------

# Read county data (convert from Arrow to regular R data frame)
data_county_raw <- vroom::vroom("raw/county_pred_final.csv.gz", show_col_types = FALSE) %>%
  as.data.frame()

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

# Read ZCTA data (convert from Arrow to regular R data frame)
data_zcta_raw <- vroom::vroom("raw/zcta_pred_final.csv.gz", show_col_types = FALSE) %>%
  as.data.frame()

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

# Load county population data (under 5 years) for weighting
pop_county <- vroom::vroom(
  "../../resources/census_population_2021.csv.xz",
  show_col_types = FALSE
) %>%
  as.data.frame() %>%
  filter(nchar(GEOID) == 5) %>%
  select(geography = GEOID, pop_under5 = `Under 5 years`) %>%
  mutate(geography = sprintf("%05d", as.numeric(geography)))

# Add Connecticut planning region population (under 5 years)
# CT switched from counties to planning regions in 2022 with new FIPS codes (09110-09190)
# See resources/ct_planning_regions_pop_under5.csv.gz (source: 2022 ACS 1-year estimates)
ct_pop <- vroom::vroom(
  "../../resources/ct_planning_regions_pop_under5.csv.gz",
  show_col_types = FALSE
) %>%
  as.data.frame() %>%
  select(geography, pop_under5)

pop_county <- bind_rows(pop_county, ct_pop)

# Merge population with county data
data_county_with_pop <- data_county_raw %>%
  mutate(
    geography = sprintf("%05d", as.numeric(county_fips)),
    state_fips = substr(geography, 1, 2),
    value = est_mean * 100
  ) %>%
  left_join(pop_county, by = "geography")

# Calculate population-weighted state averages
data_state <- data_county_with_pop %>%
  filter(!is.na(pop_under5)) %>%
  group_by(state_fips) %>%
  summarize(
    value = weighted.mean(value, pop_under5, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(geography = state_fips) %>%
  mutate(
    time = "12-31-2024"
  ) %>%
  select(geography, time, value) %>%
  arrange(geography)

# Calculate population-weighted national average
data_national <- data_county_with_pop %>%
  filter(!is.na(pop_under5)) %>%
  summarize(
    value = weighted.mean(value, pop_under5, na.rm = TRUE)
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

# Record the GitHub commit SHA to detect future changes
process$raw_state <- list(
  commit_sha = latest_commit_sha,
  commit_date = latest_commit_date,
  county_file = "county_pred_final.csv.gz",
  zcta_file = "zcta_pred_final.csv.gz",
  processed_date = as.character(Sys.Date())
)

dcf::dcf_process_record(updated = process)
