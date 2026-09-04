# =============================================================================
# CMS Birthing Friendly Hospitals Data Ingestion
# Source: https://data.cms.gov/provider-data/dataset/hbf-map
# =============================================================================

library(dplyr)
library(sf)
library(tigris)

process <- dcf::dcf_process_record()

dir.create("raw", showWarnings = FALSE)

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
# The CSV download URL is versioned and changes whenever CMS refreshes the
# dataset, so look up the current URL (and the snapshot's published date) from
# the metastore API rather than hardcoding the URL.
dataset_meta <- jsonlite::fromJSON(
  "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/hbf-map?show-reference-ids=false"
)
download_url <- dataset_meta$distribution$data$downloadURL[[1]]
snapshot_date <- dataset_meta$modified

download.file(download_url, "raw/Birthing_Friendly_Hospitals_Geocoded.csv", mode = "wb", quiet = TRUE)

raw_state <- list(hash = unname(tools::md5sum("raw/Birthing_Friendly_Hospitals_Geocoded.csv")))

# -----------------------------------------------------------------------------
# 2. Check for changes
# -----------------------------------------------------------------------------
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 3. Read raw data
  # ---------------------------------------------------------------------------
  # This dataset is a facility-level list (one row per hospital designated as
  # "Birthing Friendly" by CMS) with no time dimension of its own, so each
  # ingest run is a single snapshot dated by the source's published update date.
  data_raw <- vroom::vroom(
    "raw/Birthing_Friendly_Hospitals_Geocoded.csv",
    show_col_types = FALSE
  )

  # ---------------------------------------------------------------------------
  # 4. Transform to standard wide format
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state)

  data_state <- data_raw %>%
    count(state, name = "cms_birth_n_birthing_friendly") %>%
    left_join(state_fips_lookup, by = "state") %>%
    transmute(
      geography = geography,
      time = snapshot_date,
      cms_birth_n_birthing_friendly
    )

  data_national <- data_state %>%
    summarize(
      geography = "00",
      time = snapshot_date,
      cms_birth_n_birthing_friendly = sum(cms_birth_n_birthing_friendly)
    )

  # County-level: point-in-polygon join of each hospital's lat/lon against
  # Census county boundaries (cached locally after the first download).
  counties_file <- "raw/counties.rds"
  if (!file.exists(counties_file)) {
    counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023,
                                     progress_bar = FALSE) %>%
      select(geography = GEOID)
    saveRDS(counties_sf, counties_file)
  } else {
    counties_sf <- readRDS(counties_file)
  }

  hospitals_sf <- sf::st_as_sf(data_raw, coords = c("lon", "lat"), crs = 4326) %>%
    sf::st_transform(sf::st_crs(counties_sf))

  hospital_counties <- sf::st_join(hospitals_sf, counties_sf, join = sf::st_within) %>%
    sf::st_drop_geometry()

  # A hospital that falls just outside every polygon (e.g. a coastal point in
  # open water on a coarse cartographic boundary) is assigned to its nearest
  # county instead of being dropped.
  unmatched <- is.na(hospital_counties$geography)
  if (any(unmatched)) {
    nearest_idx <- sf::st_nearest_feature(hospitals_sf[unmatched, ], counties_sf)
    hospital_counties$geography[unmatched] <- counties_sf$geography[nearest_idx]
  }

  data_county <- hospital_counties %>%
    count(geography, name = "cms_birth_n_birthing_friendly") %>%
    mutate(time = snapshot_date) %>%
    select(geography, time, cms_birth_n_birthing_friendly)

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(bind_rows(data_state, data_national), "standard/data_state.csv.gz", ",")
  vroom::vroom_write(data_county, "standard/data_county.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # 6. Update process record
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
