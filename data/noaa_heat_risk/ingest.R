# =============================================================================
# NOAA HeatRisk Data Ingestion
# Archive:  https://www.wpc.ncep.noaa.gov/heatrisk/data/archive/
# Forecast: https://www.wpc.ncep.noaa.gov/heatrisk/data/
#
# Outputs:
#   standard/data_county.csv.gz  - County-level mean heat risk (CONUS)
#   standard/data_state.csv.gz   - State + national mean heat risk (CONUS)
#
# Columns: geography, time, value, forecast_day
#   value        = mean heat risk score (0-4 scale)
#   forecast_day = 0 for archive (Day 1 historical), 1-7 for current forecast
#
# Note: KML files on the archive page are image overlays only. This script
# uses the GeoTIFF files which contain actual raster values (0-4 scale:
# 0=No Risk, 1=Minor, 2=Moderate, 3=Major, 4=Extreme).
# =============================================================================

library(dplyr)
library(terra)
library(exactextractr)
library(sf)
library(tigris)
library(vroom)

`%||%` <- function(x, y) if (is.null(x)) y else x

# Initialize process record
process <- dcf::dcf_process_record()

# Base URLs
archive_base_url <- "https://www.wpc.ncep.noaa.gov/heatrisk/data/archive"
forecast_base_url <- "https://www.wpc.ncep.noaa.gov/heatrisk/data"

# States to exclude (non-CONUS: Alaska, Hawaii, territories)
non_conus_fips <- c( "60", "66", "69", "72", "78")

# -----------------------------------------------------------------------------
# Setup: Cache county and state geometries (downloaded once)
# -----------------------------------------------------------------------------

counties_file <- "raw/counties.rds"
if (!file.exists(counties_file)) {
  message("Downloading and caching county boundaries (one-time setup)...")
  counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023,
                                   progress_bar = FALSE) %>%
    filter(!STATEFP %in% non_conus_fips) %>%
    select(geography = GEOID)
  saveRDS(counties_sf, counties_file)
} else {
  counties_sf <- readRDS(counties_file)
}

states_file <- "raw/states.rds"
if (!file.exists(states_file)) {
  message("Downloading and caching state boundaries (one-time setup)...")
  states_sf <- tigris::states(cb = TRUE, resolution = "5m", year = 2023,
                               progress_bar = FALSE) %>%
    filter(!STATEFP %in% non_conus_fips) %>%
    select(geography = STATEFP)
  saveRDS(states_sf, states_file)
} else {
  states_sf <- readRDS(states_file)
}

# -----------------------------------------------------------------------------
# Helper: Extract county, state, and national mean heat risk from a GeoTIFF
# -----------------------------------------------------------------------------

extract_heat_risk <- function(tif_path, date_iso, forecast_day_val) {
  r <- tryCatch(terra::rast(tif_path), error = function(e) {
    message("  Could not read raster: ", conditionMessage(e))
    NULL
  })
  if (is.null(r)) return(NULL)

  r_crs <- terra::crs(r)

  # Project boundaries to raster CRS
  counties_proj <- sf::st_transform(counties_sf, crs = r_crs)
  states_proj   <- sf::st_transform(states_sf, crs = r_crs)

  # County means + pixel count (count = area-weighted number of cells)
  # low_coverage_flag = 1 when fewer than 9 pixels cover the county (~3x3 at
  # 2.5 km resolution, roughly <56 km²). Values for these counties are derived
  # from very few pixels and should be interpreted with caution.
  county_stats <- exactextractr::exact_extract(r, counties_proj,
                                                c("mean", "count"),
                                                progress = FALSE)
  county_data <- counties_proj %>%
    sf::st_drop_geometry() %>%
    mutate(
      time              = date_iso,
      value             = round(county_stats$mean, 4),
      low_coverage_flag = as.integer(county_stats$count < 9),
      forecast_day      = as.integer(forecast_day_val)
    ) %>%
    select(geography, time, value, low_coverage_flag, forecast_day) %>%
    filter(!is.na(value))

  # State means
  state_vals <- exactextractr::exact_extract(r, states_proj, "mean",
                                              progress = FALSE)
  state_data <- states_proj %>%
    sf::st_drop_geometry() %>%
    mutate(
      time         = date_iso,
      value        = round(state_vals, 4),
      forecast_day = as.integer(forecast_day_val)
    ) %>%
    select(geography, time, value, forecast_day) %>%
    filter(!is.na(value))

  # National mean (CONUS pixel mean)
  national_mean <- terra::global(r, "mean", na.rm = TRUE)$mean
  national_data <- data.frame(
    geography    = "00",
    time         = date_iso,
    value        = round(national_mean, 4),
    forecast_day = as.integer(forecast_day_val),
    stringsAsFactors = FALSE
  )

  list(county = county_data, state = bind_rows(state_data, national_data))
}

# -----------------------------------------------------------------------------
# Part 1: Archive Data (historical daily, starting 2024-08-01)
# -----------------------------------------------------------------------------

archive_start <- as.Date("2024-08-01")
archive_end   <- Sys.Date() - 1  # through yesterday

all_archive_dates   <- seq(archive_start, archive_end, by = "day")
processed_dates     <- process$processed_archive_dates %||% character(0)
new_archive_dates   <- all_archive_dates[
  !format(all_archive_dates, "%Y%m%d") %in% processed_dates
]

if (length(new_archive_dates) > 0) {
  message(sprintf("Processing %d new archive date(s)...", length(new_archive_dates)))

  new_county_list  <- list()
  new_state_list   <- list()
  newly_processed  <- character(0)

  for (d in new_archive_dates) {
    date_obj  <- as.Date(d, origin = "1970-01-01")
    date_str  <- format(date_obj, "%Y%m%d")
    date_iso  <- format(date_obj, "%Y-%m-%d")

    tif_url  <- sprintf("%s/HeatRisk_CONUS_%s.tif", archive_base_url, date_str)
    tmp_file <- tempfile(fileext = ".tif")

    success <- tryCatch({
      download.file(tif_url, tmp_file, mode = "wb", quiet = TRUE)
      TRUE
    }, error   = function(e) FALSE,
       warning = function(w) FALSE)

    if (!success || !file.exists(tmp_file)) {
      message("  Skipping ", date_iso, " (download failed)")
      next
    }

    message("  Processing ", date_iso)
    result <- extract_heat_risk(tmp_file, date_iso, 0L)
    file.remove(tmp_file)

    if (!is.null(result)) {
      new_county_list[[date_str]]  <- result$county
      new_state_list[[date_str]]   <- result$state
      newly_processed              <- c(newly_processed, date_str)
    }
  }

  if (length(new_county_list) > 0) {
    county_file <- "standard/data_county.csv.gz"
    state_file  <- "standard/data_state.csv.gz"

    new_county <- bind_rows(new_county_list)
    new_state  <- bind_rows(new_state_list)

    # Append to existing archive data if present
    if (file.exists(county_file)) {
      existing <- vroom(county_file, show_col_types = FALSE) %>%
        filter(forecast_day == 0L) %>%
        mutate(time = as.character(time))
      new_county <- bind_rows(existing, new_county) %>%
        distinct(geography, time, .keep_all = TRUE) %>%
        arrange(geography, time)
    }
    if (file.exists(state_file)) {
      existing <- vroom(state_file, show_col_types = FALSE) %>%
        filter(forecast_day == 0L) %>%
        mutate(time = as.character(time))
      new_state <- bind_rows(existing, new_state) %>%
        distinct(geography, time, .keep_all = TRUE) %>%
        arrange(geography, time)
    }

    vroom::vroom_write(new_county, county_file, delim = ",")
    vroom::vroom_write(new_state,  state_file,  delim = ",")

    process$processed_archive_dates <- c(processed_dates, newly_processed)
    dcf::dcf_process_record(updated = process)
    message(sprintf("Archive: added %d date(s).", length(newly_processed)))
  }
}

# -----------------------------------------------------------------------------
# Part 2: Current 7-Day Forecast (Days 1-7)
# Updates the forecast rows in both output files each run
# -----------------------------------------------------------------------------

message("Downloading 7-day forecast...")

today_str         <- format(Sys.Date(), "%Y%m%d")
last_fcst_run     <- process$last_forecast_date %||% ""

if (!identical(today_str, last_fcst_run)) {
  fcst_county_list <- list()
  fcst_state_list  <- list()

  for (day in 1:5) {
    tif_url   <- sprintf("%s/HeatRisk_%d_Mercator.tif", forecast_base_url, day)
    tmp_file  <- tempfile(fileext = ".tif")
    valid_iso <- format(Sys.Date() + day, "%Y-%m-%d")

    success <- tryCatch({
      download.file(tif_url, tmp_file, mode = "wb", quiet = TRUE)
      TRUE
    }, error   = function(e) FALSE,
       warning = function(w) FALSE)

    if (!success || !file.exists(tmp_file)) {
      message("  Skipping forecast day ", day, " (download failed)")
      next
    }

    message("  Processing forecast day ", day, " (", valid_iso, ")")
    result <- extract_heat_risk(tmp_file, valid_iso, as.integer(day))
    file.remove(tmp_file)

    if (!is.null(result)) {
      fcst_county_list[[as.character(day)]] <- result$county
      fcst_state_list[[as.character(day)]]  <- result$state
    }
  }

  if (length(fcst_county_list) > 0) {
    county_file <- "standard/data_county.csv.gz"
    state_file  <- "standard/data_state.csv.gz"

    new_fcst_county <- bind_rows(fcst_county_list)
    new_fcst_state  <- bind_rows(fcst_state_list)

    # Replace forecast rows, keep archive rows
    if (file.exists(county_file)) {
      archive_county <- vroom(county_file, show_col_types = FALSE) %>%
        filter(forecast_day == 0L) %>%
        mutate(time = as.character(time))
      vroom::vroom_write(bind_rows(archive_county, new_fcst_county),
                         county_file, delim = ",")
    } else {
      vroom::vroom_write(new_fcst_county, county_file, delim = ",")
    }

    if (file.exists(state_file)) {
      archive_state <- vroom(state_file, show_col_types = FALSE) %>%
        filter(forecast_day == 0L) %>%
        mutate(time = as.character(time))
      vroom::vroom_write(bind_rows(archive_state, new_fcst_state),
                         state_file, delim = ",")
    } else {
      vroom::vroom_write(new_fcst_state, state_file, delim = ",")
    }

    process$last_forecast_date <- today_str
    dcf::dcf_process_record(updated = process)
    message("Forecast updated.")
  }
} else {
  message("Forecast already updated today, skipping.")
}


#Daily Heat risk ED bundled
all_fips <- vroom::vroom('../../resources/all_fips.csv.gz') 

heat_forecast <- vroom::vroom('./standard/data_county.csv.gz') %>%
  filter(forecast_day >=1) %>%
  left_join(all_fips, by='geography') %>%
  dplyr::select(geography, time, value, forecast_day) %>%
  ungroup()

arrow::write_parquet(heat_forecast, "../bundle_injury_overdose/dist/heat_risk.parquet")
