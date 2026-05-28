# =============================================================================
# Kinsa ILI Data Ingestion
# Source: https://apiv2.kinsainsights.com/api/v1
# Credentials: KINSA_EMAIL and KINSA_PASSWORD environment variables
#
# Output:
#   standard/data.csv.gz  - National daily Cough/Cold/Flu signal
#
# Columns: geography, time, value
#   value = percent_ill (share of Kinsa thermometer users reporting illness)
# =============================================================================

library(httr2)
library(dplyr)
library(purrr)
library(vroom)

KINSA_BASE_URL <- "https://apiv2.kinsainsights.com/api/v1"

# Initialize process record
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# Authentication helpers
# -----------------------------------------------------------------------------

kinsa_login <- function(email    = Sys.getenv("KINSA_EMAIL"),
                        password = Sys.getenv("KINSA_PASSWORD")) {
  if (email == "" || password == "") {
    stop("Kinsa credentials not found. Set KINSA_EMAIL and KINSA_PASSWORD.")
  }
  resp <- request(paste0(KINSA_BASE_URL, "/auth/login")) |>
    req_method("POST") |>
    req_headers("Content-Type" = "application/json") |>
    req_body_json(list(email = email, password = password)) |>
    req_error(body = \(r) resp_body_string(r)) |>
    req_perform()
  resp_body_json(resp)
}

kinsa_latest_date <- function(access_token) {
  resp <- request(paste0(KINSA_BASE_URL, "/latest_date_available")) |>
    req_method("GET") |>
    req_headers(
      "Content-Type"  = "application/json",
      "Authorization" = paste("Bearer", access_token)
    ) |>
    req_error(body = \(r) resp_body_string(r)) |>
    req_perform()
  result <- resp_body_json(resp)
  result$data[[1]]$latest_date  # Returns "YYYY-MM-DD"
}

kinsa_signal <- function(access_token, region_type, signal_type,
                         start_date = NULL, end_date = NULL) {
  body <- list(
    region_type = as.list(region_type),
    signal_type = as.list(signal_type)
  )
  if (!is.null(start_date)) body$start_date <- start_date
  if (!is.null(end_date))   body$end_date   <- end_date

  resp <- request(paste0(KINSA_BASE_URL, "/signal")) |>
    req_method("POST") |>
    req_headers(
      "Content-Type"  = "application/json",
      "Authorization" = paste("Bearer", access_token)
    ) |>
    req_body_json(body) |>
    req_error(body = \(r) resp_body_string(r)) |>
    req_perform()

  records <- resp_body_json(resp, simplifyVector = TRUE)
  if (is.data.frame(records)) records else bind_rows(records)
}

# -----------------------------------------------------------------------------
# 1. Authenticate and check latest available date
# -----------------------------------------------------------------------------

auth        <- kinsa_login()
token       <- auth$access_token
latest_date <- kinsa_latest_date(token)
message("Latest available date: ", latest_date)

# Change detection: only re-process when new data exists
if (!identical(process$last_date, latest_date)) {

  # ---------------------------------------------------------------------------
  # 2. Determine pull range from raw cache
  # ---------------------------------------------------------------------------

  raw_file      <- "raw/kinsa.csv.gz"
  default_start <- as.Date("2019-01-01")

  existing_df <- NULL
  if (file.exists(raw_file)) {
    existing_df <- vroom::vroom(raw_file, show_col_types = FALSE,
                                col_types = vroom::cols(yyyymmdd = vroom::col_character())) %>%
      filter(!is.na(yyyymmdd))
    start       <- max(as.Date(existing_df$yyyymmdd), na.rm = TRUE) + 1
    message("Cache found (", nrow(existing_df), " rows). Pulling from: ", start)
  } else {
    start <- default_start
    message("No cache found. Pulling all data from: ", start)
  }

  end <- as.Date(latest_date)

  # ---------------------------------------------------------------------------
  # 3. Pull new data in yearly chunks (API limit: <= 1 year per request)
  # ---------------------------------------------------------------------------

  if (start <= end) {
    chunks      <- list()
    chunk_start <- start

    while (chunk_start <= end) {
      chunk_end <- min(chunk_start + 364, end)
      message("Fetching ", format(chunk_start, "%Y%m%d"), " to ",
              format(chunk_end,   "%Y%m%d"), "...")
      chunks[[length(chunks) + 1]] <- kinsa_signal(
        access_token = token,
        region_type  = "national",
        signal_type  = "COUGH_COLD_FLU",
        start_date   = format(chunk_start, "%Y%m%d"),
        end_date     = format(chunk_end,   "%Y%m%d")
      )
      chunk_start <- chunk_end + 1
    }

    new_df <- bind_rows(chunks)
    message("New rows fetched: ", nrow(new_df))

    # Append to cache, deduplicate
    raw_combined <- bind_rows(existing_df, new_df) %>%
      distinct() %>%
      arrange(yyyymmdd)

    vroom::vroom_write(raw_combined, raw_file, delim = ",")
    message("Saved ", nrow(raw_combined), " total rows to ", raw_file)
  } else {
    raw_combined <- existing_df
    message("Cache is already up to date.")
  }

  # ---------------------------------------------------------------------------
  # 4. Standardize and write output
  # ---------------------------------------------------------------------------

  data_standard <- raw_combined %>%
    mutate(
      geography = "00",
      time      = format(as.Date(yyyymmdd), "%Y-%m-%d")
    ) %>%
    select(geography, time, kinsa_cough_cold_flu = percent_ill) %>%
    filter(!is.na(kinsa_cough_cold_flu))

  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")
  message("Wrote ", nrow(data_standard), " rows to standard/data.csv.gz")

  # ---------------------------------------------------------------------------
  # 5. Update process record
  # ---------------------------------------------------------------------------

  process$last_date <- latest_date
  dcf::dcf_process_record(updated = process)
}
