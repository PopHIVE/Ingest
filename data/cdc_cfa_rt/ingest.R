# =============================================================================
# CDC CFA Epidemic Trends and Rt Data Ingestion
# Source: https://data.cdc.gov/d/ahfs-x44r
#         "Epidemic Trends and Rt: COVID-19, Influenza, and RSV at National,
#          State, Local Level" (CDC Center for Forecasting and Outbreak Analytics)
#
# NOTE - DATA LOCATION CHANGED (October 2026): CFA's updated Rt estimates are
# now published in ahfs-x44r. The previous dataset, 5dqz-y4ea ("CDC Epidemic
# Trends and Rt"), was emptied by CDC on 2026-10-23 and is no longer used here.
# Rows already in standard/data.csv.gz (2026-04-01 onward, from 5dqz-y4ea) are
# kept as history; ahfs-x44r replaces them wherever the two overlap.
#
# ahfs-x44r holds one row per county/HSA, state or national estimate for each
# disease, release (`origin_date`) and day (`target_date`). Only the national
# and state rows are used here (the HSA rows are county-level and are not part
# of this standardized file).
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record
process <- dcf::dcf_process_record()

# --- 1. Download raw data ---
# Query only the national and state rows; exporting the whole dataset would
# download every HSA estimate ever published (>2 million rows). The $order
# keeps the file byte-stable so the md5 used for change detection is too.
raw_file <- "raw/ahfs-x44r_state_national.csv.xz"
tmp_file <- tempfile(fileext = ".csv")
query_url <- paste0(
  "https://data.cdc.gov/resource/ahfs-x44r.csv",
  "?$where=estimate_level%20in(%27national%27,%27state%27)",
  "&$order=origin_date,estimate_level,state_code,disease,target_date",
  "&$limit=1000000"
)
utils::download.file(query_url, tmp_file, mode = "wb", quiet = TRUE)
raw_state <- list(hash = unname(tools::md5sum(tmp_file)))

# Only process if data has changed
if (!identical(process$ahfs_state, raw_state)) {

  # --- 2. Read raw data ---
  data_raw <- vroom::vroom(
    tmp_file,
    show_col_types = FALSE,
    col_types = vroom::cols(
      origin_date = vroom::col_date("%Y-%m-%d"),
      target_date = vroom::col_date("%Y-%m-%d"),
      state_code  = vroom::col_character(),
      median      = vroom::col_double(),
      lower_95    = vroom::col_double(),
      upper_95    = vroom::col_double(),
      p_growing   = vroom::col_double(),
      .default    = vroom::col_character()
    )
  )

  # Fail loudly rather than overwriting the standard file with nothing (CDC
  # emptied the previous dataset in October 2026).
  if (nrow(data_raw) == 0) {
    stop(
      "cdc_cfa_rt: ahfs-x44r returned 0 national/state rows - refusing to ",
      "overwrite standard/data.csv.gz. Check https://data.cdc.gov/d/ahfs-x44r."
    )
  }

  # One release in ahfs-x44r is labelled with `origin_date` in 2029 although
  # its target dates are all in 2026 (a CDC typo); treat 2029 as 2026. Any
  # release still dated in the future after that is dropped, since the
  # "latest release wins" step below would otherwise always prefer it.
  typo_year <- !is.na(data_raw$origin_date) &
    format(data_raw$origin_date, "%Y") == "2029"
  if (any(typo_year)) {
    message(
      "cdc_cfa_rt: treating origin_date year 2029 as 2026 (",
      sum(typo_year), " rows)"
    )
    data_raw$origin_date[typo_year] <- as.Date(
      sub("^2029", "2026", format(data_raw$origin_date[typo_year]))
    )
  }
  future <- !is.na(data_raw$origin_date) & data_raw$origin_date > Sys.Date()
  if (any(future)) {
    warning(
      "cdc_cfa_rt: dropping ", sum(future), " rows with origin_date after today"
    )
    data_raw <- data_raw[!future, ]
  }

  # --- 3. Transform data ---
  all_fips <- vroom::vroom(
    "../../resources/all_fips.csv.gz",
    show_col_types = FALSE
  )

  # Each release re-estimates the last 8 weeks, so keep the most recent
  # release (largest origin_date) for every location, disease and day.
  data_prepared <- data_raw |>
    mutate(
      disease_key = case_when(
        disease == "COVID-19"  ~ "covid",
        disease == "Influenza" ~ "flu",
        disease == "RSV"       ~ "rsv",
        TRUE ~ NA_character_
      )
    ) |>
    filter(!is.na(disease_key)) |>
    group_by(state_code, disease_key, target_date) |>
    filter(origin_date == max(origin_date, na.rm = TRUE)) |>
    ungroup() |>
    # state_code is already a FIPS code ("00" national, 2 digits for states)
    rename(geography = state_code, time = target_date) |>
    filter(
      geography %in% all_fips$geography,
      !is.na(median)
    ) |>
    select(
      geography, time, disease_key,
      median, lower = lower_95, upper = upper_95, p_growing
    ) |>
    distinct(geography, time, disease_key, .keep_all = TRUE)

  # Pivot to wide format: one row per geography/time, columns per disease
  data_new <- data_prepared |>
    pivot_wider(
      names_from  = disease_key,
      values_from = c(median, lower, upper, p_growing),
      names_glue  = "cdc_rt_{disease_key}_{.value}"
    ) |>
    rename_with(~ sub("_median$", "", .x), ends_with("_median"))

  # Guarantee a fixed column set even if a disease has zero estimates for the
  # whole period (e.g. flu/RSV fully "Not Estimated" off-season) - downstream
  # bundle_respiratory/build.R references these columns by name unconditionally
  expected_cols <- paste0(
    "cdc_rt_",
    rep(c("rsv", "flu", "covid"), times = 4),
    rep(c("", "_lower", "_upper", "_p_growing"), each = 3)
  )
  missing_cols <- setdiff(expected_cols, names(data_new))
  data_new[missing_cols] <- NA_real_
  data_new <- data_new[c("geography", "time", expected_cols)]

  if (nrow(data_new) == 0) {
    stop(
      "cdc_cfa_rt: transformation produced 0 rows - refusing to overwrite ",
      "standard/data.csv.gz. Check the release filter and that state_code ",
      "values match resources/all_fips.csv.gz."
    )
  }

  # --- 4. Combine with history and write standardized output ---
  # Rows from the retired dataset (5dqz-y4ea) live only in the existing
  # standard file. Keep any (geography, time) that ahfs-x44r does not cover.
  history_file <- "standard/data.csv.gz"
  if (file.exists(history_file)) {
    data_history <- vroom::vroom(
      history_file,
      show_col_types = FALSE,
      col_types = vroom::cols(
        geography = vroom::col_character(),
        time      = vroom::col_date("%Y-%m-%d"),
        .default  = vroom::col_double()
      )
    ) |>
      anti_join(data_new, by = c("geography", "time"))
  } else {
    data_history <- data_new[0, ]
  }

  data_wide <- bind_rows(data_history, data_new) |>
    select(all_of(c("geography", "time", expected_cols))) |>
    arrange(geography, time)

  vroom::vroom_write(data_wide, history_file, delim = ",")

  # Keep a compressed copy of the raw query result, exactly as downloaded
  unlink(raw_file)
  raw_con <- xzfile(raw_file, "wb")
  writeBin(readBin(tmp_file, "raw", file.size(tmp_file)), raw_con)
  close(raw_con)

  # --- 5. Update process record ---
  process$ahfs_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
