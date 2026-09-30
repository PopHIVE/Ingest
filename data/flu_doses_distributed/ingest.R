# =============================================================================
# Influenza Vaccine Doses Distributed, United States
# Source: ph8r-wzxn (CDC FluVaxView) - weekly cumulative doses (in millions)
#         distributed by U.S.-licensed manufacturers and their wholesalers,
#         2018-19 season to present. National only. Reflects distribution, not
#         administration.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc("ph8r-wzxn", "raw", process$raw_state)

if (!identical(process$raw_state, raw_state)) {

  raw <- vroom::vroom(
    "raw/ph8r-wzxn.csv.xz", delim = ",",
    col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
  )
  needed <- c("Influenza_Season", "Start_Date", "End_Date", "Cumulative_Flu_Doses_Distributed")
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) stop("ph8r-wzxn columns not found: ", paste(absent, collapse = ", "))

  parse_cdc_date <- function(x) {
    x <- substr(x, 1, 10)
    out <- as.Date(x, format = "%Y-%m-%d")
    i <- is.na(out)
    out[i] <- as.Date(x[i], format = "%m/%d/%Y")
    if (any(is.na(out) & !is.na(x))) stop("unparsed dates")
    out
  }
  # Reporting periods normally end on Saturday; the short year-end period in
  # 53-week years ends mid-week and is moved to the nearest Saturday (which
  # keeps it distinct from the neighbouring regular weeks).
  to_saturday <- function(d) {
    back <- (as.integer(format(d, "%u")) - 6) %% 7
    d - ifelse(back <= 3, back, back - 7)
  }
  norm_season <- function(x) sub("^(\\d{4})-(\\d{2})?(\\d{2})$", "\\1-\\3", x)

  data <- raw %>%
    transmute(
      geography = "00",
      time = format(to_saturday(parse_cdc_date(End_Date)), "%Y-%m-%d"),
      season = norm_season(Influenza_Season),
      flu_doses_cumulative_millions = as.numeric(Cumulative_Flu_Doses_Distributed)
    ) %>%
    arrange(time)

  n_dup <- sum(duplicated(data[c("geography", "time")]))
  if (n_dup > 0) stop("data: ", n_dup, " duplicate rows on geography, time")

  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
