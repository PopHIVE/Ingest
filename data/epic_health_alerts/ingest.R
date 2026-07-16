# =============================================================================
# Epic Research Health Alerts - ALL conditions
# Source: https://www.epicresearch.org/health-alerts/
#
# Notes:
#   - The page has no API/CSV export. It is a server-rendered (Next.js) HTML
#     page with one <table> per health-alert condition. As of this writing the
#     conditions are: Acute Pharyngitis, Cyclosporiasis, Hand, Foot, and Mouth
#     Disease, Heat Illness, Toxic Effect of Smoke, and Viral Gastroenteritis.
#     All conditions are present in the same page load behind a condition
#     filter UI - there is no separate per-condition URL or AJAX endpoint.
#     This script downloads the full page and extracts EVERY condition table by
#     iterating over the page's <section class="health-alerts-component">
#     blocks (each carries its condition name in an <h3>). The condition list
#     is therefore discovered from the page, not hardcoded - new conditions are
#     picked up automatically.
#   - The page shows only CURRENTLY ACTIVE alerts (a live snapshot), not a
#     historical archive. To build a real time series, this script appends the
#     current snapshot into standard/data.csv.gz on each run rather than
#     overwriting it. Rows are keyed on geography + condition + time (the week
#     ending Saturday), so re-running within the same week refreshes that
#     week's snapshot instead of duplicating it. A geography/condition that
#     drops off the page simply stops receiving new rows.
#   - Each row records two provenance dates: `date_scraped` (the date this
#     script pulled the page) and `date_epic_updated` (the page's own
#     "Last updated: <Month DD, YYYY>" banner, a single page-wide value that
#     applies to every condition).
#   - Rows are State-Wide or county-level depending on what Epic Research
#     issues; "State-Wide" rows use the 2-digit state FIPS, county rows are
#     matched to 5-digit county FIPS via resources/all_fips.csv.gz.
#   - Values marked with a trailing "*" on the source page are rates computed
#     from a partial (incomplete) reporting week; this is captured in
#     `partial_week_flag` rather than discarded.
# =============================================================================

library(dplyr)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
url <- "https://www.epicresearch.org/health-alerts/"
download.file(url, "raw/health-alerts.html", mode = "wb", quiet = TRUE)
raw_state <- list(hash = unname(tools::md5sum("raw/health-alerts.html")))

# Only process if the page has changed since the last run
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Load FIPS lookup
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  state_abbr_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(state_fips = geography, state)

  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5) %>%
    select(geography, geography_name, state) %>%
    mutate(
      county_name = sub(" County$", "", geography_name),
      county_name = sub(" Parish$", "", county_name),
      county_name = sub(" Borough$", "", county_name),
      county_name = sub(" Census Area$", "", county_name),
      county_name = sub(" Municipality$", "", county_name)
    ) %>%
    select(geography, county_name, state)

  # ---------------------------------------------------------------------------
  # 3. Read raw data - extract every condition's table
  # ---------------------------------------------------------------------------
  page <- xml2::read_html("raw/health-alerts.html")

  # One <section class="health-alerts-component"> per condition; the condition
  # name is its <h3>, its data is the single <table> inside it.
  sections <- rvest::html_elements(page, "section.health-alerts-component")
  if (length(sections) == 0) {
    stop("No health-alert sections found on the page; site layout may have changed.")
  }
  headings <- rvest::html_text2(rvest::html_element(sections, "h3"))

  # The page's own "Last updated: <Month DD, YYYY>" banner - a single,
  # page-wide value that applies to every condition.
  last_updated_node <- rvest::html_element(page, xpath = "//p[contains(text(), 'Last updated')]")
  date_epic_updated <- format(
    as.Date(trimws(sub("Last updated:\\s*", "", rvest::html_text2(last_updated_node))), "%B %d, %Y"),
    "%m-%d-%Y"
  )

  # Pull each section's table and tag it with its condition. Sections whose
  # table is empty (a condition with no active alerts) are dropped.
  tables <- lapply(seq_along(sections), function(i) {
    tbl_node <- rvest::html_element(sections[[i]], "table")
    if (inherits(tbl_node, "xml_missing")) return(NULL)
    tbl <- rvest::html_table(tbl_node, fill = TRUE)
    if (nrow(tbl) == 0) return(NULL)
    # Force every column to character: rvest types the rate column as double
    # for tables with no partial-week "*" and as character for those with one,
    # which would otherwise break bind_rows() across conditions.
    tbl <- dplyr::mutate(tbl, dplyr::across(dplyr::everything(), as.character))
    tbl$condition <- headings[i]
    tbl
  })
  data_raw <- bind_rows(tables)

  if (nrow(data_raw) == 0) {
    message("No active health alerts on the page; nothing to append this run.")
    process$raw_state <- raw_state
    dcf::dcf_process_record(updated = process)
    quit(save = "no")
  }

  # ---------------------------------------------------------------------------
  # 4. Transform to standard long format (one row per geography x condition)
  # ---------------------------------------------------------------------------
  scrape_date <- Sys.Date()
  wday <- as.integer(format(scrape_date, "%u")) # Mon = 1 ... Sun = 7
  week_ending_saturday <- scrape_date + ((6 - wday) %% 7)

  data_new <- data_raw %>%
    rename(
      state_name = State,
      county_raw = County,
      estimated_onset = `Estimated Onset`,
      rate_raw = `Cases per 100k (Latest Week)`
    ) %>%
    mutate(
      # Trailing "*" on the source page marks a partial (incomplete) week
      partial_week_flag = as.integer(grepl("\\*", rate_raw)),
      value = as.numeric(gsub("[^0-9.]", "", rate_raw)),
      estimated_onset = format(as.Date(estimated_onset, "%m/%d/%Y"), "%m-%d-%Y"),
      county_clean = trimws(gsub("^\\(|\\)$", "", county_raw)),
      is_statewide = county_clean == "State-Wide"
    ) %>%
    left_join(state_fips_lookup, by = c("state_name" = "geography_name")) %>%
    rename(state_geo = geography) %>%
    left_join(state_abbr_lookup, by = c("state_geo" = "state_fips")) %>%
    # County names on the page match Census title-case names directly (e.g.
    # "Los Angeles", "Pottawattamie"). If future county alerts fail to match,
    # apply the normalize_county_name() approach documented in CLAUDE.md.
    left_join(county_fips_lookup, by = c("state" = "state", "county_clean" = "county_name")) %>%
    mutate(
      geography = if_else(is_statewide, state_geo, geography),
      time = format(week_ending_saturday, "%m-%d-%Y"),
      date_scraped = format(scrape_date, "%m-%d-%Y"),
      date_epic_updated = date_epic_updated
    ) %>%
    select(
      geography, time, condition,
      estimated_onset, value, partial_week_flag,
      date_scraped, date_epic_updated
    )

  if (any(is.na(data_new$geography))) {
    unmatched <- data_raw[is.na(data_new$geography), ]
    warning(
      "Some rows could not be matched to a FIPS geography: ",
      paste(unmatched$State, unmatched$County, unmatched$condition, sep = "/", collapse = "; ")
    )
  }

  # ---------------------------------------------------------------------------
  # 5. Append to standardized output (accumulate weekly snapshots over time,
  #    since the source page only ever shows the current active alerts).
  # ---------------------------------------------------------------------------
  out_file <- "standard/data.csv.gz"
  if (file.exists(out_file)) {
    # Force character types on FIPS/date columns so leading zeros ("01", "00")
    # and the MM-DD-YYYY date strings survive the re-read and bind_rows below.
    data_prior <- vroom::vroom(
      out_file,
      col_types = vroom::cols(
        geography = vroom::col_character(),
        time = vroom::col_character(),
        condition = vroom::col_character(),
        estimated_onset = vroom::col_character(),
        date_scraped = vroom::col_character(),
        date_epic_updated = vroom::col_character(),
        .default = vroom::col_guess()
      )
    )

    # data_new first so that re-running within the same week refreshes that
    # week's snapshot (newest scrape wins) rather than duplicating it.
    data_standard <- bind_rows(data_new, data_prior) %>%
      distinct(geography, condition, time, .keep_all = TRUE) %>%
      arrange(condition, geography, time)
  } else {
    data_standard <- data_new %>% arrange(condition, geography, time)
  }

  vroom::vroom_write(data_standard, out_file, delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
