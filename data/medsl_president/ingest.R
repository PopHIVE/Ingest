# =============================================================================
# MIT Election Data and Science Lab: County Presidential Election Returns
# Source: https://doi.org/10.7910/DVN/VOQCHQ (Harvard Dataverse)
#
# Votes for president by county and party for every general election since
# 2000. The file is downloaded again whenever Dataverse publishes a new version
# of the dataset; otherwise the copy in raw/ is used.
#
# Alaska reports by election district, not borough, so it is in the state file
# only. Rows without a county FIPS (statewide write-ins, overseas ballots) also
# count toward the state totals only.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
dataverse <- "https://dataverse.harvard.edu/api"
list_raw <- function() {
  sort(list.files("raw", pattern = "^countypres.*[.](csv|tab)[.]gz$", full.names = TRUE))
}

# Dataverse issues a signed download link in reply to a guestbook response
dataverse_version <- tryCatch({
  meta <- jsonlite::fromJSON(
    paste0(dataverse, "/datasets/:persistentId/?persistentId=doi:10.7910/DVN/VOQCHQ"),
    simplifyVector = FALSE
  )$data$latestVersion
  version <- paste0(meta$versionNumber, ".", meta$versionMinorNumber)

  if (length(list_raw()) == 0 || !identical(version, process$medsl_version)) {
    files <- lapply(meta$files, `[[`, "dataFile")
    returns <- Filter(function(f) grepl("^countypres", f$filename), files)
    if (length(returns) != 1) stop("expected one countypres file, found ", length(returns))
    returns <- returns[[1]]

    signed <- httr::POST(
      paste0(dataverse, "/access/datafile/", returns$id, "?format=original"),
      body = "{}", httr::content_type_json()
    )
    httr::stop_for_status(signed)
    csv_tmp <- tempfile(fileext = ".csv")
    download.file(httr::content(signed)$data$signedUrl, csv_tmp, mode = "wb", quiet = TRUE)
    if (!grepl("county_fips", readLines(csv_tmp, n = 1))) stop("download is not the returns file")

    csv_name <- returns$originalFileName
    if (is.null(csv_name)) csv_name <- sub("[.]tab$", ".csv", returns$filename)
    unlink(list_raw())
    con <- gzfile(file.path("raw", paste0(csv_name, ".gz")), "wb")
    writeBin(readBin(csv_tmp, "raw", file.size(csv_tmp)), con)
    close(con)
  }
  version
}, error = function(e) {
  message("MEDSL download failed, using the copy in raw/: ", conditionMessage(e))
  NULL
})

raw_files <- list_raw()
if (length(raw_files) == 0) {
  stop("no raw/countypres_*.csv.gz; download it from https://doi.org/10.7910/DVN/VOQCHQ")
}
raw_file <- raw_files[length(raw_files)]
raw_state <- list(hash = tools::md5sum(raw_file)[[1]])

# -----------------------------------------------------------------------------
# 2. Check for changes
# -----------------------------------------------------------------------------
if (!identical(process$raw_state, raw_state)) {

  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }

  raw <- vroom::vroom(
    raw_file,
    col_types = vroom::cols(.default = "c"),
    na = c("", "NA"),
    altrep = FALSE,
    show_col_types = FALSE
  )
  needed <- c("year", "state", "state_po", "county_name", "county_fips", "party",
              "candidatevotes", "totalvotes")
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) stop("columns not found: ", paste(absent, collapse = ", "))
  if (!"mode" %in% names(raw)) raw$mode <- "TOTAL"

  # ---------------------------------------------------------------------------
  # 3. One row per county and election
  # ---------------------------------------------------------------------------
  # From 2020 some counties are split by voting mode (election day, absentee,
  # early, ...), with or without a TOTAL row. Use TOTAL where there is one and
  # add up the modes where there is not. totalvotes is the county total on
  # every row.
  #
  # Kansas City, MO is reported apart from the rest of Jackson County, Bedford
  # city, VA merged into Bedford County in 2013, and Shannon County, SD became
  # Oglala Lakota County in 2015.
  county_fips_crosswalk <- c(
    "36000" = "29095", "2938000" = "29095", "51515" = "51019", "46113" = "46102"
  )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2, geography != "00") %>%
    transmute(state_fips = geography, state_key = toupper(geography_name))

  votes <- raw %>%
    mutate(state_key = toupper(state)) %>%
    left_join(state_fips_lookup, by = "state_key")
  if (any(is.na(votes$state_fips))) {
    stop("states without a FIPS code: ",
         paste(unique(votes$state[is.na(votes$state_fips)]), collapse = ", "))
  }

  votes <- votes %>%
    mutate(
      party = toupper(party),
      mode = toupper(coalesce(mode, "")),
      candidatevotes = as.numeric(candidatevotes),
      totalvotes = as.numeric(totalvotes),
      unit = paste(county_name, county_fips),
      county_fips = recode(county_fips, !!!county_fips_crosswalk),
      county_fips = if_else(
        is.na(county_fips), NA_character_,
        sprintf("%05d", as.integer(county_fips))
      )
    ) %>%
    group_by(year, state_fips, unit) %>%
    filter(!any(mode == "TOTAL") | mode == "TOTAL") %>%
    ungroup()

  by_unit <- votes %>%
    group_by(year, state_fips, unit, county_name, county_fips) %>%
    summarize(
      votes_dem = sum(candidatevotes[party == "DEMOCRAT"], na.rm = TRUE),
      votes_rep = sum(candidatevotes[party == "REPUBLICAN"], na.rm = TRUE),
      votes_total = max(totalvotes, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(votes_total = if_else(is.finite(votes_total), votes_total, NA_real_))

  add_measures <- function(d) {
    d %>%
      summarize(
        medsl_votes_total = sum(votes_total, na.rm = TRUE),
        medsl_votes_dem = sum(votes_dem),
        medsl_votes_rep = sum(votes_rep),
        .groups = "drop"
      ) %>%
      mutate(
        time = paste0(year, "-12-31"),
        medsl_pct_dem = round(100 * medsl_votes_dem / medsl_votes_total, 1),
        medsl_pct_rep = round(100 * medsl_votes_rep / medsl_votes_total, 1),
        medsl_pct_rep_two_party = round(
          100 * medsl_votes_rep / (medsl_votes_dem + medsl_votes_rep), 1
        )
      ) %>%
      filter(medsl_votes_total > 0)
  }
  out_cols <- c("geography", "time", "medsl_votes_total", "medsl_votes_dem", "medsl_votes_rep",
                "medsl_pct_dem", "medsl_pct_rep", "medsl_pct_rep_two_party")

  # ---------------------------------------------------------------------------
  # 4. County file
  # ---------------------------------------------------------------------------
  county_units <- by_unit %>% filter(state_fips != "02", !is.na(county_fips))

  not_county <- county_units %>% filter(!county_fips %in% all_fips$geography)
  if (nrow(not_county) > 0) {
    message("left out of the county file (FIPS not in all_fips): ",
            paste(unique(paste(not_county$county_name, not_county$county_fips)),
                  collapse = "; "))
  }

  data_county <- county_units %>%
    filter(county_fips %in% all_fips$geography) %>%
    group_by(geography = county_fips, year) %>%
    add_measures() %>%
    select(all_of(out_cols)) %>%
    arrange(geography, time) %>%
    check_unique(c("geography", "time"), "data_county")

  # ---------------------------------------------------------------------------
  # 5. State and national file
  # ---------------------------------------------------------------------------
  data_state <- bind_rows(
    by_unit %>% group_by(geography = state_fips, year) %>% add_measures(),
    by_unit %>% group_by(geography = "00", year) %>% add_measures()
  ) %>%
    select(all_of(out_cols)) %>%
    arrange(geography, time) %>%
    check_unique(c("geography", "time"), "data_state")

  # ---------------------------------------------------------------------------
  # 6. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")
  vroom::vroom_write(data_state, "standard/data_state.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 7. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  if (!is.null(dataverse_version)) process$medsl_version <- dataverse_version
  dcf::dcf_process_record(updated = process)
}
