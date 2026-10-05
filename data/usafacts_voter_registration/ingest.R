# =============================================================================
# USAFacts: Voter Registration by Party Affiliation
# Source: https://usafacts.org/articles/more-voters-are-registering-outside-the-two-party-system/
#
# County voter registration totals and party shares (Democratic, Republican,
# other/unaffiliated) for the 27 states and DC that record party affiliation
# and publish it by county, compiled by USAFacts from state election offices.
# One report per year, the one closest to November. The csv is attached to a
# single article; if the link stops working the copy in raw/ is used.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
raw_url <- paste0(
  "https://cdn.builder.io/o/assets%2F0b9e002c2ba04655a10a2d191e448df9",
  "%2Fc211adb0735045418850b28dc602ea33?alt=media",
  "&token=1000d86d-f3a0-4c1e-abb9-48555f8408f7",
  "&apiKey=0b9e002c2ba04655a10a2d191e448df9"
)
raw_file <- "raw/voter_registration_county.csv.gz"

# Hash the uncompressed csv: the gzip bytes differ between runs
csv_tmp <- tempfile(fileext = ".csv")
downloaded <- tryCatch({
  download.file(raw_url, csv_tmp, mode = "wb", quiet = TRUE)
  file.size(csv_tmp) > 0
}, error = function(e) {
  message("USAFacts download failed, using ", raw_file, ": ", conditionMessage(e))
  FALSE
})
if (!downloaded) {
  if (!file.exists(raw_file)) stop("no download and no ", raw_file)
  writeLines(readLines(gzfile(raw_file), warn = FALSE), csv_tmp)
}

raw_state <- list(hash = tools::md5sum(csv_tmp)[[1]])

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
    csv_tmp,
    col_types = vroom::cols(.default = "c"),
    altrep = FALSE,
    show_col_types = FALSE
  )
  needed <- c("state", "geography_standardized", "year", "county_total_voters",
              "dem_share", "unaff_share", "rep_share")
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) stop("columns not found: ", paste(absent, collapse = ", "))

  if (downloaded) {
    con <- gzfile(raw_file, "wb")
    writeBin(readBin(csv_tmp, "raw", file.size(csv_tmp)), con)
    close(con)
  }

  # ---------------------------------------------------------------------------
  # 3. County names to FIPS
  # ---------------------------------------------------------------------------
  # Connecticut is reported for its eight former counties (09001-09015), so the
  # planning regions (09110-09190) are left out of the lookup.
  norm_name <- function(x) gsub("[^a-z]", "", tolower(x))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, !grepl("^091[1-9]", geography)) %>%
    mutate(
      county_key = norm_name(sub(
        " (County|Parish|Borough|Census Area|Municipality|City and Borough)$", "",
        geography_name
      ))
    ) %>%
    select(geography, state, county_key) %>%
    check_unique(c("state", "county_key"), "county lookup")

  # West Virginia's 2026 report has a stray "West" row that is not a county
  county <- raw %>%
    filter(!(state == "WV" & geography_standardized == "West")) %>%
    mutate(
      county_name = case_when(
        state == "MD" & geography_standardized == "BALTIMORE CO." ~ "Baltimore",
        state == "MD" & geography_standardized == "PR. GEORGE'S" ~ "Prince George's",
        TRUE ~ sub(" County$", "", geography_standardized)
      ),
      county_key = norm_name(county_name)
    ) %>%
    left_join(county_fips_lookup, by = c("state", "county_key"))

  unmatched <- county %>% filter(is.na(geography)) %>% distinct(state, geography_standardized)
  if (nrow(unmatched) > 0) {
    stop("counties without a FIPS code: ",
         paste(unmatched$state, unmatched$geography_standardized, collapse = "; "))
  }

  # ---------------------------------------------------------------------------
  # 4. Transform to standard wide format
  # ---------------------------------------------------------------------------
  data_county <- county %>%
    transmute(
      geography,
      time = paste0(year, "-12-31"),
      usafacts_voters_total = as.numeric(county_total_voters),
      usafacts_pct_dem = round(as.numeric(dem_share) * 100, 1),
      usafacts_pct_rep = round(as.numeric(rep_share) * 100, 1),
      usafacts_pct_other_unaffiliated = round(as.numeric(unaff_share) * 100, 1)
    ) %>%
    arrange(geography, time) %>%
    check_unique(c("geography", "time"), "data_county")

  # State rows: county shares weighted by registered voters
  data_state <- data_county %>%
    group_by(geography = substr(geography, 1, 2), time) %>%
    summarize(
      across(starts_with("usafacts_pct_"),
             ~ round(weighted.mean(.x, usafacts_voters_total), 1)),
      usafacts_voters_total = sum(usafacts_voters_total),
      .groups = "drop"
    ) %>%
    select(all_of(names(data_county))) %>%
    arrange(geography, time) %>%
    check_unique(c("geography", "time"), "data_state")

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")
  vroom::vroom_write(data_state, "standard/data_state.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
