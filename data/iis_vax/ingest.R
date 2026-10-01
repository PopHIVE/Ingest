# =============================================================================
# IIS: Monthly Cumulative Influenza and RSV Immunization by Jurisdiction
# Sources (CDC RespVaxView, Immunization Information Systems submissions):
#   ivdz-qhnr  flu vaccination, children and adults by age group, 2022-23 to present
#   n2zz-25mk  RSV vaccination, adults 75+, 2024-25
#   2yum-eg9f  RSV vaccination, adults 75+, 2025-26
#   4bdk-kyzv  RSV monoclonal antibody (nirsevimab), infants <8 months, 2024-25
#   vhcj-3k53  RSV monoclonal antibody (nirsevimab or clesrovimab), infants <8 months, 2025-26
#
# Jurisdictions submit aggregate counts to CDC monthly. Estimates are cumulative
# from the start of the season (July 1 for flu, April 1 for infant RSV
# antibodies) and use prior-year Census denominators. There is no national
# estimate because not every jurisdiction reports; 54 jurisdictions reported
# through 2024-25 but only about half did in 2025-26. Rows for months a
# jurisdiction did not submit are dropped.
#
# New York and Pennsylvania report excluding New York City and Philadelphia
# County, which report separately; the state rows are kept under the state
# FIPS with partial_state_flag = 1. Cities and the freely associated states
# (which have no FIPS) go to data_substate.csv.gz.
#
# CDC issues a new dataset ID for each RSV season; add the new ID each fall.
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

DATASETS <- list(
  flu     = c("ivdz-qhnr"),
  rsv     = c("n2zz-25mk", "2yum-eg9f"),
  rsv_mab = c("4bdk-kyzv", "vhcj-3k53")
)
all_ids <- unlist(DATASETS, use.names = FALSE)
new_state <- lapply(all_ids, function(id) dcf::dcf_download_cdc(id, "raw", process$raw_state[[id]]))
names(new_state) <- all_ids

read_raw <- function(id) {
  d <- vroom::vroom(
    sprintf("raw/%s.csv.xz", id), delim = ",",
    col_types = vroom::cols(.default = "c"), altrep = FALSE, show_col_types = FALSE
  )
  season_col <- intersect(c("Current Season", "Season", "season"), names(d))
  needed <- c("Month", "Numerator", "Population", "Jurisdiction", "Estimate", "Age_group_label")
  absent <- setdiff(needed, names(d))
  if (length(absent) > 0 || length(season_col) != 1) {
    stop(id, " columns not found: ", paste(c(absent, if (length(season_col) != 1) "season"), collapse = ", "))
  }
  d %>% transmute(
    season = .data[[season_col[1]]], month = toupper(Month), numerator = Numerator,
    population = Population, jurisdiction = trimws(Jurisdiction), estimate = Estimate,
    age = Age_group_label
  )
}
# Season month (JUL..JUN) -> last day of that calendar month
month_end <- function(season, month) {
  y1 <- as.integer(substr(season, 1, 4))
  m <- match(month, toupper(month.abb))
  if (any(is.na(m))) stop("unrecognised month: ", paste(unique(month[is.na(m)]), collapse = ", "))
  y <- ifelse(m >= 7, y1, y1 + 1)
  first_next <- as.Date(sprintf("%d-%02d-01", ifelse(m == 12, y + 1, y), ifelse(m == 12, 1, m + 1)))
  first_next - 1
}

# CDC publishes each RSV season as a new dataset ID. From November to March the
# newest RSV date should fall in the current season; if it does not, the ID
# list above needs the new season's dataset. message() rather than warning()
# so the text lands in the dcf process log.
check_rsv_current <- function(dates, ids, today = Sys.Date()) {
  m <- as.integer(format(today, "%m")); y <- as.integer(format(today, "%Y"))
  season_start <- as.Date(sprintf("%d-10-01", if (m >= 7) y else y - 1))
  latest <- suppressWarnings(max(dates, na.rm = TRUE))
  if ((m >= 11 || m <= 3) && is.finite(latest) && latest < season_start) {
    message("Warning: latest RSV data is ", latest, "; CDC has probably published ",
            "the current season under a new dataset ID (current: ",
            paste(ids, collapse = ", "), ")")
  }
}

if (!identical(process$raw_state, new_state)) {

  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    d
  }
  slug <- function(x) gsub("^_|_$", "", gsub("[^a-z0-9]+", "_", tolower(x)))


  # Territories have no name in all_fips.csv.gz
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  geo_lookup <- bind_rows(
    all_fips %>% filter(nchar(geography) == 2, !is.na(geography_name)) %>% select(geography, geography_name),
    tibble(
      geography = c("72", "66", "78", "60", "69"),
      geography_name = c("Puerto Rico", "Guam", "U.S. Virgin Islands", "American Samoa", "N. Mariana Islands")
    )
  ) %>% distinct(geography_name, .keep_all = TRUE)

  # Jurisdictions reported as a state minus a separately reporting city
  PARTIAL_STATE <- c(
    "New York" = "36", "New York State" = "36", "New York (excluding New York City)" = "36",
    "Pennsylvania" = "42", "Pennsylvania (excluding Philadelphia County)" = "42"
  )
  # Non-FIPS jurisdictions: cities and freely associated states
  SUBSTATE_STATE <- c(
    "New York City" = "36", "Philadelphia" = "42", "Chicago" = "17", "Houston" = "48",
    "Federated States of Micronesia" = NA, "Marshall Islands" = NA, "Republic of Palau" = NA
  )

  base <- bind_rows(lapply(names(DATASETS), function(v) {
    bind_rows(lapply(DATASETS[[v]], read_raw)) %>% mutate(vaccine = v)
  })) %>%
    mutate(
      time = month_end(season, month),
      iis_n_vaccinated = suppressWarnings(as.numeric(gsub(",", "", numerator))),
      iis_population   = suppressWarnings(as.numeric(gsub(",", "", population))),
      iis_coverage     = suppressWarnings(as.numeric(estimate)) * 100
    )

  bad_num <- unique(base$numerator[is.na(base$iis_n_vaccinated) & !is.na(base$numerator) & base$numerator != "Not Submitted"])
  if (length(bad_num) > 0) stop("unexpected numerator values: ", paste(bad_num, collapse = ", "))
  if (any(base$iis_coverage > 150, na.rm = TRUE)) stop("Estimate does not look like a proportion")

  base <- base %>%
    filter(!(is.na(iis_n_vaccinated) & is.na(iis_coverage))) %>%
    mutate(
      geography = case_when(
        jurisdiction %in% names(PARTIAL_STATE) ~ unname(PARTIAL_STATE[jurisdiction]),
        TRUE ~ geo_lookup$geography[match(jurisdiction, geo_lookup$geography_name)]
      ),
      partial_state_flag = as.integer(jurisdiction %in% names(PARTIAL_STATE)),
      substate = jurisdiction %in% names(SUBSTATE_STATE),
      time = format(time, "%Y-%m-%d")
    )

  unmapped <- unique(base$jurisdiction[is.na(base$geography) & !base$substate])
  if (length(unmapped) > 0) stop("jurisdictions not mapped: ", paste(unmapped, collapse = ", "))

  data <- base %>%
    filter(!substate) %>%
    select(geography, time, season, vaccine, age, jurisdiction, partial_state_flag,
           iis_n_vaccinated, iis_population, iis_coverage) %>%
    arrange(vaccine, age, geography, time) %>%
    check_unique(c("geography", "time", "vaccine", "age"), "data")
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  data_substate <- base %>%
    filter(substate) %>%
    mutate(
      geography = slug(jurisdiction),
      state_fips = unname(SUBSTATE_STATE[jurisdiction]),
      geography_level = if_else(is.na(state_fips), "Freely associated state", "City")
    ) %>%
    select(geography, geography_name = jurisdiction, geography_level, state_fips,
           time, season, vaccine, age, iis_n_vaccinated, iis_population, iis_coverage) %>%
    arrange(vaccine, age, geography, time) %>%
    check_unique(c("geography", "time", "vaccine", "age"), "data_substate")
  vroom::vroom_write(data_substate, "standard/data_substate.csv.gz", ",")

  process$raw_state <- new_state
  dcf::dcf_process_record(updated = process)
}

rsv_id <- tail(DATASETS$rsv, 1)
rsv_raw <- read_raw(rsv_id)
check_rsv_current(month_end(rsv_raw$season, rsv_raw$month), rsv_id)
