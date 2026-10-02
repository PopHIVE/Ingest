# =============================================================================
# Census Population Estimates Program (PEP) -- population by age and sex
# Source: https://www.census.gov/programs-surveys/popest.html (Census Data API)
#   2010-2019: Vintage 2019  api/data/2019/pep/charagegroups (5-year age groups)
#   2020-2023: Vintage 2023  api/data/2023/pep/charv         (5-year age groups)
# Geography: County, State, National.  Time: annual (July 1 estimate).
# Requires a free Census API key in the CENSUS_API_KEY environment variable
# (https://api.census.gov/data/key_signup.html).
#
# Used as the age x sex denominator for rates (e.g. NHTSA youth traffic
# fatality rates in bundle_youth_wellbeing). Ages are limited to the under-25
# 5-year bands plus the all-ages total.
# =============================================================================

library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()

api_key <- Sys.getenv("CENSUS_API_KEY")
if (!nzchar(api_key)) stop("CENSUS_API_KEY environment variable is not set")

# -----------------------------------------------------------------------------
# 1. Download raw data (one raw file per vintage)
# -----------------------------------------------------------------------------
census_get <- function(url, tries = 3) {
  for (i in seq_len(tries)) {
    out <- tryCatch(jsonlite::fromJSON(url), error = function(e) NULL)
    if (!is.null(out)) {
      df <- as.data.frame(out[-1, , drop = FALSE], stringsAsFactors = FALSE)
      names(df) <- out[1, ]
      # Predicates in the query (e.g. &AGE=...) are echoed back as extra columns
      return(df[, !duplicated(names(df)), drop = FALSE])
    }
    Sys.sleep(2 * i)
  }
  stop("Census API request failed: ", sub("key=.*", "key=<hidden>", url))
}

state_codes <- sprintf("%02d", c(1, 2, 4:6, 8:13, 15:42, 44:51, 53:56))  # 50 states + DC

# Fetch national, state, and per-state county rows for one vintage.
#   dataset  : e.g. "2023/pep/charv"
#   get      : variables to request
#   filters  : character vector of predicate strings, one request set each
#              (charv rejects comma-separated AGE values), e.g. "&AGE=0401&MONTH=7"
fetch_vintage <- function(dataset, get, filters) {
  message("Fetching ", dataset, "...")
  bind_rows(lapply(filters, function(f) {
    base <- paste0("https://api.census.gov/data/", dataset, "?get=", get, f)
    nat <- census_get(paste0(base, "&for=us:*&key=", api_key)) %>%
      mutate(geography = "00", .keep = "unused")
    sta <- census_get(paste0(base, "&for=state:*&key=", api_key)) %>%
      mutate(geography = state, .keep = "unused")
    cou <- lapply(state_codes, function(s) {
      census_get(paste0(base, "&for=county:*&in=state:", s, "&key=", api_key)) %>%
        mutate(geography = paste0(state, county), .keep = "unused")
    })
    message("  done: ", f)
    bind_rows(nat, sta, cou)
  }))
}

# 5-year age groups under 25 plus all-ages total; all sexes
raw_2019 <- fetch_vintage(
  "2019/pep/charagegroups", "POP,AGEGROUP,SEX,DATE_CODE,DATE_DESC",
  "&AGEGROUP=0,1,2,3,4,5"
)
raw_2023 <- fetch_vintage(
  "2023/pep/charv", "POP,AGE,SEX,YEAR,MONTH",
  paste0("&MONTH=7&AGE=", c("0000", "0401", "0509", "1014", "1519", "2024"))
)

vroom::vroom_write(raw_2019, "raw/pep_charagegroups_2019.csv.xz", delim = ",")
vroom::vroom_write(raw_2023, "raw/pep_charv_2023.csv.xz", delim = ",")

raw_state <- list(
  charagegroups_2019 = unname(tools::md5sum("raw/pep_charagegroups_2019.csv.xz")),
  charv_2023         = unname(tools::md5sum("raw/pep_charv_2023.csv.xz"))
)

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Standardize
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  sex_labels <- c("0" = "Overall", "1" = "Male", "2" = "Female")

  d2019 <- raw_2019 %>%
    # Keep the July 1 estimates for 2010-2019 (drops the April 2010 census /
    # estimates-base rows, which carry the same year).
    filter(grepl("^7/1/\\d{4} population estimate", DATE_DESC)) %>%
    transmute(
      geography,
      year = as.integer(sub("^7/1/(\\d{4}).*", "\\1", DATE_DESC)),
      age = c("0" = "Overall", "1" = "0-4", "2" = "5-9", "3" = "10-14",
              "4" = "15-19", "5" = "20-24")[AGEGROUP],
      sex = sex_labels[SEX],
      pep_population = as.numeric(POP)
    )

  d2023 <- raw_2023 %>%
    transmute(
      geography,
      year = as.integer(YEAR),
      age = c("0000" = "Overall", "0401" = "0-4", "0509" = "5-9", "1014" = "10-14",
              "1519" = "15-19", "2024" = "20-24")[AGE],
      sex = sex_labels[SEX],
      pep_population = as.numeric(POP)
    )

  data_standard <- bind_rows(d2019, d2023) %>%
    filter(geography %in% all_fips$geography, !is.na(age), !is.na(sex)) %>%
    mutate(time = paste0(year, "-12-31")) %>%
    select(geography, time, age, sex, pep_population) %>%
    distinct() %>%
    arrange(geography, time, age, sex)

  stopifnot(!anyDuplicated(data_standard[c("geography", "time", "age", "sex")]))

  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 4. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
