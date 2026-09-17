# =============================================================================
# NIS-Teen: Vaccination Coverage among Adolescents (13-17 Years)
# Source: https://data.cdc.gov/d/ee48-w5t6 (CDC TeenVaxView)
#
# Annual state and national coverage estimates by vaccine, dose, and sex (HPV
# only) for ages 13-17 and 13-15, plus pooled 2018-2022 estimates by insurance,
# poverty, race/ethnicity, and urbanicity. HHS regions and sub-state local
# areas (TX-Bexar County, NY-City of New York, IL-Rest of state, ...) are
# dropped; Puerto Rico, Guam, and the U.S. Virgin Islands are kept.
# =============================================================================

library(dplyr)
library(tidyr)

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc("ee48-w5t6", "raw", process$raw_state)

if (!identical(process$raw_state, raw_state)) {

  # Territories have no name in all_fips.csv.gz
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  geo_lookup <- bind_rows(
    all_fips %>%
      filter(nchar(geography) == 2, !is.na(geography_name)) %>%
      select(geography, geography_name),
    tibble(
      geography = c("00", "72", "66", "78"),
      geography_name = c("United States", "Puerto Rico", "Guam", "U.S. Virgin Islands")
    )
  ) %>%
    distinct(geography_name, .keep_all = TRUE)

  raw <- vroom::vroom(
    "raw/ee48-w5t6.csv.xz",
    col_types = vroom::cols(.default = "c"),
    altrep = FALSE,
    show_col_types = FALSE
  )
  needed <- c("Vaccine/Sample", "Dose", "Geography Type", "Geography", "Survey Year",
              "Dimension Type", "Dimension", "Estimate (%)", "95% CI (%)", "Sample Size")
  absent <- setdiff(needed, names(raw))
  if (length(absent) > 0) {
    stop("ee48-w5t6 columns not found: ", paste(absent, collapse = ", "))
  }

  VALUE_COLS <- c("nis_teen_coverage", "nis_teen_coverage_lcl",
                  "nis_teen_coverage_ucl", "nis_teen_sample_size")

  base <- raw %>%
    rename(
      vax_sample     = `Vaccine/Sample`,
      dose_raw       = Dose,
      geo_type       = `Geography Type`,
      geography_name = Geography,
      survey_years   = `Survey Year`,
      dim_type       = `Dimension Type`,
      dim            = Dimension,
      estimate       = `Estimate (%)`,
      ci             = `95% CI (%)`,
      sample_size    = `Sample Size`
    ) %>%
    filter(
      geography_name == "United States" |
        (geo_type == "States/Local Areas" & !grepl("^[A-Z]{2}-", geography_name))
    ) %>%
    inner_join(geo_lookup, by = "geography_name") %>%
    # The dose labels use the U+2265 (>=) glyph; replace it at the byte level
    # so the script does not depend on a UTF-8 locale.
    mutate(
      vax_sample = gsub("\xe2\x89\xa5", ">=", vax_sample, useBytes = TRUE),
      dose_raw   = gsub("\xe2\x89\xa5", ">=", coalesce(dose_raw, ""), useBytes = TRUE),
      vaccine = case_when(
        vax_sample == "HPV"          ~ "HPV",
        vax_sample == "Tetanus"      ~ "Tdap",
        grepl("MenACWY", vax_sample) ~ "MenACWY",
        grepl("MMR", vax_sample)     ~ "MMR",
        grepl("HepB", vax_sample)    ~ "HepB",
        grepl("Hep A", vax_sample)   ~ "HepA",
        vax_sample == "Varicella"    ~ "Varicella"
      ),
      # Only HPV is reported by sex; the sex is a suffix on the dose label.
      sex = case_when(
        vaccine != "HPV"                        ~ "Overall",
        grepl("Males and Females$", dose_raw)   ~ "Overall",
        grepl("Females$", dose_raw)             ~ "Female",
        grepl("Males$", dose_raw)               ~ "Male",
        TRUE                                    ~ "Overall"
      ),
      # For MenACWY, MMR, HepB, and HepA the dose is part of the vaccine label.
      dose = case_when(
        vaccine == "HPV" ~ sub(",\\s*(Males and Females|Females|Males)$", "", dose_raw),
        dose_raw == ""   ~ sub("\\s+(MenACWY|MMR|HepB|Hep A)$", "", vax_sample),
        TRUE             ~ dose_raw
      ),
      dose = sub(" Among HPV Vaccination Initiators", " Among Initiators", dose),
      nis_teen_coverage    = suppressWarnings(as.numeric(estimate)),
      nis_teen_sample_size = suppressWarnings(as.numeric(sample_size))
    )

  unmapped <- unique(base$vax_sample[is.na(base$vaccine)])
  if (length(unmapped) > 0) {
    warning("Vaccine/Sample values not mapped (dropped): ", paste(unmapped, collapse = ", "))
  }

  base <- base %>%
    filter(!is.na(vaccine)) %>%
    separate(ci, into = c("nis_teen_coverage_lcl", "nis_teen_coverage_ucl"),
             sep = " to ", convert = TRUE, fill = "right")

  check_unique <- function(d, keys, label) {
    n_dup <- sum(duplicated(d[keys]))
    if (n_dup > 0) {
      stop(label, ": ", n_dup, " duplicate rows on ", paste(keys, collapse = ", "))
    }
    d
  }

  # Annual estimates by age group
  data_age <- base %>%
    filter(dim_type == "Age", grepl("^\\d{4}$", survey_years)) %>%
    mutate(
      time = paste0(survey_years, "-12-31"),
      age  = sub(" Years$", "", dim)
    ) %>%
    select(geography, time, survey_years, age, sex, vaccine, dose, all_of(VALUE_COLS)) %>%
    arrange(geography, time, vaccine, dose, sex, age) %>%
    check_unique(c("geography", "time", "age", "sex", "vaccine", "dose"), "data")

  vroom::vroom_write(data_age, "standard/data.csv.gz", ",")

  # Pooled 2018-2022 estimates by demographic group, ages 13-17. Each file
  # carries the pooled Overall row as its reference level.
  write_pooled <- function(dim_label, col, recode) {
    d <- base %>%
      filter(survey_years == "2018-2022", dim_type %in% c(dim_label, "Overall")) %>%
      mutate(
        time = "2022-12-31",
        !!col := if_else(dim_type == "Overall", "Overall", unname(recode[dim]))
      )
    unknown <- unique(d$dim[is.na(d[[col]])])
    if (length(unknown) > 0) {
      warning(dim_label, " levels not recoded (dropped): ", paste(unknown, collapse = ", "))
    }
    d <- d %>%
      filter(!is.na(.data[[col]])) %>%
      select(geography, time, survey_years, all_of(col), sex, vaccine, dose, all_of(VALUE_COLS)) %>%
      arrange(geography, .data[[col]], vaccine, dose, sex) %>%
      check_unique(c("geography", "time", col, "sex", "vaccine", "dose"), paste0("data_", col))
    vroom::vroom_write(d, sprintf("standard/data_%s.csv.gz", col), ",")
  }

  write_pooled("Insurance Coverage", "insurance", c(
    "Uninsured" = "Uninsured",
    "Any Medicaid" = "Medicaid",
    "Private Insurance Only" = "Private",
    "Other" = "Other"
  ))
  write_pooled("Poverty", "poverty", c(
    "Below Poverty Level" = "Below Poverty",
    "Living At or Above Poverty Level" = "At or Above Poverty"
  ))
  write_pooled("Race and Ethnicity", "race_ethnicity", c(
    "White, Non-Hispanic" = "White",
    "Black, Non-Hispanic" = "Black",
    "Hispanic" = "Hispanic",
    "Other or Multiple Races, Non-Hispanic" = "Other or Multiple"
  ))
  write_pooled("Urbanicity", "urban", c(
    "Living In a Non-MSA" = "Rural",
    "Living In a MSA Non-Principal City" = "Smaller City",
    "Living In a MSA Principal City" = "Larger City"
  ))

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
