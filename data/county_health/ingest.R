# =============================================================================
# County Health Rankings Data Ingestion
# Source: University of Wisconsin Population Health Institute
#         https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation
#
# Data file: raw/analytic_data2025_v3.csv
#   - Two header rows: row 1 = human-readable names, row 2 = machine column names
#   - Contains national (fipscode=00000), state (countycode=000), and county rows
#   - Annual release; all rows carry year=2025
#
# Outputs:
#   standard/data_state.csv.gz   -- national (geography="00") + 51 states
#   standard/data_county.csv.gz  -- 3000+ county rows (5-digit FIPS)
# =============================================================================

library(dplyr)
library(vroom)

# -----------------------------------------------------------------------------
# Initialize process record
# -----------------------------------------------------------------------------
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# Change detection via md5 hash
# -----------------------------------------------------------------------------
csv_path <- "raw/analytic_data2025_v3.csv"

if (!file.exists(csv_path)) {
  stop("Raw data file not found: ", csv_path,
       "\nDownload from https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation",
       " and place the analytic CSV in raw/")
}

current_hash <- list(hash = unname(tools::md5sum(csv_path)))

if (!identical(process$raw_state, current_hash)) {

  # ---------------------------------------------------------------------------
  # Read raw data
  # The CSV has two header rows. skip=1 discards the human-readable row and
  # uses the machine-name row (v001_rawvalue, etc.) as column names.
  # col_types = "c" for fipscode to preserve leading zeros.
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom(
    csv_path,
    skip = 1,
    col_types = c(fipscode = "c", statecode = "c", countycode = "c"),
    show_col_types = FALSE
  )

  # ---------------------------------------------------------------------------
  # Transform: geography, time, and select key rawvalue measures
  # ---------------------------------------------------------------------------
  data_standard <- data_raw %>%
    mutate(
      geography = case_when(
        fipscode == "00000" ~ "00",
        substr(fipscode, 3, 5) == "000" ~ substr(fipscode, 1, 2),
        TRUE ~ fipscode
      ),
      time = paste0(year, "-12-31")
    ) %>%
    select(
      geography,
      time,
      # Health outcomes
      chr_premature_death           = v001_rawvalue,
      chr_life_expectancy           = v147_rawvalue,
      chr_poor_or_fair_health       = v002_rawvalue,
      chr_poor_physical_health_days = v036_rawvalue,
      chr_poor_mental_health_days   = v042_rawvalue,
      chr_low_birth_weight          = v037_rawvalue,
      chr_premature_age_adj_mortality = v127_rawvalue,
      chr_child_mortality           = v128_rawvalue,
      chr_infant_mortality          = v129_rawvalue,
      chr_frequent_physical_distress = v144_rawvalue,
      chr_frequent_mental_distress  = v145_rawvalue,
      # Health behaviors
      chr_adult_smoking             = v009_rawvalue,
      chr_adult_obesity             = v011_rawvalue,
      chr_physical_inactivity       = v070_rawvalue,
      chr_excessive_drinking        = v049_rawvalue,
      chr_drug_overdose_deaths      = v138_rawvalue,
      chr_teen_births               = v014_rawvalue,
      chr_sexually_transmitted_infections = v045_rawvalue,
      chr_suicides                  = v161_rawvalue,
      chr_insufficient_sleep        = v143_rawvalue,
      # Clinical care
      chr_uninsured                 = v085_rawvalue,
      chr_uninsured_adults          = v003_rawvalue,
      chr_uninsured_children        = v122_rawvalue,
      chr_primary_care_physicians   = v004_rawvalue,
      chr_mental_health_providers   = v062_rawvalue,
      chr_preventable_hospital_stays = v005_rawvalue,
      chr_flu_vaccinations          = v155_rawvalue,
      chr_diabetes_prevalence       = v060_rawvalue,
      chr_hiv_prevalence            = v061_rawvalue,
      chr_mammography_screening     = v050_rawvalue,
      # Social & economic factors
      chr_unemployment              = v023_rawvalue,
      chr_median_household_income   = v063_rawvalue,
      chr_children_in_poverty       = v024_rawvalue,
      chr_income_inequality         = v044_rawvalue,
      chr_some_college              = v069_rawvalue,
      chr_high_school_completion    = v168_rawvalue,
      chr_high_school_graduation    = v021_rawvalue,
      chr_food_insecurity           = v139_rawvalue,
      chr_disconnected_youth        = v149_rawvalue,
      # Physical environment & safety
      chr_air_pollution_pm          = v125_rawvalue,
      chr_broadband_access          = v166_rawvalue,
      chr_exercise_access           = v132_rawvalue,
      chr_severe_housing_problems   = v136_rawvalue,
      chr_injury_deaths             = v135_rawvalue,
      chr_homicides                 = v015_rawvalue,
      chr_firearm_fatalities        = v148_rawvalue,
      chr_motor_vehicle_crash_deaths = v039_rawvalue,
      chr_residential_segregation   = v141_rawvalue,
      chr_social_associations       = v140_rawvalue
    )

  # ---------------------------------------------------------------------------
  # Split into state-level (national + state) and county-level files
  # ---------------------------------------------------------------------------
  data_state <- data_standard %>%
    filter(nchar(geography) <= 2)

  data_county <- data_standard %>%
    filter(nchar(geography) == 5)

  # ---------------------------------------------------------------------------
  # Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(data_state,  "standard/data_state.csv.gz",  delim = ",")
  vroom::vroom_write(data_county, "standard/data_county.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_hash
  dcf::dcf_process_record(updated = process)
}
