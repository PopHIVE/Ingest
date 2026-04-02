# =============================================================================
# ABCs Group A Streptococcus Data Ingestion
# Source: https://data.cdc.gov/Public-Health-Surveillance/Active-Bacterial-Core-surveillance-ABCs-Group-A-St/9y49-tura/about_data
# =============================================================================

library(dplyr)

process <- dcf::dcf_process_record()

raw_state <- dcf::dcf_download_cdc(
  "9y49-tura",
  "raw",
  process$raw_state
)

if (!identical(process$raw_state, raw_state)) {

  data_raw <- vroom::vroom("raw/9y49-tura.csv.xz", show_col_types = FALSE) %>%
    rename(
      year    = Year,
      value   = Value,
      units   = Units,
      bacteria = Bacteria,
      topic   = Topic,
      viewby  = ViewBy,
      viewby2 = ViewBy2
    ) %>%
    mutate(
      # Normalize casing inconsistencies across years
      topic = case_when(
        tolower(trimws(topic)) == "case rates"                    ~ "Case rates",
        tolower(trimws(topic)) == "death rates"                   ~ "Death rates",
        tolower(trimws(topic)) == "number of cases and deaths"    ~ "Number of cases and deaths",
        tolower(trimws(topic)) == "syndromes"                     ~ "Syndromes",
        tolower(trimws(topic)) == "antibiotic resistance"         ~ "Antibiotic resistance",
        tolower(trimws(topic)) %in% c("emm types", "emm types")  ~ "Emm types",
        TRUE ~ topic
      ),
      time      = as.Date(paste0(year, "-12-31")),
      geography = "00"
    )

  # ---------------------------------------------------------------------------
  # 1. Case rates and death rates (by age, sex, race, and overall)
  # ---------------------------------------------------------------------------
  rate_topics <- c("Case rates", "Death rates")

  make_measure <- function(df) {
    df %>% mutate(measure = if_else(topic == "Case rates", "case_rate", "death_rate"))
  }

  # Overall (one row per year/measure)
  rates_overall <- data_raw %>%
    filter(topic %in% rate_topics, viewby == "Overall") %>%
    make_measure() %>%
    mutate(age = "Overall", sex = "Overall", race_ethnicity = "Overall")

  # Age-stratified (exclude the Overall row within Age viewby)
  age_map <- c(
    "<1 year old"     = "<1 years",
    "1 year old"      = "1 year old",
    "1 years old"     = "1 year old",
    "2-4 years old"   = "2-4 years old",
    "5-17 years old"  = "5-17 years old",
    "18-34 years old" = "18-34 years old",
    "35-49 years old" = "35-49 years old",
    "50-64 years old" = "50-64 years old",
    "\u226565 years old" = "65+ years olds"
  )
  rates_age <- data_raw %>%
    filter(topic %in% rate_topics, viewby == "Age", viewby2 %in% names(age_map)) %>%
    make_measure() %>%
    mutate(age = age_map[viewby2], sex = "Overall", race_ethnicity = "Overall")

  # Sex-stratified (Male/Female only — Overall already covered above)
  rates_sex <- data_raw %>%
    filter(topic %in% rate_topics, viewby == "Sex", viewby2 %in% c("Male", "Female")) %>%
    make_measure() %>%
    mutate(age = "Overall", sex = viewby2, race_ethnicity = "Overall")

  # Race-stratified (non-Overall values only)
  race_map <- c("Black" = "Black", "White" = "White", "Other races" = "Other")
  rates_race <- data_raw %>%
    filter(topic %in% rate_topics, viewby == "Race", viewby2 %in% names(race_map)) %>%
    make_measure() %>%
    mutate(age = "Overall", sex = "Overall", race_ethnicity = race_map[viewby2])

  data_rates <- bind_rows(rates_overall, rates_age, rates_sex, rates_race) %>%
    mutate(measure = if_else(measure == "case_rate", "abcs_gas_rate_cases", "abcs_gas_rate_deaths")) %>%
    select(geography, time, age, sex, race_ethnicity, measure, value)

  # ---------------------------------------------------------------------------
  # 2. Total case counts and deaths (national aggregate)
  # ---------------------------------------------------------------------------
  data_counts <- data_raw %>%
    filter(
      topic == "Number of cases and deaths",
      viewby == "ALL"
    ) %>%
    mutate(
      measure = case_when(
        viewby2 == "Total cases"      ~ "abcs_gas_N_cases",
        viewby2 == "Number of deaths" ~ "abcs_gas_N_deaths",
        TRUE ~ NA_character_
      ),
      age            = "Overall",
      sex            = "Overall",
      race_ethnicity = "Overall"
    ) %>%
    filter(!is.na(measure)) %>%
    # Source has duplicate 2023 "Total cases" entries; keep the larger (national estimate)
    group_by(geography, time, measure) %>%
    slice_max(value, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(geography, time, age, sex, race_ethnicity, measure, value)

  data_main <- bind_rows(data_rates, data_counts) %>%
    tidyr::pivot_wider(
      id_cols     = c(geography, time, age, sex, race_ethnicity),
      names_from  = measure,
      values_from = value
    )

  vroom::vroom_write(data_main, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 3. Syndromes (percent of cases by clinical presentation)
  # ---------------------------------------------------------------------------
  syndrome_name_map <- c(
    "Cellulitis"                = "cellulitis",
    "Bacteremia without focus"  = "bacteremia_without_focus",
    "Pneumonia"                 = "pneumonia",
    "Necrotizing fasciitis"     = "necrotizing_fasciitis",
    "Streptococcal toxic shock" = "strep_toxic_shock",
    "Other"                     = "other"
  )

  data_syndromes <- data_raw %>%
    filter(topic == "Syndromes", viewby %in% names(syndrome_name_map)) %>%
    mutate(
      measure = paste0("abcs_gas_pct_syndrome_", syndrome_name_map[viewby])
    ) %>%
    select(geography, time, measure, value) %>%
    tidyr::pivot_wider(names_from = measure, values_from = value)

  vroom::vroom_write(data_syndromes, "standard/data_syndromes.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 4. Antibiotic resistance (percent resistant / number of isolates)
  # ---------------------------------------------------------------------------
  antibiotics <- c(
    "Penicillin", "Erythromycin", "Clindamycin**",
    "Cefotaxime", "Tetracycline", "Vancomycin", "Number of isolates"
  )

  data_resistance <- data_raw %>%
    filter(topic == "Antibiotic resistance", viewby %in% antibiotics) %>%
    mutate(
      drug    = tolower(sub("\\*\\*$", "", viewby)),
      measure = if_else(
        viewby == "Number of isolates",
        "abcs_gas_n_isolates",
        paste0("abcs_gas_pct_resistant_", drug)
      )
    ) %>%
    select(geography, time, measure, value) %>%
    tidyr::pivot_wider(names_from = measure, values_from = value)

  vroom::vroom_write(data_resistance, "standard/data_resistance.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 5. emm types (percent and counts of isolates by emm type)
  # ---------------------------------------------------------------------------
  data_emm <- data_raw %>%
    filter(topic == "Emm types") %>%
    mutate(
      emm_clean = tolower(gsub("[^a-zA-Z0-9]", "_", viewby)),
      measure   = case_when(
        units == "Percent" ~ paste0("abcs_gas_emm_pct_", emm_clean),
        TRUE               ~ paste0("abcs_gas_emm_count_", emm_clean)
      )
    ) %>%
    select(geography, time, measure, value) %>%
    tidyr::pivot_wider(names_from = measure, values_from = value)

  vroom::vroom_write(data_emm, "standard/data_emm.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
