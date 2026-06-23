#
# Download
#

library(dplyr)

process <- dcf::dcf_process_record()

# Single combined dataset for all three pathogens (replaces three separate CSV URLs)
raw_state <- dcf::dcf_download_cdc(
  "atcp-73re",
  "raw",
  process$raw_state
)

#
# Reformat
#

if (!identical(process$raw_state, raw_state)) {

  # Load FIPS crosswalk (state names → 2-digit FIPS)
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2, geography != "00") %>%
    select(geography, geography_name)

  # Read raw data - site-level, all pathogens in one file
  # Column names use Title_Case in the CDC export (e.g. Pathogen_Target, Site_WVAL)
  data_raw <- vroom::vroom(
    "raw/atcp-73re.csv.xz",
    col_types = list(
      `State/Territory` = "c",
      Week_End           = "c",
      Pathogen_Target    = "c",
      Site_WVAL          = "d",
      Population_Served  = "d"
    ),
    col_select = c("State/Territory", "Week_End", "Pathogen_Target", "Site_WVAL", "Population_Served"),
    show_col_types = FALSE
  ) %>%
    rename(
      state_territory   = `State/Territory`,
      week_end          = Week_End,
      pathogen_target   = Pathogen_Target,
      site_wval         = Site_WVAL,
      population_served = Population_Served
    )

  # Map pathogen names to variable names used in standard output
  data_raw <- data_raw %>%
    mutate(
      variable = case_when(
        pathogen_target == "SARS-CoV-2"       ~ "wastewater_covid",
        pathogen_target == "Influenza A virus" ~ "wastewater_flua",
        pathogen_target == "RSV"               ~ "wastewater_rsv",
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(variable), !is.na(site_wval), !is.na(state_territory))

  # Aggregate site → state level using population-weighted mean
  data_state <- data_raw %>%
    mutate(population_served = if_else(is.na(population_served), 1, population_served)) %>%
    group_by(state_territory, week_end, variable) %>%
    summarize(
      value = weighted.mean(site_wval, population_served, na.rm = TRUE),
      .groups = "drop"
    )

  # Pivot to wide format (one column per pathogen)
  data <- data_state %>%
    tidyr::pivot_wider(
      id_cols      = c("state_territory", "week_end"),
      names_from   = "variable",
      values_from  = "value"
    )

  # Convert state names to FIPS codes
  data <- data %>%
    left_join(state_fips_lookup, by = c("state_territory" = "geography_name")) %>%
    filter(!is.na(geography)) %>%
    mutate(time = format(as.Date(week_end), "%Y-%m-%d")) %>%
    select(geography, time, wastewater_covid, wastewater_flua, wastewater_rsv)

  # Apply 95th percentile cap to suppress occasional extreme values
  data <- data %>%
    mutate(
      covid_95 = quantile(wastewater_covid, probs = 0.95, na.rm = TRUE),
      flu_95   = quantile(wastewater_flua,  probs = 0.95, na.rm = TRUE),
      rsv_95   = quantile(wastewater_rsv,   probs = 0.95, na.rm = TRUE),
      wastewater_covid = if_else(wastewater_covid > covid_95, covid_95, wastewater_covid),
      wastewater_flua  = if_else(wastewater_flua  > flu_95,  flu_95,  wastewater_flua),
      wastewater_rsv   = if_else(wastewater_rsv   > rsv_95,  rsv_95,  wastewater_rsv)
    ) %>%
    select(-covid_95, -flu_95, -rsv_95)

  # Compute population-weighted national average from state-level data
  state_ids <- dcf::dcf_load_census(out_dir = "../../resources", state_only = TRUE)

  nat_ave <- data %>%
    left_join(state_ids, by = c("geography" = "GEOID")) %>%
    group_by(time) %>%
    mutate(
      wgt_covid = (Total * !is.na(wastewater_covid)) / sum(Total * !is.na(wastewater_covid), na.rm = TRUE),  #only counts pop for state if the state contributes WW data
      wgt_rsv   = (Total * !is.na(wastewater_rsv))   / sum(Total * !is.na(wastewater_rsv),   na.rm = TRUE),
      wgt_flua  = (Total * !is.na(wastewater_flua))  / sum(Total * !is.na(wastewater_flua),  na.rm = TRUE)
    ) %>%
    summarize(
      wastewater_covid    = sum(wgt_covid * wastewater_covid, na.rm = TRUE),
      wastewater_rsv      = sum(wgt_rsv   * wastewater_rsv,   na.rm = TRUE),
      wastewater_flua     = sum(wgt_flua  * wastewater_flua,  na.rm = TRUE),
      wgt_check_rsv       = sum(wgt_rsv,   na.rm = TRUE),
      wgt_check_flua      = sum(wgt_flua,  na.rm = TRUE),
      wgt_check_covid     = sum(wgt_covid, na.rm = TRUE)
    ) %>%
    mutate(
      wastewater_covid = if_else(wgt_check_covid == 1, wastewater_covid, NA_real_),
      wastewater_flua  = if_else(wgt_check_flua  == 1, wastewater_flua,  NA_real_),
      wastewater_rsv   = if_else(wgt_check_rsv   == 1, wastewater_rsv,   NA_real_)
    ) %>%
    select(time, wastewater_covid, wastewater_flua, wastewater_rsv) %>%
    mutate(geography = "00")

  data_combined <- bind_rows(data, nat_ave)
  vroom::vroom_write(data_combined, "standard/data.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
