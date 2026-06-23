# =============================================================================
# CDC Measles Cases by Age Group and Vaccination Status
# Source: https://www.cdc.gov/measles/data-research/index.html
# Files:
#   raw/cdc_measles_new_cases_age.csv     - New weekly cases (hospitalization counts)
#   raw/cdc_measles_cumulative_age.csv    - Cumulative cases (hospitalization percentages)
# Output:
#   standard/data.csv.gz - Combined data with type = "new_cases" | "cumulative"
# =============================================================================
#These files are processed on a separate repostory (https://github.com/PopHIVE/measles_age_cdc_scraper) and pulled in and formatted here. 
library(tidyverse)


# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}


library(dplyr)

process <- dcf::dcf_process_record()

# GitHub raw base URL for standard files
base_url <- "https://github.com/PopHIVE/measles_age_cdc_scraper/raw/refs/heads/main/"

# Files to download from the remote standard folder
raw_files <- c(
  "measles_structured.csv"
)

# Download each file and track hashes for change detection
current_hashes <- list()
any_changed <- FALSE

for (f in raw_files) {
  url <- paste0(base_url, "/", f)
  dest <- file.path("raw", f)

  tryCatch({
    download.file(url, dest, mode = "wb", quiet = TRUE)
    current_hashes[[f]] <- tools::md5sum(dest)
  }, error = function(e) {
    message("Warning: failed to download ", f, ": ", e$message)
  })
}


cum_file  <- "raw/measles_structured.csv"

# Change detection based on file hashes
raw_state <- list(
  cum_hash = tools::md5sum(cum_file)
)

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # New weekly cases
  # Columns: update_date, cases_under_5, cases_5_19, cases_over_20,
  #          cases_age_unknown, cases_unvaccinated_unknown, cases_one_dose,
  #          cases_two_doses, hospitalizations_total, hospitalizations_under_5,
  #          hospitalizations_5_19, hospitalizations_over_20,
  #          hospitalizations_age_unknown
  # ---------------------------------------------------------------------------

  a1 <- vroom::vroom(cum_file) %>%
  filter(snapshot_date >= '2025-02-18') %>%
    mutate(
      geography = "00",
      type      = "cumulative",
      time      = as.Date(`update_date`, format = "%B %d, %Y"),
      time = lubridate::floor_date(time, unit = "week", week_start = 7) + 6, # Align to Saturday of current week (which will be partial--this is consistent with how national data are reported)
      week = MMWRweek::MMWRweek(time)$MMWRweek,
      year = MMWRweek::MMWRweek(time)$MMWRyear
    ) %>% 
    group_by(update_date) %>%
    mutate(updateN = row_number())%>%
    filter(updateN==1) %>% #
      dplyr::select(-updateN) %>%
      ungroup()
    

cum_cases <- a1 %>%
    pivot_longer(cols = c(total_cases,age_under5_n, age_5_19_n, age_20plus_n), names_to = "age_group", values_to = "cdc_cum_cases") %>%
    group_by(time, year,week,age_group) %>%
    ungroup() %>%
    mutate(age_group = case_when(
      age_group == "age_under5_n" ~ "<5 years",
      age_group == "age_5_19_n" ~ "5-19 years",
      age_group == "age_20plus_n" ~ "20+ years",
      age_group == "total_cases" ~ "Total",
      TRUE ~ age_group
    )
    ) %>%
    filter(year>=2025) %>%
    arrange(age_group, time, year) %>%
    group_by(age_group, year) %>%
     mutate(cdc_new_cases = cdc_cum_cases - lag(cdc_cum_cases, default = 0)) %>%
     dplyr::select(time, year, week, age_group, cdc_cum_cases, cdc_new_cases) 

cum_hospitalization <- a1 %>%
    pivot_longer(cols = c(hosp_total_n,hosp_under5_n, hosp_5_19_n, hosp_20plus_n), names_to = "age_group", values_to = "cdc_cum_hosp") %>%
    group_by(time, year,week,age_group) %>%
    ungroup() %>%
    mutate(age_group = case_when(
      age_group == "hosp_under5_n" ~ "<5 years",
      age_group == "hosp_5_19_n" ~ "5-19 years",
      age_group == "hosp_20plus_n" ~ "20+ years",
      age_group == "hosp_total_n" ~ "Total",
      TRUE ~ age_group
    )
    ) %>%
    filter(year>=2025) %>%
    arrange(age_group, time, year) %>%
    group_by(age_group, year) %>%
     mutate(cdc_new_hosp = cdc_cum_hosp - lag(cdc_cum_hosp, default = 0)) %>%
     dplyr::select(time, year, week, age_group, cdc_cum_hosp, cdc_new_hosp) 

cum_cases_vax <- a1 %>%
mutate(vax_unvax_unknown_n = total_cases*vax_unvax_or_unknown_pct/100,
       vax_one_mmr_n = total_cases*vax_one_mmr_pct/100,
       vax_two_mmr_n = total_cases*vax_two_mmr_pct/100) %>%
    pivot_longer(cols = c(total_cases,vax_unvax_unknown_n,vax_one_mmr_n, vax_two_mmr_n), names_to = "vax_group", values_to = "cdc_cum_cases") %>%
    group_by(time, year,week,vax_group) %>%
    ungroup() %>%
    filter(vax_group !='total_cases') %>%
    mutate(vax_group = case_when(
      vax_group == "vax_unvax_unknown_n" ~ "Unvaccinated/Unknown",
      vax_group == "vax_one_mmr_n" ~ "One dose MMR",
      vax_group == "vax_two_mmr_n" ~ "Two doses MMR",
      vax_group == "total_cases" ~ "Total",
      TRUE ~ vax_group
    )
    ) %>%
    filter(year>=2025) %>%
    arrange(vax_group, time, year) %>%
    group_by(vax_group, year) %>%
     mutate(
      cdc_new_cases = cdc_cum_cases - lag(cdc_cum_cases, default = 0),
      age_group ='Total') %>%
     dplyr::select(time, year, week, age_group, vax_group, cdc_cum_cases, cdc_new_cases) 

combined <- cum_cases %>%
full_join(cum_hospitalization, by = c("time", "year", "week", "age_group")) %>%
  arrange(age_group, time, year) %>%
  mutate(vax_group ='Total') %>%
  bind_rows(cum_cases_vax) 

  vroom::vroom_write(combined, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}


# ggplot(cum_cases) +z
#     mutate(
#      cdc_new_cases = cdc_cum_cases - lag(cdc_cum_cases, default = 0)) %>%
#     dplyr::select(time, year, week, age_group, cdc_cum_cases, cdc_new_cases) 

# ggplot(cum_cases) +
# geom_line(aes(x = week, y = cdc_cum_cases, color = age_group)) +
# facet_wrap(~year)

# ggplot(cum_cases) +
# geom_line(aes(x = week, y = cdc_new_cases, color = age_group)) +
# facet_wrap(~year)


# ggplot(cum_hospitalization) +
# geom_line(aes(x = week, y = cdc_cum_hosp, color = age_group)) +
# facet_wrap(~year)

# ggplot(cum_hospitalization) +
# geom_line(aes(x = week, y = cdc_new_hosp, color = age_group)) +
# facet_wrap(~year)