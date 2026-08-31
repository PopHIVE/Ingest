#
# Download
#
process <- dcf::dcf_process_record()

raw_state <- dcf::dcf_download_cdc(
  "3cxc-4k8q",
  "raw",
  process$raw_state
)


#
# Reformat
#

if (!identical(process$raw_state, raw_state)) {
  # CDC's export has switched between at least two date-time formats for
  # `posted`/`mmwrweek_end` (e.g. "07/16/2026 10:14:39 AM" and
  # "2026 Jul 16 10:14:39 AM"), so parse with lubridate rather than a single
  # hardcoded as.Date() format -- a format mismatch silently produces NA dates,
  # which then break MMWRweek::MMWRweek()'s internal year arithmetic.
  parse_cdc_date <- function(x) {
    as.Date(lubridate::parse_date_time(
      x, orders = c("mdY IMS p", "Ymd IMS p", "mdY", "Ymd"), quiet = TRUE
    ))
  }

  data <- vroom::vroom("raw/3cxc-4k8q.csv.xz", show_col_types = FALSE) %>%
    dplyr::select(level, pcr_detections, pcr_tests,mmwrweek_end, posted)

  data$posted <- parse_cdc_date(data$posted)
  data <- data[data$level != "National" & data$posted == max(data$posted, na.rm = TRUE), ]
  data$geography <- sub("Region ", "hhs_", data$level, fixed = TRUE)
  data$time <- parse_cdc_date(data$mmwrweek_end)
  data$nrevss <- data$pcr_detections

  report_time <- MMWRweek::MMWRweek(parse_cdc_date(data$mmwrweek_end))
  data$nrevss_week <- report_time$MMWRweek
  data$nrevss_year <- report_time$MMWRyear
  
  key <- readRDS('../../resources/hhs_regions.rds') %>%
    mutate(geography = gsub('Region ', 'hhs_',Group.1)
    ) %>%
    dplyr::select(x, geography)
  
  pop_unstra_hhs <- vroom::vroom("../../resources/pop_hhs.csv.gz", ",")
  
  data_nat <- data %>%
    left_join(pop_unstra_hhs, by = c("level" = "hhs")) %>%
    group_by(time) %>%
    mutate(wgt = popsize / sum(popsize)) %>%
    mutate(across(
      c("nrevss"),
      ~ round(sum(.x * wgt, na.rm = T), 5)
        )
    ) %>%
    dplyr::select( posted, time, nrevss, nrevss_week, nrevss_year ) %>%
    mutate(pcr_detections = nrevss) %>%
    distinct() %>%
    mutate(level = "United States")
  
  
  d <- data %>%
    bind_rows(.,data_nat) %>%
    rename(value = nrevss,
           date = time) %>%
    mutate(epiyr = lubridate::year(date), 
           year = nrevss_year,
           epiyr = if_else(nrevss_week<=26,year - 1 ,year),
           epiyr = paste0(epiyr,'-', epiyr+1),
           epiwk  = if_else( nrevss_week<=26, nrevss_week+52, nrevss_week  ),
           week = nrevss_week,
           epiwk=epiwk-26,
           source = 'CDC NREVSS'
    ) %>%
    left_join(key, by='geography') %>%
    dplyr::select(-geography) %>%
    rename(geography=x) %>%
    mutate(scaled_cases = value/max(value)*100,
           geography = if_else(level=='United States', 'United States', geography)) %>%
    dplyr::select(source, geography, date, scaled_cases, pcr_detections, epiyr, epiwk, week, year) %>%
    rename(time=date) 
  

  
  
  vroom::vroom_write(
    d,
    "standard/data.csv.gz",
    ","
  )

  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
