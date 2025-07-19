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
  data <- vroom::vroom("raw/3cxc-4k8q.csv.xz", show_col_types = FALSE)

  data$posted <- as.Date(data$posted, "%m/%d/%Y")
  data <- data[data$level != "National" & data$posted == max(data$posted), ]
  data$geography <- sub("Region ", "hhs_", data$level, fixed = TRUE)
  data$time <- lubridate::floor_date(as.Date(data$mmwrweek_end, "%m/%d/%Y"), 'week')
  data$nrevss <- data$pcr_detections

  report_time <- MMWRweek::MMWRweek(as.Date(data$mmwrweek_end, "%m/%d/%Y"))
  data$nrevss_week <- report_time$MMWRweek
  data$nrevss_year <- report_time$MMWRyear

  key <- readRDS('../../resources/hhs_regions.rds') %>%
    mutate(geography = gsub('Region ', 'hhs_',Group.1)
    ) %>%
    dplyr::select(x, geography)
  
  d <- data %>%
    rename(value = nrevss,
           date = time) %>%
    mutate(epiyr = lubridate::year(date), 
           year = nrevss_year,
           epiyr = if_else(nrevss_week<=26,year - 1 ,nrevss_week),
           epiwk  = if_else( nrevss_week<=26, nrevss_week+52, nrevss_week  ),
           week = nrevss_week,
           epiwk=epiwk-26,
           source = 'CDC NREVSS'
    ) %>%
    left_join(key, by='geography') %>%
    dplyr::select(-geography) %>%
    rename(geography=x) %>%
    mutate(scaled_cases = value/max(value)*100) %>%
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
