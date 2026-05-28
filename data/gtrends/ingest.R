library(dplyr)
#
# Download
#


base_url <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term="

terms <- c("Naloxone", "overdose","narcan","drug+overdose", "rsv", "%252Fg%252F11j30ybfx6", "9mm","heat+exhaustion","heat+stroke","shotgun")

raw_state <- jsonlite::read_json(
  "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data_state.json"
)
raw_state <- raw_state[grep(
  paste0("data/term=(?:", paste(URLdecode(terms), collapse = "|"), ")"),
  names(raw_state)
)]
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {

for (term in terms) {
  term_dir <- paste0("raw/term=", term)
  dir.create(term_dir, showWarnings = FALSE)
  download.file(
    paste0(base_url, term, "/part-0.parquet"),
    paste0(term_dir, "/part-0.parquet"),
    mode = "wb"
  )
}

#repeat for yearly data
base_url_year <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data_yearly/term="

terms <- c("Naloxone", "overdose","narcan","drug+overdose", "9mm","heat+exhaustion","heat+stroke","shotgun")
for (term in terms) {
  term_dir <- paste0("raw_year/term=", term)
  dir.create(term_dir, showWarnings = FALSE)
  download.file(
    paste0(base_url_year, term, "/part-0.parquet"),
    paste0(term_dir, "/part-0-year.parquet"),
    mode = "wb"
  )
}

  data_week <- dplyr::collect(dplyr::filter(
    arrow::open_dataset("raw"),
    grepl("US", location),
    date > 2014
  ))%>%
    mutate(resolution = 'week')
  
  data_year <- dplyr::collect(dplyr::filter(
    arrow::open_dataset("raw_year"),
    grepl("US", location),
    date > 2014
  )) %>%
    mutate(resolution = 'year')

  data <- bind_rows(data_week, data_year)
    
  # aggregate over repeated samples
  data <- dplyr::summarize(
    dplyr::group_by(data, term, location, date, resolution),
    value = mean(value),
    .groups = "keep"
  )
  data$term <- paste0("gtrends_", tolower(data$term))
  data$term[data$term == "gtrends_%2fg%2f11j30ybfx6"] <- "gtrends_rsv_vaccine"
  data <- tidyr::pivot_wider(
    data,
    id_cols = c("location", "date","resolution"),
    names_from = "term"
  )
  colnames(data)[1L:3L] <- c("geography", "time", 'resolution')

  # convert state abbreviations to GEOIDs
  state_ids <- dcf::dcf_load_census(
    out_dir = "../../resources",
    state_only = TRUE
  )
  
  data$geography <- structure(
    c('00',state_ids$GEOID),
    names = c('United States',state_ids$region_name)
  )[structure(
    c(state.name, "District of Columbia", 'United States'),
    names = c(state.abb, "DC","US")
  )
  [sub(
    "US-",
    "",
    data$geography,
    fixed = TRUE
  )]]

  data$time <- as.character(as.Date(data$time)+ 6) # week end date
  
  data <- data %>%
    ungroup() %>%
    mutate( month = lubridate::month(as.Date(time)),
            season = if_else(month>=7 & month <=10,1,0),
            gtrends_rsv_adjusted = gtrends_rsv - season*(4.41-1.69)*gtrends_rsv_vaccine - (1-season)*3.41*gtrends_rsv_vaccine,  #2.655 based on the regression below
            gtrends_rsv_adjusted = if_else(gtrends_rsv_adjusted<0,0,gtrends_rsv_adjusted),
            gtrends_rsv_adjusted = gtrends_rsv_adjusted / max(gtrends_rsv_adjusted, na.rm=T)
    ) %>%
    mutate(across(
      c(gtrends_rsv_vaccine,
        gtrends_naloxone,
        `gtrends_drug+overdose`,
        gtrends_narcan,
        gtrends_overdose,
        gtrends_rsv,
        `gtrends_heat+exhaustion`,
        `gtrends_heat+stroke`, 
        gtrends_9mm,
        gtrends_shotgun),
      \(x) x / max(x, na.rm = TRUE) * 100  #scales each value to 100
    )) %>%
    dplyr::select(-month, -season)
  
  data  %>% 
    filter(resolution=='week') %>%
    dplyr::select(-resolution) %>%
  vroom::vroom_write(., "standard/data.csv.gz", ",")
  
  
  current_month <- lubridate::month(Sys.Date())
  current_year <- lubridate::year(Sys.Date())
  
  #yearly data. for partial years, only keep if have accumulated through Aug
  data  %>% 
    mutate(keep_year = if_else(current_month>=8,current_year, current_year-1),
           data_year = lubridate::year(time)
           ) %>%
    filter(resolution=='year' & data_year<=keep_year) %>%
    dplyr::select(-resolution, -keep_year, -data_year) %>%
    vroom::vroom_write(., "standard/data_year.csv.gz", ",")

  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
  
  ############
  #####DMA
  ###################
  data_dma_week <- dplyr::collect(dplyr::filter(
    arrow::open_dataset("raw"),
        date > 2014
  )) %>%
    mutate(resolution = 'week')
  
  data_dma_year <- dplyr::collect(dplyr::filter(
    arrow::open_dataset("raw_year"),
    date > 2014
  ))%>%
    mutate(resolution = 'year')
  
  data_dma <- bind_rows(data_dma_year,data_dma_week)
  
  # aggregate over repeated samples
  data_dma <- dplyr::summarize(
    dplyr::group_by(data_dma, term, location, date, resolution),
    value = mean(value),
    .groups = "keep"
  )
  data_dma$term <- paste0("gtrends_", tolower(data_dma$term))
  data_dma$term[data_dma$term == "gtrends_%2fg%2f11j30ybfx6"] <- "gtrends_rsv_vaccine"
  data_dma <- tidyr::pivot_wider(
    data_dma,
    id_cols = c("location", "date","resolution"),
    names_from = "term"
  )
  colnames(data_dma)[1L:3L] <- c("geography", "time", "resolution")
  
  data_dma <- data_dma %>% 
    ungroup() %>%
    as.data.frame() %>%
    filter(!grepl('US', geography))
  
   #
  # ##Metro; Crosswalk the DMA to counties FIPS codes
  # #https://www.kaggle.com/datasets/kapastor/google-trends-countydma-mapping?resource=download
  cw1 <- read.csv('../../resources/GoogleTrends_CountyDMA_Mapping.csv') %>%
    mutate(GOOGLE_DMA = toupper(GOOGLE_DMA))
  
  #Metro region
  #https://stackoverflow.com/questions/61213647/what-do-gtrendsr-statistical-areas-correlate-with
  #Nielsen DMA map: http://bl.ocks.org/simzou/6459889
  #read in 'countries' file from gtrendsR
  countries <- read.csv('../../resources/countries_gtrendsR.csv')
  metros <- countries[countries$country_code == 'US', ]
  
  metros <-
    metros[grep("[[:digit:]]", substring(metros$sub_code, first = 4)), ]
  
  metros$numeric.sub.area <- gsub('US-', '', metros$sub_code)
  
  
  dma_link1 <- cbind.data.frame(
    'DMA_name' = toupper(metros$name),
    'DMA' = metros$numeric.sub.area
  ) %>%
    rename(DMA_ID = DMA) %>%
    full_join(cw1, by = c("DMA_name" = "GOOGLE_DMA")) %>%
    dplyr::select(STATE, COUNTY, STATEFP, CNTYFP, DMA_ID) %>%
    mutate(DMA_ID = as.numeric(DMA_ID)) %>%
    filter(!is.na(DMA_ID))
  
  
  #
  #
  # ##Google metro data
  #view_dma <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/google_dma.parquet')
  g1_metro <- data_dma %>%
   # reshape2::melt(.,id.vars=c('geography','time')) %>%
    group_by(geography, time, resolution) %>%
    summarise(across(
      c(`gtrends_drug+overdose`,gtrends_naloxone, gtrends_narcan, gtrends_overdose,gtrends_rsv_vaccine, gtrends_rsv, `gtrends_heat+exhaustion`,`gtrends_heat+stroke`, gtrends_9mm, gtrends_shotgun ),
      ~ mean(.x, na.rm = TRUE)
    ), .groups = "drop") %>% #averages over duplicate pulls
    ungroup() %>%
    mutate(
      time = as.character(as.Date(time)+ 6) # week end date
    ) %>%
    filter(!is.na(geography)) %>%
    filter(time >= '2018-07-01') %>%
    rename(DMA_ID = geography) %>%
    mutate(DMA_ID = as.numeric(DMA_ID)) %>%
    left_join(dma_link1, by = c('DMA_ID' = 'DMA_ID'), relationship = "many-to-many") %>%
    filter(!is.na(STATEFP), !is.na(CNTYFP)) %>%
    group_by(time, resolution, STATEFP, CNTYFP) %>%
    summarise(across(starts_with("gtrends"), ~ mean(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(
      STATEFP = sprintf("%02d", STATEFP),
      geography = paste0(STATEFP, sprintf("%03d", CNTYFP)),
    ) %>%
    mutate(across(
      c(`gtrends_drug+overdose`, gtrends_narcan,gtrends_naloxone, gtrends_overdose,gtrends_rsv_vaccine, gtrends_rsv, `gtrends_heat+exhaustion`,`gtrends_heat+stroke`, gtrends_9mm, gtrends_shotgun),
      \(x) {
        p99 <- quantile(x, 0.99, na.rm = TRUE)
        pmin(x, p99)
      })
    ) %>%
    mutate(across(
      c(gtrends_rsv_vaccine,
        gtrends_naloxone,
        `gtrends_drug+overdose`,
        gtrends_narcan,
        gtrends_overdose,
        gtrends_rsv,
        `gtrends_heat+exhaustion`,
        `gtrends_heat+stroke`, 
        gtrends_9mm,
        gtrends_shotgun),
      \(x) x / max(x, na.rm = TRUE) * 100  #scales each value to 100
    )) %>%
    dplyr::select(geography, time, resolution, starts_with("gtrends"))
  

  g1_metro %>%
    filter(resolution == 'week') %>%
    dplyr::select(-resolution) %>%
    vroom::vroom_write(., "standard/data_dma.csv.gz", ",")
  
  g1_metro %>%
    filter(resolution == 'year') %>%
    dplyr::select(-resolution) %>%
    vroom::vroom_write(., "standard/data_dma_year.csv.gz", ",")
  
}

