#
# Download
#

base_url <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term="
terms <- c("Naloxone", "overdose","narcan","drug+overdose", "rsv", "%252Fg%252F11j30ybfx6")
for (term in terms) {
  term_dir <- paste0("raw/term=", term)
  dir.create(term_dir, showWarnings = FALSE)
  download.file(
    paste0(base_url, term, "/part-0.parquet"),
    paste0(term_dir, "/part-0.parquet"),
    mode = "wb"
  )
}

#
# Reformat
#

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "parquet",
  recursive = TRUE,
  full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  data <- dplyr::collect(dplyr::filter(
    arrow::open_dataset("raw"),
    grepl("US", location),
    date > 2014
  ))

  # aggregate over repeated samples
  data <- dplyr::summarize(
    dplyr::group_by(data, term, location, date),
    value = mean(value),
    .groups = "keep"
  )
  data$term <- paste0("gtrends_", tolower(data$term))
  data$term[data$term == "gtrends_%2fg%2f11j30ybfx6"] <- "gtrends_rsv_vaccine"
  data <- tidyr::pivot_wider(
    data,
    id_cols = c("location", "date"),
    names_from = "term"
  )
  colnames(data)[1L:2L] <- c("geography", "time")

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
    dplyr::select(-month, -season)
  
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
  
  ############
  #####DMA
  ###################
  data_dma <- dplyr::collect(dplyr::filter(
    arrow::open_dataset("raw"),
        date > 2014
  ))
  
  # aggregate over repeated samples
  data_dma <- dplyr::summarize(
    dplyr::group_by(data_dma, term, location, date),
    value = mean(value),
    .groups = "keep"
  )
  data_dma$term <- paste0("gtrends_", tolower(data_dma$term))
  data_dma$term[data_dma$term == "gtrends_%2fg%2f11j30ybfx6"] <- "gtrends_rsv_vaccine"
  data_dma <- tidyr::pivot_wider(
    data_dma,
    id_cols = c("location", "date"),
    names_from = "term"
  )
  colnames(data_dma)[1L:2L] <- c("geography", "time")
  
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
    'DMA_name' = metros$name,
    'DMA' = metros$numeric.sub.area
  ) %>%
    rename(DMA_ID = DMA) %>%
    full_join(cw1, by = c("DMA_name" = "GOOGLE_DMA")) %>%
    dplyr::select(STATE, COUNTY, STATEFP, CNTYFP, DMA_ID) %>%
    mutate(DMA_ID = as.numeric(DMA_ID)) %>%
    filter(!is.na(DMA_ID))
  
  
  g_states <- paste('US', state.abb, sep = '-')
  
  #
  #
  # ##Google metro data
  #view_dma <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Webslim/respiratory_diseases/rsv/google_dma.parquet')
  g1_metro <- data_dma %>%
   # reshape2::melt(.,id.vars=c('geography','time')) %>%
    filter(!(geography %in% g_states)) %>%
    group_by(geography, time) %>%
    summarise(across(
      c(`gtrends_drug+overdose`,gtrends_naloxone, gtrends_narcan, gtrends_overdose,gtrends_rsv_vaccine, gtrends_rsv ),
      ~ mean(.x, na.rm = TRUE)
    ), .groups = "drop") %>% #averages over duplicate pulls
    ungroup() %>%
    collect() %>%
    mutate(
      time = as.character(as.Date(time)+ 6) # week end date
    ) %>%
    filter(!is.na(geography)) %>%
    filter(time >= as.Date('2018-07-01')) %>%
    rename(DMA_ID = geography) %>%
    mutate(DMA_ID = as.numeric(DMA_ID)) %>%
    left_join(dma_link1, by = c('DMA_ID' = 'DMA_ID'),relationship = "many-to-many") %>% #many to many join by date and counties
    group_by(STATEFP, CNTYFP) %>%
    mutate(
      STATEFP = sprintf("%02d", STATEFP),
      geography = paste0(STATEFP, sprintf("%03d", CNTYFP)),
    ) %>%
    ungroup()%>%
    mutate(across(
      c(`gtrends_drug+overdose`, gtrends_narcan,gtrends_naloxone, gtrends_overdose,gtrends_rsv_vaccine, gtrends_rsv),
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
        gtrends_rsv),
      \(x) x / max(x, na.rm = TRUE) * 100  #scales each value to 100
    )) %>%
    dplyr::select(geography, time,  gtrends_narcan,gtrends_naloxone,`gtrends_drug+overdose`, gtrends_overdose,gtrends_rsv_vaccine, gtrends_rsv)
  

  
  vroom::vroom_write(g1_metro, "standard/data_dma.csv.gz", ",")
  
}
