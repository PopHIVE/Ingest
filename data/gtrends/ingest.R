#
# Download
#

base_url <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term="
terms <- c("Naloxone", "overdose", "rsv", "%252Fg%252F11j30ybfx6")
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
    state_ids$GEOID,
    names = state_ids$region_name
  )[structure(
    c(state.name, "District of Columbia"),
    names = c(state.abb, "DC")
  )[sub(
    "US-",
    "",
    data$geography,
    fixed = TRUE
  )]]

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
    reshape2::melt(.,id.vars=c('geography','time')) %>%
    rename(location = geography,
           term=variable,
           date=time) %>%
    filter(!(location %in% g_states)) %>%
    group_by(date, location, term) %>%
    summarize(value = mean(value),
    ) %>% #averages over duplicate pulls
    ungroup() %>%
    collect() %>%
    mutate(
      date2 = as.Date(date),
      date = as.Date(ceiling_date(date2, 'week')) - 1
    ) %>%
    group_by(term) %>%
    mutate(location = as.numeric(location),
           ucl = quantile(value, probs=0.99),
           value = if_else(value>ucl, ucl, value)) %>%
    ungroup() %>%
    filter(!is.na(location)) %>%
    rename(search_volume = value) %>%
    filter(date >= as.Date('2018-07-01')) %>%
    left_join(dma_link1, by = c('location' = 'DMA_ID'),relationship = "many-to-many") %>% #many to many join by date and counties
    group_by(STATEFP, CNTYFP) %>%
    mutate(
      fips = paste0(STATEFP, sprintf("%03d", CNTYFP)),
      fips = as.numeric(fips)
    ) %>%
    ungroup() %>%
    mutate(
      search_volume_scale = search_volume / max(search_volume, na.rm = T) * 100
    ) %>%
    ungroup() %>%
    dplyr::select(date, fips, search_volume_scale,term) %>%
    rename(value = search_volume_scale) 
  
  vroom::vroom_write(g1_metro, "standard/data_dma.csv.gz", ",")
  
}
