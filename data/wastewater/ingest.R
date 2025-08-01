#
# Download
#

base_url <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/"
files <- c(
  rsv = "rsv/nwssrsvstateactivitylevel.csv",
  flua = "FluA/nwssfluastateactivitylevelDL.csv",
  covid = "SC2/nwsssc2stateactivitylevelDL.csv"
)
for (file in names(files)) {
  url <- paste0(base_url, files[[file]])
  path <- paste0("raw/", file, ".csv")
  download.file(url, path)
  unlink(paste0(path, ".xz"))
  system2("xz", c("-f", path))
}

#
# Reformat
#

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw",
  "csv",
  recursive = TRUE,
  full.names = TRUE
)))
process <- dcf::dcf_process_record()

if (!identical(process$raw_state, raw_state)) {
  data <- do.call(
    rbind,
    lapply(list.files("raw", "xz", full.names = TRUE), function(file) {
      d <- vroom::vroom(
        file,
        ",",
        col_types = list(
          `State/Territory` = "c",
          Week_Ending_Date = "c",
          Data_Collection_Period = "c",
          `State/Territory_WVAL` = "d"
        ),
        col_select = c(
          "State/Territory",
          "Week_Ending_Date",
          "Data_Collection_Period",
          "State/Territory_WVAL"
        )
      )
      d <- d[
        !is.na(d$`State/Territory`) & d$Data_Collection_Period == "All Results",
        -3L
      ]
      colnames(d) <- c("geography", "time", "value")
      d$variable <- paste0(
        "wastewater_",
        strsplit(basename(file), ".", fixed = TRUE)[[1]][[1]]
      )
      d
    })
  )
  data <- tidyr::pivot_wider(
    data,
    id_cols = c("geography", "time"),
    names_from = "variable"
  )

  # convert state names to GEOIDs
  state_ids <- vroom::vroom(
    "https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt",
    delim = "|",
    col_types = list(STATE = "c", STATEFP = "c")
  )
  data$geography <- structure(state_ids$STATEFP, names = state_ids$STATE_NAME)[
    data$geography
  ]
  
  state_ids <- dcf::dcf_load_census(
    out_dir = "../../resources",
    state_only = TRUE
  )
  
  nat_ave <- data %>%
    left_join(state_ids, by=c('geography'='GEOID'))%>%
    group_by(time) %>%
    mutate(wgt_covid = (Total*!is.na(wastewater_covid))/sum(Total*!is.na(wastewater_covid), na.rm=T), #population weight
           wgt_rsv = (Total*!is.na(wastewater_rsv))/sum(Total*!is.na(wastewater_rsv), na.rm=T), #population weight
           wgt_flua = (Total*!is.na(wastewater_flua))/sum(Total*!is.na(wastewater_flua), na.rm=T), #population weight
           wgt_part_covid = wgt_covid*wastewater_covid,
           wgt_part_rsv = wgt_rsv*wastewater_rsv,
           wgt_part_flua = wgt_flua*wastewater_flua
           ) %>% 
    summarize(wastewater_covid = sum(wgt_part_covid, na.rm=T),
              wastewater_rsv= sum(wgt_part_rsv, na.rm=T),
              wastewater_flua= sum(wgt_part_flua, na.rm=T),
              wgt_check_rsv =sum(wgt_rsv, na.rm=T),
              wgt_check_flua =sum(wgt_flua, na.rm=T),
              wgt_check_covid =sum(wgt_covid, na.rm=T)
              ) %>%
    mutate(wastewater_covid = if_else(wgt_check_covid==1,  wastewater_covid,NA_real_),
           wastewater_flua = if_else(wgt_check_flua==1, wastewater_flua,NA_real_),
           wastewater_rsv = if_else(wgt_check_rsv==1,  wastewater_rsv,NA_real_)
           ) %>%
    dplyr::select(time, wastewater_covid,wastewater_flua,wastewater_rsv) %>%
    mutate(geography = '00')

  data_combined <- bind_rows(data,nat_ave )
  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

