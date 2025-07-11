#
# Download
#

base_url <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/"
files <- c(
  rsv = "RSVStateLevelDownloadCSV.csv",
  flua = "FluA/FluAStateMapDownloadCSV.csv",
  covid = "SC2StateLevelDownloadCSV.csv"
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

  vroom::vroom_write(data, "standard/data.csv.gz", ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
