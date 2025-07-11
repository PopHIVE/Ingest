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
}
