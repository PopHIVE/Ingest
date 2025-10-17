dcf_read_epic_injury <- function (path, path_root = ".") 
{
  full_path <- if (file.exists(path)) {
    path
  }
  else {
    sub("//", "/", paste0(path_root, "/", path), fixed = TRUE)
  }
  lines <- readLines(full_path, n = 25L, skipNul = FALSE)
  metadata_break <- grep("^[, ]*$", lines)
  if (!length(metadata_break)) {
    cli::cli_abort("path does not appear to point to a file in the Epic format (no metadata separation)")
  }
  meta_end <- min(metadata_break) - 1L
  data_start <- (if (length(metadata_break) == 1L) {
    metadata_break
  }
  else {
    max(metadata_break[metadata_break == c(-1L, metadata_break[-1L])])
  }) + 1L
  meta <- c(list(file = path, md5 = unname(tools::md5sum(full_path)), 
                 date_processed = Sys.time(), standard_name = ""), as.list(unlist(lapply(strsplit(sub(",+$", 
                                                                                                      "", lines[seq_len(meta_end)]), ",", fixed = TRUE), function(r) {
                                                                                                        l <- list(paste(r[-1L], collapse = ","))
                                                                                                        if (l[[1]] == "") {
                                                                                                          r <- strsplit(r, ": ", fixed = TRUE)[[1L]]
                                                                                                          l <- list(paste(r[-1L], collapse = ","))
                                                                                                        }
                                                                                                        names(l) <- r[[1L]]
                                                                                                        l[[1L]] <- gsub("^\"|\"$", "", l[[1L]])
                                                                                                        l
                                                                                                      }))))
  standard_names <- c(vaccine_mmr = "MMR receipt", rsv_tests = "RSV tests", 
                      flu = "Influenza", self_harm = "self-harm", covid = "COVID", 
                      rsv = "RSV", obesity = "BMI", obesity = "obesity", ed_opioid = "OPIOID", 
                      ed_firearm = "Firearm", ed_workplace = "Workplace", ed_fall = "Diagnoses: Fall", 
                      ed_drowning = "Drowning", all_encounters = "All ED Encounters", 
                      all_patients = "All Patients")
  standard_names <- toupper(standard_names)
  meta_string <- toupper(paste(unlist(meta), collapse = " "))
  for (i in seq_along(standard_names)) {
    if (grepl(standard_names[[i]], meta_string, fixed = TRUE)) {
      meta$standard_name = names(standard_names)[[i]]
      break
    }
  }
  
  id_vars <- c("state", "county", "age", "year", "month", "week")
  
  id_cols <- seq_len(length(strsplit(lines[data_start], "^,|Measures,")[[1L]]) - 
                       1L)
  
  head2 <- strsplit(lines[data_start + 1L], ",", fixed = TRUE)[[1L]]
  
  matches <- which(sapply(head2, function(el)
    any(sapply(id_vars, function(id)
      grepl(id, el, ignore.case = TRUE))
    )
  ))
  
  header <- c(head2[matches], 
              strsplit(lines[data_start], ",", fixed = TRUE)[[1L]][-matches])
  
  if("ED Diagnoses" %in% header){
    header <- header[-which(header=='ED Diagnoses')]
  }
  if(sum(grepl('Initial Encounter', header))>0){
    header <- header[-grep('Initial Encounter', header)]
  }
  
  data <- arrow::read_csv_arrow(full_path, col_names = header, 
                                col_types = paste(rep("c", length(header)), collapse = ""), 
                                na = c("", "-"), skip = data_start + 1L)
  percents <- grep("^(?:Percent|Base|RSV test)", header)
  if (length(percents)) {
    for (col in percents) {
      data[[col]] <- sub("%", "", data[[col]], fixed = TRUE)
    }
  }
  number <- grep("Number", header, fixed = TRUE)
  if (length(number)) {
    for (col in number) {
      data[[col]][data[[col]] == "10 or fewer"] <- 5L
    }
  }
  for (col in id_cols) {
    data[[col]] <- vctrs::vec_fill_missing(data[[col]], "down")
  }
  if (all(c("Measures", "Base Patient") %in% colnames(data))) {
    data <- Reduce(merge, lapply(split(data, data$Measures), 
                                 function(d) {
                                   measure <- d$Measures[[1L]]
                                   d[[measure]] <- d[["Base Patient"]]
                                   d[, !(colnames(d) %in% c("Measures", "Base Patient"))]
                                 }))
  }
  colnames(data) <- standard_columns(colnames(data))
  if (meta$standard_name == "obesity") {
    meta$standard_name <- paste0(meta$standard_name, "_", 
                                 if ("state" %in% colnames(data)) 
                                   "state"
                                 else "county")
  }
  else if (meta$standard_name == "all_encounters" && "week" %in% 
           colnames(data)) {
    meta$standard_name = "all_encounters_weekly"
  }
  if ("age" %in% colnames(data)) {
    data <- data %>% filter(!is.na(age))
    std_age <- standard_age(data$age)
    missed_ages <- (data$age != "No value") & is.na(std_age)
    if (any(missed_ages)) {
      std_age[missed_ages] <- data$age[missed_ages]
      missed_levels <- unique(data$age[missed_ages])
      cli::cli_warn("missed age levels: {.field {missed_levels}}")
    }
    data$age <- std_age
  }
  list(metadata = meta, data = data)
}

standard_columns <- function(cols) {
  cols <- gsub(" ", "_", sub("number of ", "n_", tolower(cols)), fixed = TRUE)
  cols[grep("^age", cols)] <- "age"
  cols[grep("^state", cols)] <- "state"
  cols[grep("^county", cols)] <- "county"
  cols[grep("bmi_30", cols)] <- "bmi_30_49.8"
  cols[grep("hemoglobin_a1c_7", cols)] <- "hemoglobin_a1c_7"
  cols[grep("mmr_receipt", cols)] <- "mmr_receipt"
  cols[grep("OPIOID", toupper(cols))] <- "opioid_ed"
    cols[grep("^rsv_tests", cols)] <- "rsv_tests"
  cols
}

standard_age <- function(age) {
  c(
    `less than 1` = "<1 Years",
    `1 and < 2` = "1-2 Years",
    `2 and < 3` = "2-3 Years",
    `3 and < 4` = "3-4 Years",
    `1 and < 5` = "1-4 Years",
    `1 year or more and less than 5` = "1-4 Years",
    `4 and < 5` = "4-5 Years",
    `less than 5` = "<5 Years",
    `5 and < 6` = "5-6 Years",
    `5 and < 18` = "5-17 Years",
    `5 or more and less than 18 (1)` = "5-17 Years",
    `6 and < 7` = "6-7 Years",
    `6 or more` = "6+ Years",
    `7 and < 8` = "7-8 Years",
    `8 and < 9` = "8-9 Years",
    `9 or more` = "9+ Years",
    `less than 10` = "<10 Years",
    `10 and < 15` = "10-14 Years",
    `15 and < 20` = "15-19 Years",
    `less than 18` = "<18 Years",
    `18 and < 25` = "18-24 Years",
    `18 and < 40` = "18-39 Years",
    `18 and < 45` = "18-44 Years",
    `18 and < 50` = "18-49 Years",
    `18 or more and less than 50` = "18-49 Years",
    `20 and < 40` = "20-39 Years",
    `25 and < 35` = "25-34 Years",
    `35 and < 45` = "35-44 Years",
    `40 and < 65` = "40-64 Years",
    `45 and < 55` = "45-54 Years",
    `45 and < 65` = "45-64 Years",
    `50 and < 65` = "50-64 Years",
    `50 or more and less than 64` = "50-64 Years",
    `55 and < 65` = "55-64 Years",
    `less than 65` = "<65 Years",
    `65 or more` = "65+ Years",
    `65 and < 110` = "65+ Years",
    `total` = "Total",
    `less than 15` = "<15 Years",
    `15 and < 25` = "15-25 Years",
    `25 and < 45` = "25-45 Years",
    `45 and < 65` = "45-64 Years",
    `65 Years or more`= "65+ Years",
    `65+`= "65+ Years",
    `45-64`= "45-64 Years"
    
  )[
    sub(" [Yy]ears", "", sub("^[^a-z0-9]+|:.*$", "", tolower(age)))
  ]
}
