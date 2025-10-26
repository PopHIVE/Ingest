

library(tidyverse)
library(arrow)

#https://vaers.hhs.gov/eSubDownload/index.jsp?fn=2025VAERSDATA.csv
#https://vaers.hhs.gov/eSubDownload/index.jsp?fn=2025VAERSSYMPTOMS.csv
#https://vaers.hhs.gov/eSubDownload/index.jsp?fn=2025VAERSVAX.csv

raw_vaers <- function(update = F) {
  if (update == T) {
    test <- read_csv("https://vaers.hhs.gov/eSubDownload/index.jsp?fn=2025VAERSVAX.csv")
    #The files in '/raw/staging' are the raw files downloaded from the VAERS website. These are very large (up to 500MB). Here we ingest the files, combine into a single mega file for each, and then break up into small parquet files
    
    all.staging <- list.files('./raw/staging')
    allyears <- as.numeric(unique(substr(all.staging, 1, 4)))
    allyears <- allyears[!is.na(allyears)]
    
    #VAERS Symptoms files
    symptoms <- lapply(allyears, function(x)
      read_csv_arrow(paste0(
        './raw/staging/', x, 'VAERSSYMPTOMS.csv'
      ))) %>%
      bind_rows()
    
    arrow::write_dataset(
      symptoms,
      path = './raw/symptoms.parquet',
      format = 'parquet' ,
      max_rows_per_file = 50000
    )
    
    #VAERS Vaccine files
    vax <- lapply(allyears, function(x)
      read_csv(paste0('./raw/staging/', x, 'VAERSVAX.csv')))
    
    vax <- map_dfr(vax, ~ mutate_all(.x, as.character)) #convert everything to character
    
    vax <-  bind_rows(vax)
    
    arrow::write_dataset(
      vax,
      path = './raw/vax.parquet',
      format = 'parquet' ,
      max_rows_per_file = 50000
    )
    
    
    
    #VAERS data files
    
    vaersdata <- lapply(allyears, function(x)
      read_csv(paste0('./raw/staging/', x, 'VAERSDATA.csv')))
    
    vaersdata <- map_dfr(vaersdata, ~ mutate_all(.x, as.character)) #convert everything to character
    
    vaersdata <- bind_rows(vaersdata)
    
    arrow::write_dataset(
      vaersdata,
      path = './raw/vaersdata.parquet',
      format = 'parquet' ,
      max_rows_per_file = 10000
    )
  }
}


raw_vaers(update=F)


