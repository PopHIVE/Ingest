#https://dissc-yale.github.io/dcf/reference/dcf_download_cmsmmd.html
library(dcf)
library(tidyverse)
codebook <- dcf::dcf_download_cmsmmd(codebook_only = TRUE)

variable_codes <- dcf_standardize_cmsmmd()

#https://data.cms.gov/data-api/v1/mmd-tool/?_source=prev_final_long_fltr12_racecat_all_sexcat_all_23_p&population=f&year=23&geography=c&measure=v&condition=2&sexcat=.|IS%20NULL&agecat=4&dual=.|IS%20NULL&eligcat=.|IS%20NULL&racecat=.|IS%20NULL&fltr=1&_size=500000



codes_to_test <- c(1, 2, 4, 5, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22, 26, 28, 29, 30, 31,
                   41, 42, 43, 44, 45, 47, 48, 49, 50, 51, 52, 53, 54, 57, 58, 59, 60, 61, 62, 64, 65, 66,
                   69, 70, 71, 73, 76, 78, 79, 80, 81, 82, 86, 88, 90, 91, 92, 93, 94, 95, 101, 102, 103,
                   104, 105, 106, 107, 108, 109, 110, 111, 144, 147, 149, 150, 153, 154, 155)

#downloaded <- lapply(codes_to_test, function(X) {
  download_df <- dcf_download_cmsmmd(
    "prevalence",
    population = "f",
    # geography='s',
    condition = codes_to_test,
    race = NULL,
    sex = NULL,
    age = NULL,
    adjust = 1
  )
  
  # convert codes to levels
  data_standard <- dcf_standardize_cmsmmd(download_df$data)
#  return(data_standard)
#}
#)
#downloaded_df <- bind_rows(downloaded)

  data_standard <- data_standard %>% group_by(year)

arrow::write_dataset(
  data_standard ,
  path = './raw/staging_api/all_staging.parquet',
  format = 'parquet' ,
  max_rows_per_file = 10000000
)


