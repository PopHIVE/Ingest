#https://dissc-yale.github.io/dcf/reference/dcf_download_cmsmmd.html
library(dcf)
codebook <- dcf::dcf_download_cmsmmd(codebook_only = TRUE)

variable_codes <- dcf_standardize_cmsmmd()

#https://data.cms.gov/data-api/v1/mmd-tool/?_source=prev_final_long_fltr12_racecat_all_sexcat_all_23_p&population=f&year=23&geography=c&measure=v&condition=2&sexcat=.|IS%20NULL&agecat=4&dual=.|IS%20NULL&eligcat=.|IS%20NULL&racecat=.|IS%20NULL&fltr=1&_size=500000

downloaded <- dcf_download_cmsmmd(
  "prevalence",
  population = "f",
 # geography='s',
  condition=109,
  #year=2015,
  race = NULL,
  sex = NULL,
  age = NULL,
  adjust = 1
)

# convert codes to levels
data_standard <- dcf_standardize_cmsmmd(downloaded$data)

arrow::write_parquet(
  data_standard, './raw/staging_api/all_staging.parquet',

)


data_standard 
