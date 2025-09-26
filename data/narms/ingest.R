library(tidyverse)
library(dcf)

#
# Download and add files to the raw directory
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "jbhn-e8xn",
  "raw",
  process$raw_state
)

if (!identical(process$raw_state, raw_state)) {
  data_raw <- vroom::vroom("./raw/jbhn-e8xn.csv.xz", show_col_types = FALSE) 
    
    
    # record processed raw state
    process$raw_state <- raw_state
    dcf::dcf_process_record(updated = process)
}