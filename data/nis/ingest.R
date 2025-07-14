library(dcf)
library(tidyverse)
library(cdlTools)

#
# Download
#

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "fhky-rtsk",
  "raw",
  process$raw_state
)


if (!identical(process$raw_state, raw_state)) {
  
   data <- vroom::vroom("./raw/fhky-rtsk.csv.xz", show_col_types = FALSE) %>%
     rename(birth_year = `Birth Year/Birth Cohort`, dim1=`Dimension Type`, age=Dimension,vax_uptake=`Estimate (%)`, 
            samp_size_vax=`Sample Size`,ci=`95% CI (%)`) %>%
     dplyr::select(Vaccine,Geography, Dose, dim1, vax_uptake,samp_size_vax,ci, age,birth_year ,) %>%
     filter(grepl('MMR',Vaccine)|grepl('Varicella',Vaccine)|  grepl('DTaP',Vaccine)|  grepl('Hep A',Vaccine)|  
              grepl('Hep B',Vaccine)| grepl('Hib',Vaccine)|  grepl('PCV',Vaccine) |
              grepl('Combined 7',Vaccine) |  grepl('Polio',Vaccine) | grepl('Rotavirus',Vaccine) 
     ) 
     
     
     vroom::vroom_write(
     data,
     "standard/data.csv.gz",
     ","
   )
   
   # record processed raw state
   process$raw_state <- raw_state
   dcf::dcf_process_record(updated = process)
}