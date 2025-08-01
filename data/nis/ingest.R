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
     ) %>%
     filter(
       Geography %in%
         c(state.name, 'District of Columbia', 'United States') &
         birth_year %in%
         c(
           '2011',
           '2012',
           '2013',
           '2014',
           '2015',
           '2016',
           '2017',
           '2018',
           '2019',
           '2020',
           '2021',
           '2022',
           '2023',
           '2024',
           '2025'
         ) &
         dim1 == 'Age'
     )%>%
     mutate(
       vax_order = as.numeric(as.factor(Vaccine)),
       Vaccine_dose = as.factor(paste(Vaccine, Dose)),
       Vaccine_dose = gsub('NA', '', Vaccine_dose),
       Vaccine_dose = trimws(Vaccine_dose)
     ) %>%
     rename(pct_uptake = vax_uptake) %>%
     separate(ci, into = c("pct_uptake_lcl", "pct_uptake_ucl"), sep = " to ", convert = TRUE) %>%
     dplyr::select(Geography, birth_year, age, Vaccine_dose, pct_uptake,pct_uptake_lcl,pct_uptake_ucl,samp_size_vax) %>%
     rename(statename = Geography, vaccine = Vaccine_dose,   sample_size=samp_size_vax) %>%
     mutate(geography = cdlTools::fips(statename, to='FIPS'),
            geography = if_else(statename=='United States', 0,geography),
            geography = sprintf("%02d", geography),
            
            age_months = if_else(grepl('Month', age), as.numeric(gsub("\\D", "", age)),
                              if_else(grepl('Day',age),0, 
                                      NA_real_)),
            age_days = age_months * (365/12), 
            time= as.Date(paste(birth_year,'01','01', sep='-')) + age_days
                          ) %>%
     dplyr::select(-statename, -age_months, -age_days)
   
     
     
     vroom::vroom_write(
     data,
     "standard/data.csv.gz",
     ","
   )
   
   # record processed raw state
   process$raw_state <- raw_state
   dcf::dcf_process_record(updated = process)
}
