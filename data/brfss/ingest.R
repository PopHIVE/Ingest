library(tidyverse)
library(reshape2)
library(arrow)

process <- dcf::dcf_process_record()


raw_state <- dcf::dcf_download_cdc(
  "dttw-5yxu",
  "raw",
  parquet = TRUE,
  process$raw_state
)



# add files to the `raw` directory

#
# Reformat
#

# read from the `raw` directory, and write to the `standard` directory

if (!identical(process$raw_state, raw_state)) {
  
chronic <- open_dataset('./raw/dttw-5yxu.parquet') %>%
  filter(Topic %in% c('Diabetes','BMI Categories')) %>%
  filter( Break_Out_Category  %in% c( "Age Group",'Overall') & (Response %in% c('Yes')| grepl('Obese', Response)) & 
            Data_value_unit=='%') %>%
           filter(Locationdesc  !='All States, DC and Territories (median) **') %>%
  rename(age = Break_Out, 
         geography=LocationID,
         value_lcl= Confidence_limit_Low,
         value_ucl= Confidence_limit_High,
         value = Data_value,
         ) %>%
  mutate(time = as.Date(paste0(Year,'-01-','01'))) %>%
  dplyr::select(time,age, geography, Topic, Response,Sample_Size, Data_value_type, value, value_lcl, value_ucl)%>%
    collect() 
       
#How deal with multiple categories for obesity (pregnancy, pre diabetes)
wide1 <- chronic %>%
  dplyr::select(time,age, geography, Topic, Sample_Size,  value, value_lcl, value_ucl) %>%
  mutate( Topic = if_else(Topic=='BMI Categories','pct_obesity', 
                if_else(Topic=='Diabetes','pct_diabetes', NA_character_ 
                      )) 
          ) %>%
  rename(sample_size=Sample_Size) %>%
  reshape2::melt(.,id.vars=c('time','age','geography','Topic')) %>%
    reshape2::dcast(., time+age+geography~Topic+variable) 

vroom::vroom_write(wide1, "standard/data.csv.gz", ",")

# record processed raw state
process$raw_state <- raw_state
dcf::dcf_process_record(updated = process)

}
