#https://github.com/DISSC-yale/dcf
#remotes::install_github("dissc-yale/dcf")

library(dcf)
library(tidyverse)

dcf_build()

##RUN ONCE from parent directory (not within an existing project) 
# dcf_init('PopHIVE_DataStandards')
#########

###########
#Add new sources
###########

#dcf_add_source("nssp")
#dcf_add_source("gtrends")
#dcf_add_source("NREVSS")
#dcf_add_source("wastewater")
#dcf_add_source("epic")
#dcf_add_source("brfss")
#dcf_add_source("respnet") 
#dcf_add_source("nis") 
#dcf_add_source("abcs")
#dcf_add_source("abcs")
#dcf_add_source("nchs_mortality")

#dcf_add_source("schoolvaxview")

###########################
#Process individual sources
###########################
# dcf_process("nssp")
# dcf_process("gtrends")
# dcf_process("NREVSS")
# dcf_process("wastewater")
# dcf_process("epic")
# dcf_process("brfss")
# dcf_process("respnet")

## Add bundles
### dcf::dcf_add_bundle("bundle_respiratory")

##Process bundle
#For bundle projects, you can list the standard files you're using in process.json, and then you can read that in with dcf_process_record
### dcf::dcf_process("bundle_respiratory", ".")
dcf_build()
