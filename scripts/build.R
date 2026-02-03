#https://github.com/DISSC-yale/dcf
#remotes::install_github("dissc-yale/dcf")

library(dcf)
library(tidyverse)

dcf_build()

##RUN ONCE from parent directory (not within an existing project) 
# dcf_init('ingest')
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
#dcf::dcf_add_source("delphi_nhsn")
#dcf::dcf_add_source("delphi_hospital_claims")
#dcf::dcf_add_source("delphi_doctors_claims")
#dcf::dcf_add_source("wonder_provisional_mortality")
#dcf::dcf_add_source("cms_mmd")
#dcf::dcf_add_source("medicaid_quality")
#dcf::dcf_add_source("vaers")
#dcf::dcf_add_source("narms")
#dcf::dcf_add_source("wisqars")
#dcf::dcf_add_source("atlas_amr")

#dcf::dcf_add_source("nnds")

#dcf_add_source("schoolvaxview")

###########################
#Process individual sources
###########################
# dcf_process("nssp")
# dcf_process("gtrends")
# dcf_process("NREVSS")
# dcf_process("wastewater")
# dcf::dcf_process("epic")
# dcf::dcf_process("brfss")
# dcf_process("respnet")
# dcf_process("delphi_hospital_claims")
# dcf_process("delphi_ili_fluview")

# dcf_process("delphi_nhsn")
# dcf_process("nis")
# dcf_process("cms_mmd")
# dcf_process("schoolvaxview")
# dcf_process("wisqars")
# dcf_process("nchs_mortality")


#check structure of the files before merging
#dcf::dcf_check('brfss')
#dcf::dcf_check('epic')
#dcf::dcf_check('nchs_mortality')
#dcf::dcf_check('abcs')
#dcf::dcf_check('epic')
#dcf::dcf_check('bundle_respiratory')


## Add bundles
### dcf::dcf_add_bundle("bundle_respiratory")
### dcf::dcf_add_bundle("bundle_chronic_diseases")
### dcf::dcf_add_bundle("bundle_childhood_immunizations")
### dcf::dcf_add_bundle("bundle_injury_overdose")


##Process bundle
#run these from the relevant bundle directory
# dcf::dcf_process("bundle_respiratory", ".")
# dcf::dcf_process("bundle_childhood_immunizations", ".")
# dcf::dcf_process("chronic_diseases", ".")
# dcf::dcf_process("bundle_injury_overdose", ".")

#Update mermaid diagram
#dcf_status_diagram()


#dcf::dcf_status_diagram()

#dcf::dcf_init() sets up github actions
