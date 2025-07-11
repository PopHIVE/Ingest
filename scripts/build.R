#https://github.com/DISSC-yale/dcf
#remotes::install_github("dissc-yale/dcf")

library(dcf)
library(tidyverse)

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

dcf_build()
