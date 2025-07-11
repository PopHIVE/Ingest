#https://github.com/DISSC-yale/dcf
#remotes::install_github("dissc-yale/dcf")

library(dcf)
library(tidyverse)

##RUN ONCE 
# dcf_init('pophive')
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

dcf_build()
