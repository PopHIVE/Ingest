library(dcf)
library(tidyverse)
#dcf::dcf_process("epic")
dcf_process("measles_age_cdc2")
dcf_process("measles_jhu")

dcf::dcf_process("bundle_measles", ".")
