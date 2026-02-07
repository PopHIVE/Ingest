library(vroom)
library(dplyr)

#loading in relevant datasets
cms_data <- vroom::vroom('data/cms_mmd/standard/data_state_county_age.csv.gz')
medicaid_data <- vroom::vroom('data/medicaid_quality/standard/data.csv.gz')

#exploring both datasets
glimpse(cms_data)
glimpse(medicaid_data)

head(cms_data)
head(medicaid_data)

#filter medicaid data to cancer screening variables
medicaid_cancer <- medicaid_data %>%
  filter(grepl("Breast Cancer Screening|Cervical Cancer Screening|Colorectal Cancer Screening", 
               measure_name))

#aggregate cms data to state level
cms_state <- cms_data %>%
  mutate(state_fips = substr(geography, 1, 2)) %>%
  select(state_fips, time, age, race_ethnicity, sex,
         cms_scrn_prvnt_colorectal_cancer,
         cms_scrn_prvnt_mammogram,
         cms_scrn_prvnt_prostate_cancer,
         cms_scrn_prvnt_pap_test) %>%
  group_by(state_fips, time, age, race_ethnicity, sex) %>%
  summarise(across(starts_with("cms_"), ~ mean(.x, na.rm = TRUE)), .groups = "drop")

#create dist file
dir.create("dist", showWarnings = FALSE)

#create parquet files
arrow::write_parquet(cms_state, 'dist/cms_cancer_screening_state.parquet')
arrow::write_parquet(medicaid_data, 'dist/medicaid_quality.parquet')

cms_preview <- arrow::read_parquet('dist/cms_cancer_screening_state.parquet')
medicaid_preview <- arrow::read_parquet('dist/medicaid_quality.parquet')

View(cms_preview)
View(medicaid_preview)