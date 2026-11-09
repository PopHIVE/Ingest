## Where can I learn more about the data and processing
Documentation for our databases can be found on our [Process documentation](https://pophive.github.io/processing-documentation/) page

## Data dictionary and rules for re-use for individual datasets
https://pophive.github.io/Ingest/

## See and search our available datasets
https://pophive.github.io/Ingest/data-table.html

## Check the data status
https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest

## Using these data

The data shown on PopHIVE.org are found in the Ingest project project in the ./Data/bundle_*/dist/ subfolders. The files are stored in either parquet or compressed csv format. If using R, parquet files can be downloaded using the arrow package in R. For example:

library(arrow)

url1 <- 'https://github.com/PopHIVE/Ingest/raw/refs/heads/main/data/bundle_respiratory/dist/covid_overall_trends.parquet'

ds1 <- read_parquet(url1)

compressed csv can be downloaded with vroom::vroom() in R:

url2 <- 'https://github.com/PopHIVE/Ingest/raw/refs/heads/main/data/nchs_mortality/standard/data_county.csv.gz'

ds2 <- vroom::vroom(url2)

In general, the data closest to the source data are found in the 'value' column. Some datasets also include a 3 week moving average (value_smooth), and a smoothed value, scaled to between 0-100 (value_smooth_scale). The data in 'value' are generally drawn directly from the source data. Exceptions include:

1)  In some datasets where national level data were not provided by the source, a national average is calculated using a population-weighted average.

2)  For Epic Cosmos, if the data are based on fewer than 10 counts, the cell is suppressed. For visualization purposes, this is filled in with a value halfway between 0 and the minimum value reported for that state. These values are indicated with suppressed_flag=1.

Time-stamped archives of the data are available in the Pulled Data folder.

## FAQ

*Can I re-use the data from PopHIVE?*

Yes! Much of the data are drawn from publicly available Federal datasets obtained from CDC or data.gov. Other data, including the results of research performed using Epic Cosmos or the data available through Google Health Trends, can be used with appropriate attribution. A suggested citation relating to this data is 'Results of research performed with Epic Cosmos were obtained from the PopHIVE platform [url for Github corresponding to the specific data source].’

Please cite the use of data from PopHIVE and the original source. the DOI for PopHIVE is [![DOI](https://zenodo.org/badge/1018069747.svg)](https://doi.org/10.5281/zenodo.17345935)


*Who is it for?* PopHIVE is designed for a broad audience: - Members of the public who want to understand what’s happening in their communities. - Clinicians who need to anticipate trends and adjust care. - Public health departments and local governments who need up-to-date data to allocate resources. - Researchers, journalists, and advocates working to tell stories and drive policy change. - Policy makers and decision-makers who need to understand the basics of who, what, and where about health issues occurring in the areas they serve.

*Can you show ZIP code-level data?* Because the data is de-identified, we can’t always go down to ZIP code level, especially for sensitive conditions like STIs or mental health outcomes. For some topics, like asthma or heat-related illness, we can show more granular data. Our data team is constantly working to expand local detail while protecting individual privacy.

*Will you show additional conditions in the future?* Yes. PopHIVE is evolving based on user needs and feedback. As high-quality, de-identified data becomes available, we plan to expand condition-specific dashboards, such as those for diabetes, maternal health, and behavioral health. Please provide us feedback on what you’d like to see here.

*How do I know the data is accurate or reliable?* PopHIVE’s data team continually evaluates the quality and representativeness of the data. In some cases (like diabetes Hemoglobin A1C data), completeness varies, and we are committed to transparency about what the data can and can’t tell us. This is an evolving platform, and we're building new functionality and insights over time.

*How are you using electronic health record data from Epic? Isn’t that a violation of HIPAA?* PopHIVE doesn’t change any rules or regulations around health data sharing. We only use de-identified, aggregate data, following all existing privacy laws. We’re not sharing individual patient records—we’re simply making existing public health trends more timely and accessible for the public good.

*Are you accepting additional data sets?* Yes! We welcome partnerships and are actively working to expand PopHIVE’s data offerings. If you have a reliable, de-identified dataset that could help improve public understanding of health, we’d love to hear from you. Please submit here.

*How can I give feedback on this tool?* We’d love to hear from you. PopHIVE is shaped by the people who use it. Whether you have a technical suggestion, want to request a feature, or share how it helped your community, please submit [here](https://docs.google.com/forms/d/e/1FAIpQLSchAasiq7ovCCNz9ussb7C2ntkZ-8Rjc7-tNSglkf5boS-A0w/viewform?pli=1).


# Major change log
**November 14, 2025**
We have updated several aspects of the obesity and diabetes definitions from Epic Cosmos. The denominator population has been updated to include base patients with an encounter, and a elevated HbA1c measurement or BMI>30 measurement in the 2 years prior to the encounter. This allows for stratification over time and more accurately captures the active users. We also change from a 10 year look back period to a 2 year look back period to be in line with the definitions used by the Medicare CCW. In addition to these changes, we have added two additional ways to measure diabetes and obesity prevalence based on the Epic Cosmos data. This is based on the [CCW definitions](https://www2.ccwdata.org/web/guest/home/), which evaluates the presence of diganostic codes for diabetes or obesity during a 2 year lookback period. The updated file can be found [here](https://github.com/PopHIVE/Ingest/blob/main/data/bundle_chronic_diseases/dist/prevalence_by_geography_and_year_and_source.parquet)

**November 21,2025**
The CDC updated their invsdive pneumococcal disease file to i clude geographic site for 1998-2023. The file with geographic stratification by serotype has been updated accordingly, and the dashboard now shows 2023 instead of 2019

**June 5, 2026**
The county-level MMR school vaccine coverage data obtained from the Washington Post for Tennessee was flagged as inaccurate by the Tennessee Department of Health, and they provided an alternative data source that is collected with a more robust methodology, which has now been included in the schoolvax_washpost data in its place

**August 25, 2026**
Delphi has upgraded to a new platform and changed how they smooth their data. This impacts Delphi Doctors Claims and Hospital Claims. Under the old system, the mathematical approach prevented case counts from ever reaching zero. The new system now provides daily updates smoothed using a rolling seven-day average, and we are pulling the Saturday value from this new feed to represent each week. This update impacts low values of flu cases but has very little impact on COVID-19 data. 

**October 31,2026**
There was a data parsing issue when ingesting the NSSP commercial lab testing data due to the presence of commas in the source numeric data that was causing data to be masked for certain states and inaccurate national aggregates.

**November 2, 2026**
Several bundle files changed shape so that every stratified file carries a row for the combined category and names its data source. The bundle_sex_disparities files now keep the source's both-sexes row as `sex = "Overall"` and carry a `source` column; the youth wellbeing drowning and concussion files gain an `All ages` row; and `flu_hospital_capacity` gains a `source` column. Values are unchanged.

**November 5, 2026**
The NARMS Now human clinical files (resistance by agent and by pattern) were rebuilt from a full re-scrape of the CDC dashboard. The dashboard's responses mark a cell as "same as the previous cell", and the parser treated the first cell of each row as missing instead of carrying that value over, so a share of cells in every year was recorded as `not_tested` when the dashboard actually reported a value; about 157,000 agent cells and 279,000 pattern cells now carry values. Recent years also pick up CDC's revisions, and data now extend through the current year. The scrape moved out of the twice-weekly build into a monthly workflow that only re-pulls the years whose national totals changed.

# Guide to adding data and rebuilding the bundle

## Steps for adding new datasets

###Create the data source folder

Run 
```r{} 
dcf_add_source("DATASETNAME")
```

### Convert raw files to standard format
Edit the ingest.R file. As an example, here we add a file from data.gov using dcf_download_cdc(). The goal is to download a raw file and convert to the [standard format](https://dissc-yale.github.io/dcf/articles/standards.html) 

```r{}

process <- dcf::dcf_process_record()
raw_state <- dcf::dcf_download_cdc(
  "kvib-3txy",
  "raw",
  process$raw_state
)


if (!identical(process$raw_state, raw_state)) {

#read in raw, filter, and do any formatting needed
data1 <- vroom::vroom('raw/kvib-3txy.csv.xz') %>%
    filter(Type=='Unadjusted Rate' & Sex=='Overall' & `Race/Ethnicity`=='Overall') %>%
    rename(virus= 'Surveillance Network',
           age = 'Age group',
           state = Site,
           time= 'Week Ending Date' ) %>%
    mutate( virus = if_else(grepl('COVID', toupper(virus)),'rate_covid',
                        if_else(grepl('RSV', toupper(virus)),'rate_rsv',   
                            if_else(grepl('FLU', toupper(virus)),'rate_flu',           
                                    'rate_any'                          
                                  )))
    ) %>%
    dcast( .,  time + age + state ~ virus, value.var = 'Weekly Rate') %>%
    mutate( rate_flu = if_else(is.na(rate_flu),0, rate_flu), #do not fill in below
            geography = if_else(state=='Overall', 0,
                                cdlTools::fips(state, to='FIPS'))
          
            ) %>%
    filter(age =='Overall') %>%
    dplyr::select(-state)


  #Write standard data
  vroom::vroom_write(
    data1,
    "standard/data.csv.gz",
    ","
  )
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
  

```

### Edit the measure_info.json
Each variable should have an entry. for example:

"rate_any": {
    "id": "rate_any",
    "short_name": "Number of laboratory confirmed cases of RSV, influenza or COVID-19 per 100,000 people",
    "long_name": "",
    "category": "",
    "short_description": "",
    "long_description": "",
    "statement": "",
    "measure_type": "Incidence",
    "unit": "Cases per 100,000 people",
    "time_resolution": "Week",
    "restrictions": "",
    "sources": [],
    "citations": []
  }
  
### Create a bundle

Groups of related datasets are combined into a bundle. For example run:
```{r}
dcf::dcf_process("bundle_respiratory", ".")
```
This creates a bundle folder for respiratory in the data folder

### Edit the bundle

Open the build.R file. This is where datasets should be combined and formatted into final 'production' formats. Output files are saved into the dist/ folder in whatever format is needed (e.g., parquet)

### Edit the process.json 

Any standard format files that are used in the bundle should be referenced in process.json. For example:

  "source_files": [
    "epic/standard/weekly.csv.gz",
    "gtrends/standard/data.csv.gz",
    "wastewater/standard/data.csv.gz",
    "abcs/standard/data.csv.gz",
    "abcs/standard/uad.csv.gz",
    "NREVSS/standard/data.csv.gz",
    "nssp/standard/data.csv.gz",
    "respnet/standard/data.csv.gz"
  ]
  
### Update and build the data
From the parent directory, run:

```{r}
dcf_build()
```

  
  
  


# Legal Disclaimer

These data and PopHIVE statistical outputs are provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors, contributors, or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the data or the use or other dealings in the data.

The PopHIVE statistical outputs are research tools intended for use in the fields of public health and medicine. They are not intended for clinical decision making, are not intended to be used in the diagnosis or treatment of patients and may not be useful or appropriate for any clinical purpose. Users of the PopHIVE statistical outputs should be aware of their responsibilities to ensure the ethical and appropriate use of this technology, including adherence to any applicable legal and regulatory requirements.

The content and data provided with the statistical outputs do not replace the expertise of healthcare professionals. Healthcare professionals should use their professional judgment in evaluating the outputs of the PopHIVE statistical outputs.

# Change log

Fixes that change published output data.

- **2026-11-09 — `medicaid_quality`**
  - **`standard/data.csv.gz` and `standard/data_combined.csv.gz` are removed** and replaced by `standard/data_child_core_set.csv.gz` and `standard/data_adult_core_set.csv.gz`. Bundles that read `data.csv.gz` must switch to the new files.
  - The new files are long (one row per state, year and measure) with columns `geography`, `time`, `domain`, `measure`, `measure_type`, `value`, `pct_25`, `pct_75`, `population_match`. Each value comes from the reported population closest to the Core Set's target (Child: all Medicaid + CHIP; Adult: non-dual Medicaid), flagged in `population_match`. See `data/medicaid_quality/README.md`.
  - Annual `time` is `YYYY-12-31` (was `YYYY-01-01`); the always-"Total" `age` / `sex` / `race_ethnicity` columns are dropped.
  - `measure_type` (whether higher or lower rates are better) is new, from CMS's field (FFY 2019 on), inferred from CMS quartile order for measures that ended earlier.
  - `pct_25` / `pct_75` are always the 25th / 75th percentiles of the state rates. From FFY 2016 CMS orders its quartiles by performance, so for lower-is-better measures (PCR, PQIs, AMB, LBW, ...) the two were previously swapped from 2016 on.
  - The National Core Indicators adult survey measures, re-abbreviated by CMS from NCIDDS-AD to NCIIDD-AD in FFY 2024 with identical definitions, are one series (`medicaid_nciidd_ad_*`, 2020–2025).
  - Each measure's `domain` is its most recent CMS label in every year (CMS used some other labels in FFY 2014).
- **2026-11-09 — bundles using `medicaid_quality`** (now read from the Core Set files; one Medicaid value per state, year and measure, chosen by the population hierarchy above)
  - `bundle_cancer_screening/medicaid_cancer_screening.parquet`, `bundle_preventative_services/medicaid_preventative_services.parquet`, `bundle_injury_overdose/medicaid_injury_overdose.parquet` and `bundle_youth_wellbeing/medicaid_state_payer.parquet`: the `payer` column is replaced by `population_match`, and each state-year has one row per measure instead of one per payer group. The youth wellbeing file keeps its name, but there are no longer separate payer rates for a payer selector.
  - `bundle_adolescent_vaccination/adolescent_vax_state.parquet`, `bundle_sti/sti_state.parquet` and `bundle_maternal_health/maternal_state.parquet`: new `population_match` column (blank for non-Medicaid rows). Medicaid values may differ where the closest population is not the combined Medicaid & CHIP rate used before, and now include child measures reported only for CHIP (`chip_only`) and adult measures reported only with dual eligibles (`broader_incl_duals`). Medicaid rows in `maternal_state.parquet` are dated `YYYY-12-31` like the other sources (were `YYYY-01-01`).
  - `bundle_preventative_services`: three Medicaid measures were labeled as screenings but measure care for people already diagnosed. They are renamed: "Depression Screening" → "Antidepressant Medication Management" (AMM-AD), "Cardiovascular Disease Screening" → "Controlling High Blood Pressure" (CBP-AD), "Diabetes Screening" → "HbA1c Testing in Adults with Diabetes" (HA1C-AD). They no longer pair with the Medicare screening rows of the same old names in `combined_preventative_services.parquet`, whose `value_medicaid` is now filled only for Chlamydia Screening and Influenza Vaccine.
  - `bundle_injury_overdose/medicaid_injury_overdose.parquet`: "Opioid Use Disorder" → "Medication Treatment for Opioid Use Disorder" (OUD-AD is the share of adults with OUD who received medication for it, higher is better, not how common OUD is); "Follow-Up After ED Visit for Alcohol and Drug Abuse" → "Follow-Up Within 30 Days After ED Visit for Substance Use" (CMS's current FUA-AD name).
  - `bundle_cancer_screening/medicaid_cancer_screening.parquet`: "Colorectal Cancer Screening" held only CMS's younger Medicaid age band; it is now "Colorectal Cancer Screening, Ages 46-50", and the older band is added as "Colorectal Cancer Screening, Ages 51-65" (FFY 2023: 46-49 and 50-64). Neither band pairs with the Medicare colorectal rows in `combined_cancer_screening.parquet`.
  - `bundle_youth_wellbeing/medicaid_state_payer.parquet`: the "Nutrition" focus area now uses `medicaid_wcc_ch_nutrition_rate` (WCC-CH counseling for nutrition, FFY 2020-2025) instead of `medicaid_wcc_ch_rate`, which is BMI-percentile documentation, not nutrition. Dashboards selecting `medicaid_wcc_ch_rate` must switch to the new measure id.
  - Description fixes (no data change): `bundle_sti` chlamydia screening names give the age groups (16-20, 21-24); `bundle_maternal_health` low-risk cesarean is for mothers under age 20, and postpartum care notes the visit window change in FFY 2020.
