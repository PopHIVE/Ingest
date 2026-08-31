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
