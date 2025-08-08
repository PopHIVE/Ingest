# bundle_childhood_immunizations

This is a Data Collection Framework data bundle project, initialized with `dcf::dcf_add_bundle`.

You can us the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_childhood_immunizations", "..")
```

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

  
  
  
