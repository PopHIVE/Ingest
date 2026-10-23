# drowning

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```

## Ages 0-24 and all ages

`scrape_age_bands.py` pulls ages 0-24 as one WONDER cell per geography and year by
filtering the age variable to those groups (nothing is summed afterwards), so a cell
is suppressed only if all of 0-24 has 1-9 deaths. A second query has no age filter:
all drowning deaths not stratified by age (`age = "Overall"`; WONDER's own total, not a
sum of sexes). National and state filter the Ten-Year Age Groups (<1, 1-4, 5-14,
15-24); county filters the equivalent Five-Year groups (<1 ... 20-24) because CDC omits
county Population and Crude Rate when the Ten-Year variable is used. County is one
all-states query per band and year (42 queries in all, about 40 minutes):

```bash
.venv/Scripts/python scrape_age_bands.py --levels national state county
```

Output: `raw/ageband_<level>_ucd_icd10_expanded_2018_2024.parquet`, which `ingest.R`
adds to `standard/data_state.csv.gz` (national + state) and
`standard/data_county.csv.gz` as age levels `0-24` and `Overall`. The direct
all-ages `Overall` replaces the Overall row that was summed from the sexes. 0-24
overlaps the ten-year age rows, so never sum across age.

Suppressed death counts (1-9) are filled in with 5 in every row, with
`suppressed_flag = 1` and the rate recomputed from it; confidence bounds stay
`-999` there. `-999` otherwise means no value could be computed.

## National rows

State and county data come from manual CDC WONDER downloads
(`raw/ucd_icd10_expanded_2018_2024.parquet`). National rows (`geography = "00"`,
written to `standard/data_state.csv.gz`) come from WONDER's own national queries,
scraped separately with [PopHIVE/wonder-scraper](https://github.com/PopHIVE/wonder-scraper):

```bash
cd data/drowning   # from the repo root
python -m venv .venv
.venv/Scripts/python -m pip install selenium webdriver-manager pandas pyarrow \
    "git+https://github.com/govex/cdc-wonder-scraper.git"
.venv/Scripts/python scrape_national.py   # needs Chrome; a few minutes
```

This writes `raw/national_ucd_icd10_expanded_2018_2024.parquet`; `ingest.R` picks
it up (it never scrapes). Re-run the scrape only when WONDER adds a data year.
