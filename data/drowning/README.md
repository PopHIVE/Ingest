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
