# heroes

Thin pull-script for [PopHIVE/heroes](https://github.com/PopHIVE/heroes) — ARPA-H HEROES program area-level snapshots (Maternal Health SOC rate, ASCVD 10-year risk, Opioid overdose EMS rate).

`ingest.R` does no transformation — it downloads `standard/data_maternal.csv.gz`, `data_ascvd.csv.gz`, `data_opioid.csv.gz`, and `measure_info.json` from the standalone repo and drops them here for bundling. All ingest logic, source documentation, and data caveats (binned-range values, ZCTA3 vs. county geography, the `time` placeholder) live in the [heroes repo's own README](https://github.com/PopHIVE/heroes#readme).

## Usage

```r
source("ingest.R")
```
