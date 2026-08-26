# Vector-Borne Diseases Bundle

This bundle combines CDC ArboNET arboviral disease surveillance data with
NNDSS-reported vector-borne, tick-borne, and travel-associated disease case
counts for the PopHIVE platform.

## Data Sources

- **CDC ArboNET**: Annual national, state, and county-level human disease
  case counts (total and neuroinvasive), non-human activity indicators, and
  West Nile virus presumptive viremic blood donor counts, for Eastern
  equine encephalitis, Jamestown Canyon, La Crosse, Powassan, St. Louis
  encephalitis, and West Nile viruses.
- **CDC NNDSS**: Weekly cumulative year-to-date case counts, at the national
  and state level, for chikungunya, Eastern/Western equine encephalitis,
  Jamestown Canyon, La Crosse, Powassan, St. Louis encephalitis, and West
  Nile virus disease (the ArboNET-overlapping arboviral diseases), plus
  babesiosis, dengue virus infections (dengue, dengue-like illness, severe
  dengue), ehrlichiosis/anaplasmosis (Anaplasma phagocytophilum, Ehrlichia
  chaffeensis, Ehrlichia ewingii, and undetermined), malaria, and
  non-congenital Zika virus disease.

## Output File

### vector_borne.parquet

Single long-format file combining ArboNET's national/state/county annual
measures with NNDSS's national/state weekly cumulative case counts,
distinguished by `source` and `geography_level`.

**Columns:**
- `geography`: For `geography_level` "national"/"state", the state/territory
  name or "United States". For `geography_level` "county", the 5-digit
  county FIPS code. Connecticut reported by county through 2022 and by
  planning region (FIPS 09110-09190) from 2023 onward, so its county
  geography codes change mid-series.
- `geography_level`: "national", "state", or "county". NNDSS contributes
  national and state rows only; ArboNET contributes all three.
- `date`: Year-ending date (ArboNET) or MMWR week-ending date (NNDSS)
- `measure`: Disease/measure identifier (see `measure_info.json` for
  definitions)
- `value`: Case count, non-human activity indicator, or blood donor count
  (ArboNET), or cumulative year-to-date case count (NNDSS)
- `source`: Data source ("CDC ArboNET" or "CDC NNDSS")

## Building the Bundle

From the project root:
```r
dcf::dcf_process("bundle_vector_borne", ".")
```

Or from this directory:
```r
source("build.R")
```
