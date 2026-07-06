# Enteric Diseases Bundle

This bundle combines NNDSS-reported enteric/gastrointestinal disease surveillance data for the PopHIVE platform.

## Data Sources

- **CDC NNDSS**: Weekly cumulative year-to-date case counts for campylobacteriosis, cholera, giardiasis, salmonellosis (excluding Typhi/Paratyphi), cyclosporiasis, typhoid fever (Salmonella Typhi), paratyphoid fever (Salmonella Paratyphi), Shiga toxin-producing E. coli (STEC), and shigellosis.

## Output Files

### enteric_diseases.parquet

Long-format weekly case counts by state and disease.

**Columns:**
- `geography`: State name or "United States"
- `date`: Week-ending date
- `year`: MMWR year
- `week`: MMWR week
- `measure`: Disease identifier (see `measure_info.json` for definitions)
- `value`: Cumulative year-to-date case count
- `source`: Data source ("CDC NNDSS")

## Building the Bundle

From the project root:
```r
dcf::dcf_process("bundle_enteric_diseases", ".")
```

Or from this directory:
```r
source("build.R")
```
