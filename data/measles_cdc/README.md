# CDC Measles Weekly Cases

National-level weekly measles case counts from the CDC.

## Data Source

- **URL**: https://www.cdc.gov/wcms/vizdata/measles/MeaslesCasesWeekly.json
- **Provider**: Centers for Disease Control and Prevention (CDC)
- **Update Frequency**: Weekly

## Output Files

- `standard/data.csv.gz`: National weekly case counts with columns:
  - `geography`: FIPS code ("00" for national)
  - `time`: Week ending date (MM-DD-YYYY, Saturday)
  - `value`: Number of cases reported
