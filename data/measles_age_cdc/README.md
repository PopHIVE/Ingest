# CDC Measles Cases by Age Group

National-level cumulative measles case counts by age group, manually recorded from the CDC measles surveillance page.

## Source

- **URL**: https://www.cdc.gov/measles/data-research/index.html
- **Update frequency**: Weekly (manual)

## Data Description

This dataset contains cumulative counts of confirmed measles cases in the United States, stratified by age group:

- **0-4**: Children under 5 years old
- **5-19**: Children and adolescents 5-19 years old
- **20+**: Adults 20 years and older
- **Unknown**: Cases with unknown age
- **Overall**: Total cumulative cases (all ages)

## Output Files

- `standard/data.csv.gz`: Standardized output with columns:
  - `geography`: FIPS code ("00" for national)
  - `time`: Date in MM-DD-YYYY format
  - `age`: Age group
  - `value`: Cumulative case count
