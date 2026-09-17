# NIS-Teen

Adolescent (13-17) vaccination coverage from the CDC National Immunization
Survey-Teen, pulled from data.cdc.gov dataset `ee48-w5t6` (TeenVaxView).

`standard/data.csv.gz` holds the annual estimates (2006 onward) by vaccine,
dose, sex (HPV only), and age group (13-17, 13-15) for the states, DC, Puerto
Rico, Guam, the U.S. Virgin Islands, and the nation (`00`). `time` is the end
of the survey year.

The four `data_*.csv.gz` files hold CDC's pooled 2018-2022 estimates for ages
13-17 by insurance coverage, poverty status, race and ethnicity, and
urbanicity. They are dated `2022-12-31` with `survey_years = "2018-2022"`, and
each includes the pooled `Overall` row.

HHS regions and the sub-state local areas CDC publishes (city and county
oversamples) are dropped. The `>=` glyph in CDC's dose labels is written out
as `>=`.

```r
dcf::dcf_process("nis_teen")
```
