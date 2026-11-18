# nsch — National Survey of Children's Health

Mirror of [PopHIVE/nsch](https://github.com/PopHIVE/nsch). `ingest.R` downloads
`standard/data_age.csv.gz`, `data_age_sex.csv.gz`, `data_age_race_ethnicity.csv.gz`
and `measure_info.json` from that repository; the download from the Census Bureau,
the Title V measure coding and the survey estimation all happen there. Edit the
method or the measure descriptions in PopHIVE/nsch, not here.

Single-year, design-based state and national estimates (2016 onward) by age
(`0-5`, `6-11`, `12-17`, `Overall` = 0-17), age x sex and age x race/ethnicity.
Each measure has `_lcl`, `_ucl`, `_n`, `_suppressed`, `_unreliable` and
`_not_asked` columns; suppressed and not-asked estimates are left missing.

Feeds `bundle_youth_wellbeing` (`dist/nsch_state_age_demographics.parquet`).
