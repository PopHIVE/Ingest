# ascvd

Mean 10-year ASCVD risk, ages 40-70 (Veradigm), by 3-digit ZIP, overall and by age, race, and sex.

No automated download -- place the `data_submission_VERADIGM_<yyyy-mm>.xlsx` submission in `raw/` by hand, then run `dcf::dcf_process("ascvd")`. Only the first tab ("ASCVD BY ZIP3") is used.

## Caveats

- Risk is a mean score stored in the source as a proportion; it is converted to percent.
- `time` is the last date of the period represented (01/01/2022 - 08/31/2025 -> 2025-08-31).
- Geography is ZCTA3 (not FIPS); `geography_type` (`national`, `zcta3`) is kept. The source's blank-zip3 row is treated as the national total.
- Each row varies one of age / race_ethnicity / sex; the others are `Overall`. Age `70+` is the source's `age_70` column. Strata with no observations are omitted.

## Output

`standard/data.csv.gz`: `geography`, `geography_type`, `time`, `age`, `race_ethnicity`, `sex`, `ascvd_risk`, `ascvd_n`.
