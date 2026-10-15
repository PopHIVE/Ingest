# ascvd

10-year ASCVD risk, ages 40-70: 10-year ASCVD risk, ages 40-70, in percent (Veradigm).

No automated download -- place fresh `Results_ASCVD-<date>_<AREA>.csv` exports in `raw/` by hand, then run `dcf::dcf_process("ascvd")`.

## Caveats

- Outcomes are binned ranges only (`_low`, `_high`, `_label`); never average low/high into a point estimate.
- `time` is the last date of the period represented (01/01/2022 - 07/31/2025 -> 2025-07-31), not the extract date.
- Geography is ZCTA3 (not FIPS); `geography_type` (`national`, `state`, `zcta3`) is kept.
- Demographic enrichment columns in the raw exports are dropped.

## Output

`standard/data.csv.gz`: `geography`, `geography_type`, `time`, `ascvd_population`, `ascvd_risk_low`, `_high`, `_label`.
