# opioid

Opioid overdose: EMS encounters for opioid overdose per 100,000 (NEMSIS).

No automated download -- place fresh `Results_Opioid-<date>_<AREA>.csv` exports in `raw/` by hand, then run `dcf::dcf_process("opioid")`.

## Caveats

- Outcomes are binned ranges only (`_low`, `_high`, `_label`); never average low/high into a point estimate.
- `time` is the last date of the period represented (01/01/2022 - 07/31/2025 -> 2025-07-31), not the extract date.
- Geography is real county FIPS; `geography_type` (`national`, `state`, `county`) is kept.
- Demographic enrichment columns in the raw exports are dropped.

## Output

`standard/data.csv.gz`: `geography`, `geography_type`, `time`, `opioid_population`, `opioid_ems_rate_low`, `_high`, `_label`.
