# maternal

Maternal Health -- severe obstetric complications (SOC): severe obstetric complications per 10,000 deliveries (HealthVerity).

No automated download -- place fresh `Results_Maternal-<date>_<AREA>.csv` exports in `raw/` by hand, then run `dcf::dcf_process("maternal")`.

## Caveats

- Outcomes are binned ranges only (`_low`, `_high`, `_label`); never average low/high into a point estimate.
- `time` is the last date of the period represented (01/01/2021 - 05/31/2025 -> 2025-05-31), not the extract date.
- Geography is ZCTA3 (not FIPS); `geography_type` (`national`, `state`, `zcta3`) is kept.
- The export labeled `AR` contains Arizona ZCTA3s and is remapped to `AZ`.
- Demographic enrichment columns in the raw exports are dropped.

## Output

`standard/data.csv.gz`: `geography`, `geography_type`, `time`, `maternal_population`, `maternal_soc_rate_low`, `_high`, `_label`.
