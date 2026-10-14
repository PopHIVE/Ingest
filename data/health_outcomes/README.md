# health_outcomes

Area-level snapshot exports for four health outcome topics, ingested by a single `ingest.R`:

| Topic | Measure | Source | Geography |
|-------|---------|--------|-----------|
| alcohol | EMS encounters for alcohol-related emergencies per 100,000 | NHTSA / NEMSIS | county FIPS |
| ascvd | 10-year ASCVD risk, ages 40-70 (%) | Veradigm | ZCTA3 |
| maternal | Severe obstetric complications per 10,000 deliveries | HealthVerity | ZCTA3 |
| opioid | EMS encounters for opioid overdose per 100,000 | NEMSIS | county FIPS |

No automated download -- place fresh `Results_<Topic>-<date>_<AREA>.csv` exports in `raw/` by hand, then run `dcf::dcf_process("health_outcomes")`.

## Caveats

- Outcomes are binned ranges only (`_low`, `_high`, `_label`); never average low/high into a point estimate.
- `time` is the last date of the period represented (2025-07-31 for alcohol/ascvd/opioid; 2025-05-31 for maternal), not the extract date.
- `geography_type` (`national`, `state`, `county`, `zcta3`) is kept because ZCTA3 codes are not FIPS.
- The maternal export labeled `AR` contains Arizona ZCTA3s and is remapped to `AZ`.
- Demographic enrichment columns in the raw exports are dropped.

## Output

One standard file per topic, each with `geography`, `geography_type`, `time`, `<topic>_population`, `<topic>_<measure>_low`, `_high`, `_label`:

- `standard/data_alcohol.csv.gz`
- `standard/data_ascvd.csv.gz`
- `standard/data_maternal.csv.gz`
- `standard/data_opioid.csv.gz`
