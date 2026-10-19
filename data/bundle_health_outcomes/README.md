# bundle_health_outcomes

Health-outcome indicators assembled from four standardized PopHIVE sources.
Each source gets its own wide-format parquet in `dist/` rather than being merged
into one table, because the sources differ in geography, measure type, and
stratification.

## Outputs (`dist/`)

| File | Source dir | Geography | Measures |
|------|------------|-----------|----------|
| `ascvd.parquet` | `ascvd` (Veradigm) | `00` national + 3-digit ZCTA3 | `ascvd_risk` (mean 10-year ASCVD risk, %), `ascvd_n`; stratified by `age`, `race_ethnicity`, `sex` |
| `alcohol.parquet` | `alcohol` (NEMSIS) | FIPS: national, county | `alcohol_ems_rate_low`/`_high`/`_label`, `alcohol_population` |
| `opioid.parquet` | `opioid` (NEMSIS) | FIPS: national, state, county | `opioid_ems_rate_low`/`_high`/`_label`, `opioid_population` |
| `maternal.parquet` | `maternal` (HealthVerity) | `00` national + state + 3-digit ZCTA3 | `maternal_soc_rate_low`/`_high`/`_label`, `maternal_population` |

## Caveats

- `ascvd` and `maternal` use ZCTA3 codes, **not** FIPS; `geography_type` says
  which kind a row is. ZCTA3 codes are stored as strings (leading zeros kept).
- EMS and obstetric-complication outcomes are binned ranges only (`_low`,
  `_high`, `_label`). Never average low/high into a point estimate.
- `time` is the last date of the period each source represents, and differs by
  source (ascvd 2025-08-31, alcohol/opioid 2025-07-31, maternal 2025-05-31).
- This is distinct from `bundle_maternal_health`, which holds broader
  maternal- and infant-health indicators from other sources.

## Rebuild

```R
dcf::dcf_process("bundle_health_outcomes", clear_state = TRUE)
```
