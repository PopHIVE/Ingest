# bundle_maternal_health

Maternal- and infant-health indicators assembled from standardized PopHIVE
sources. Each output is tall-format (`geography`, `time`, `measure`, `value`),
one row per geography × year × measure.

## Outputs (`dist/`)

| File | Geography | Measures |
|------|-----------|----------|
| `maternal_state.parquet`  | 2-digit FIPS (+ `00` national) | all 15 |
| `maternal_county.parquet` | 5-digit FIPS | 7 (census + CHR only) |

## Sources

| Source dir | Contributes | Geo |
|------------|-------------|-----|
| `census` (ACS) | `birth_rate` | state + county |
| `county_health_rankings` | `teen_birth_rate`, `low_birth_weight`, `infant_mortality`, `child_mortality`, `smoking_during_pregnancy`, `breastfeeding` | state + county |
| `medicaid_quality` (CMS Core Set, Medicaid payer) | `medicaid_prenatal_postpartum_care_adult`/`_child`, `medicaid_first_prenatal_visit`, `medicaid_contraceptive_postpartum_adult`/`_child`, `medicaid_low_birthweight`, `medicaid_low_birthweight_risk_adjusted` | state only |
| `cdc_vssr` (NCHS VSRR, provisional) | `maternal_mortality_rate` | national only |

## Known data-quality handling

- **CHR infant mortality, 2013 release** is dropped in `build.R`: that release
  stored a mis-scaled/unrelated metric in the column (state 483–1254, county up
  to 3042 per 1,000 — impossible). Every other year (2014–present) is correct.
  The proper fix belongs upstream in `county_health_rankings/ingest.R`.
- `medicaid_quality` is filtered to `payer == "Medicaid"` to avoid duplicate
  geography–year–measure rows across the Medicaid/CHIP/Total payer splits; its
  state names are mapped to FIPS via `resources/all_fips.csv.gz`.
- `cdc_vssr` is filtered to `age == "Overall" & race_ethnicity == "Overall"`
  before pivoting — the bundle's tall format has no demographic dimension, so
  only the national all-ages/all-races rate is included here. The full
  age/race-stratified breakdown is available directly from
  `cdc_vssr/standard/data.csv.gz`.

## Roadmap

Additional indicators depend on sources not yet ingested:

- **Causes of pregnancy-related death** — CDC PMSS
- **Gestational diabetes / hypertension, delivery method & location** — CDC WONDER / Epic Cosmos
- **Prenatal care adequacy** — PeriStats / March of Dimes
- **Severe obstetric complications / severe maternal morbidity (SMM)** — the CDC
  SMM composite is derived from HCUP/state hospital discharge data, which has no
  clean public API. Add as a `severe_obstetric_complications` measure once a
  source is available.

## Rebuild

```R
dcf::dcf_process("bundle_maternal_health", "..")
```
