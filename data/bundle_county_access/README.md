# bundle_county_access

This is a Data Collection Framework data bundle project, initialized with `dcf::dcf_add_bundle`.

## Output Files

- `dist/county_access.parquet` — County Health Rankings healthcare access measures, one row per county x year x `outcome_name` (`chr_*` measures).
- `dist/county_determinants.parquet` — county-level social, economic, environmental, and health-resource determinants from HUD CHAS, HRSA AHRF, BLS LAUS, the USDA Food Access Research Atlas, and the U.S. Census Bureau (ACS 5-year social determinants, SAHIE insurance coverage, SAIPE income and child poverty, the 2020 urban/rural allocation, and 2020 Census self-response). Same long format plus a `source` column.
- `dist/state_determinants.parquet` — the state/national rows of the same sources (HUD CHAS, AHRF, BLS, Census; USDA food access and the Census urban/rural allocation are county-only).

Census population-structure measures (`acs_POP*`, `acs_PCT_*`, `pep_*`, median
age, dependency ratio, disability, diversity index, median home value) are *not*
here — this bundle carries only the census measures that have cross-source
counterparts to sit beside.

The full census dataset, including the measures here, is also published as a
source-complete mirror in `bundle_census`. **This bundle is canonical for
analysis** — its measures are curated to be comparable across sources
(`acs_UMP` beside `bls_pct_unemployment`, `acs_UNS` beside `chr_uninsured`,
`acs_SNP` beside `usda_pct_limited_access_low_income`). Do not union the two
bundles; you will double-count.

You can us the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_county_access", "..")
```
