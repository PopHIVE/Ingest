# bundle_county_access

This is a Data Collection Framework data bundle project, initialized with `dcf::dcf_add_bundle`.

## Output Files

- `dist/county_access.parquet` — County Health Rankings healthcare access measures, one row per county x year x `outcome_name` (`chr_*` measures).
- `dist/county_determinants.parquet` — county-level social, economic, environmental, and health-resource determinants from HUD CHAS, HRSA AHRF, BLS LAUS, and the USDA Food Access Research Atlas. Same long format plus a `source` column.
- `dist/state_determinants.parquet` — the state/national rows of the same sources (HUD CHAS, AHRF, BLS; USDA is county-only).

You can us the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_county_access", "..")
```
