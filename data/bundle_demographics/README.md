# bundle_demographics

This is a Data Collection Framework data bundle project, initialized with `dcf::dcf_add_bundle`.

Population-structure measures from the U.S. Census Bureau, kept in one place so
other bundles can join to them as denominators or context rather than each
carrying its own copy.

## Output Files

Both files share the long format `geography`, `time`, `measure`, `value`, `source`.

- `dist/demographics_county.parquet` — county-level (5-digit FIPS) population
  counts and shares by age band, sex, and race/ethnicity, plus median age, age
  dependency ratio, disability rate, race-ethnicity diversity index, and median
  home value.
- `dist/demographics_state.parquet` — the state (2-digit FIPS) and national
  (`"00"`) rows of the same measures.

## Sources

- `census/standard/data_state.csv.gz`, `census/standard/data_county.csv.gz` —
  ACS 5-year estimates, vintages 2019–2024 (`source = "Census ACS 5-Year"`).
- `census/standard/data_pep.csv.gz` — Population Estimates Program, 2023 vintage
  (`source = "Census PEP"`). This file carries national, state, and county rows
  together, so the build splits it by FIPS length.

ACS and PEP measure overlapping concepts with different methodologies and
race/ethnicity classifications. They are reported side by side and distinguished
by `source` rather than reconciled into a single series.

## Related bundles

The census source's other measure families deliberately live elsewhere, so each
measure has exactly one home:

- `acs_BTH` (fertility) → `bundle_maternal_health`
- ACS social determinants, SAHIE, SAIPE, urban/rural allocation, 2020 Census
  self-response → `bundle_county_access`
  (`county_determinants.parquet` / `state_determinants.parquet`)

You can us the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_demographics", "..")
```
