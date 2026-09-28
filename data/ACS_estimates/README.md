# ACS_estimates

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

Split out of `data/census/` (see that folder's README) to give the "2024
American Community Survey 5-Year Estimates, Powered by Metopio" dataset its
own source directory, separate from census's PEP/SAIPE/SAHIE/OQM programs.

## Programs

| Program | Endpoint / file | Output | Geography | Vintage | Prefix |
|---|---|---|---|---|---|
| ACS 5-year | `acs/acs5`, `acs/acs5/subject` | `data_state.csv.gz`, `data_county.csv.gz` | state + national `"00"`; county | 2019–2024 | `acs_` |
| Urban/rural allocation | `2020_UA_COUNTY.xlsx` | *appends columns to* `data_county.csv.gz` | county only | 2020 | `census_ur_` |

Urban/rural stays bundled with ACS (rather than living in `census/`) because
it does not write its own file -- it reads the county file the ACS block just
wrote, drops any existing `census_ur_*` columns, and re-joins.

## Independent change detection

Each program has its own guard, so a normal run short-circuits in seconds and
a failure in one does not lose the other's progress. Each calls
`dcf::dcf_process_record()` separately.

| Block | Guarded on |
|---|---|
| `sdoh` | `process$last_vintage_year` vs the latest ACS vintage |
| `ur` | md5 of the raw xlsx (`process$ur_state`), plus a check that `census_ur_*` columns are present |

### Forcing a rebuild

`dcf::dcf_process(force = TRUE)` only decides whether `ingest.R` *runs* — it
does not bypass the guards above. So a change to a **derivation** in this
script (a corrected formula or unit rescale) will not propagate on its own,
because the upstream vintage is unchanged. Use:

```bash
CENSUS_FORCE_REBUILD=sdoh Rscript -e 'dcf::dcf_process("ACS_estimates", ".", force = TRUE)'
```

`CENSUS_FORCE_REBUILD` takes `all` or a comma-separated subset of `sdoh,ur`.
Forcing `sdoh` re-pulls ACS for every year and geography level (~3 minutes).

## Ordering constraint

`sdoh` must run before `ur`. The urban/rural block does not write its own
file — it reads the county file `sdoh` just wrote, drops any existing
`census_ur_*` columns, and re-joins. This is self-healing: rewriting
`data_county.csv.gz` removes those columns, which makes `ur_cols_present`
false and re-fires the join automatically.

## Unit conventions

All rates and shares are **proportions on a 0–1 scale internally**, converted
to the repo's 0-100 percent standard by `to_percent_scale()` before writing,
matching `bls_laus`, `hud_chas` and `usda_food_access`. So are the ACS
income-quintile shares from table B19082. Exceptions, all documented in
`measure_info.json`: `acs_OWS` (unbounded S80/S20 ratio), `acs_DEP`
(dependency ratio), `acs_GNI` / `acs_REX` (0–1 index), `acs_AGE` (years),
`acs_POP*` (person counts), and `acs_INB`, `acs_INC`, `acs_PCI`, `acs_VAL`
(nominal dollars, **not** inflation-adjusted).

`ACS_NA_CODES` strips the Census sentinel values (`-666666666`, `-999999999`,
…) that would otherwise read as real observations.

## Consumers

| Bundle | Takes |
|---|---|
| `bundle_census` | everything (source-complete mirror, no allow-list) |
| `bundle_county_access` | ACS social determinants, urban/rural |
| `bundle_maternal_health` | `acs_BTH`, as `birth_rate` |

`census/standard/data_*.csv.gz` files were previously read directly by path
by PopHIVE/us-rates. If it also reads `data_state.csv.gz`/`data_county.csv.gz`,
update its paths to point at `ACS_estimates/standard/` instead — **this is a
breaking change beyond this repo.**

## Commands

```R
dcf_check_source("ACS_estimates", "..")
dcf_process("ACS_estimates", "..")
```
