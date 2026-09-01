# census

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

An **umbrella source** covering five U.S. Census Bureau programs. `ingest.R`
follows the repo's "Multiple Data Sources in One Directory" convention (see
CLAUDE.md): one script, one `process.json`, but each program downloaded,
guarded, and recorded independently.

## Programs

| Program | Endpoint / file | Output | Geography | Vintage | Prefix |
|---|---|---|---|---|---|
| ACS 5-year | `acs/acs5`, `acs/acs5/subject` | `data_state.csv.gz`, `data_county.csv.gz` | state + national `"00"`; county | 2019–2024 | `acs_` |
| Urban/rural allocation | `2020_UA_COUNTY.xlsx` | *appends columns to* `data_county.csv.gz` | county only | 2020 | `census_ur_` |
| Population Estimates (PEP) | `pep/charv` | `data_pep.csv.gz` | national + state + county | 2023 | `pep_` |
| Income & Poverty (SAIPE) | `timeseries/poverty/saipe` | `data_saipe.csv.gz` | national + state + county | 2024 | `saipe_` |
| Health Insurance (SAHIE) | `timeseries/healthins/sahie` | `data_sahie.csv.gz` | national + state + county | 2024 | `sahie_` |
| Operational Quality (OQM) | 2020 Decennial release 4 `.xlsx` | `data_oqm.csv.gz` | national + state + county | 2020 | `oqm_` |

`data_pep`, `data_saipe`, `data_sahie` and `data_oqm` each carry national, state
**and** county rows in a single file. Consumers split them by
`nchar(geography)`. Keep that shape.

## Independent change detection

Each program has its own guard, so a normal run short-circuits in seconds and a
failure in one program does not lose another's progress. Each calls
`dcf::dcf_process_record()` separately.

| Block | Guarded on |
|---|---|
| `sdoh` | `process$last_vintage_year` vs the latest ACS vintage |
| `ur` | md5 of the raw xlsx (`process$ur_state`), plus a check that `census_ur_*` columns are present |
| `pep` | `process$pep_vintage_year` |
| `saipe` | `process$saipe_year` |
| `sahie` | `process$sahie_year` |
| `oqm` | md5 of the raw xlsx (`process$oqm_state`) |

### Forcing a rebuild

`dcf::dcf_process(force = TRUE)` only decides whether `ingest.R` *runs* — it
does not bypass the guards above. So a change to a **derivation** in this script
(a corrected formula or unit rescale) will not propagate on its own, because the
upstream vintage is unchanged. Use:

```bash
CENSUS_FORCE_REBUILD=sdoh Rscript -e 'dcf::dcf_process("census", ".", force = TRUE)'
```

`CENSUS_FORCE_REBUILD` takes `all` or a comma-separated subset of
`sdoh,ur,pep,saipe,oqm,sahie`. Forcing `sdoh` re-pulls ACS for every year and
geography level (~3 minutes).

## Ordering constraint

`sdoh` must run before `ur`. The urban/rural block does not write its own file —
it reads the county file `sdoh` just wrote, drops any existing `census_ur_*`
columns, and re-joins. This is self-healing: rewriting `data_county.csv.gz`
removes those columns, which makes `ur_cols_present` false and re-fires the join
automatically.

## Unit conventions

All rates and shares are **proportions on a 0–1 scale**, matching `bls_laus`,
`hud_chas` and `usda_food_access`. SAIPE's `SAEPOVRT0_17_PT` and SAHIE's
`PCTUI_PT` arrive as 0–100 percentages and are rescaled on ingest; so are the
ACS income-quintile shares from table B19082. Exceptions, all documented in
`measure_info.json`: `acs_OWS` (unbounded S80/S20 ratio), `acs_DEP`
(dependency ratio), `acs_GNI` / `acs_REX` (0–1 index), `acs_AGE` (years),
`acs_POP*` / `pep_population` (person counts), and `acs_INB`, `acs_INC`,
`acs_PCI`, `acs_VAL`, `saipe_median_household_income` (nominal dollars, **not**
inflation-adjusted).

`ACS_NA_CODES` strips the Census sentinel values (`-666666666`, `-999999999`, …)
that would otherwise read as real observations.

## Consumers

| Bundle | Takes |
|---|---|
| `bundle_census` | everything (source-complete mirror, no allow-list) |
| `bundle_county_access` | ACS social determinants, SAHIE, SAIPE, urban/rural, OQM |
| `bundle_maternal_health` | `acs_BTH`, as `birth_rate` |

PopHIVE/us-rates reads `standard/data_*.csv.gz` directly by path, so **renaming
or relocating these files is a breaking change** beyond this repo.

## Why one folder

The repo convention endorses multi-dataset source directories, and ACS and
urban/rural are genuinely coupled through `data_county.csv.gz`. If this ever
does need splitting, the clean cut is four-and-two: `pep`, `saipe`, `sahie` and
`oqm` each already write one standalone file with its own guard and its own
`_sources` entry, while ACS and urban/rural stay together.

## Commands

```R
dcf_check_source("census", "..")
dcf_process("census", "..")
```
