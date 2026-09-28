# census

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

An **umbrella source** covering four U.S. Census Bureau programs. `ingest.R`
follows the repo's "Multiple Data Sources in One Directory" convention (see
CLAUDE.md): one script, one `process.json`, but each program downloaded,
guarded, and recorded independently.

The ACS 5-year estimates ("2024 American Community Survey 5-Year Estimates,
Powered by Metopio") and the 2020 Census urban/rural classification were split
out into `data/ACS_estimates/`; see that folder's README.

## Programs

| Program | Endpoint / file | Output | Geography | Vintage | Prefix |
|---|---|---|---|---|---|
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
CENSUS_FORCE_REBUILD=pep Rscript -e 'dcf::dcf_process("census", ".", force = TRUE)'
```

`CENSUS_FORCE_REBUILD` takes `all` or a comma-separated subset of
`pep,saipe,oqm,sahie`.

## Unit conventions

All rates and shares are **proportions on a 0–1 scale internally**, converted
to the repo's 0-100 percent standard by `to_percent_scale()` before writing,
matching `bls_laus`, `hud_chas` and `usda_food_access`. SAIPE's
`SAEPOVRT0_17_PT` and SAHIE's `PCTUI_PT` arrive as 0–100 percentages and are
rescaled on ingest the same way. Exceptions, all documented in
`measure_info.json`: `pep_population` (person counts) and
`saipe_median_household_income` (nominal dollars, **not** inflation-adjusted).

## Consumers

| Bundle | Takes |
|---|---|
| `bundle_census` | everything (source-complete mirror, no allow-list) |
| `bundle_county_access` | SAHIE, SAIPE, OQM |

PopHIVE/us-rates reads `standard/data_*.csv.gz` directly by path, so **renaming
or relocating these files is a breaking change** beyond this repo.

## Commands

```R
dcf_check_source("census", "..")
dcf_process("census", "..")
```
