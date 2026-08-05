# bundle_gas

Group A Streptococcus (GAS) surveillance, combining three sources at three very
different grains. Built by `build.R` into six long-format parquets under `dist/`.

| Parquet | Source | Grain | Measures |
|---|---|---|---|
| `epic_gas.parquet` | `epic_gas` | State + national, quarterly, by age | `n_strep_throat`, `pct_strep_throat`, `n_patients` |
| `nnds_stss.parquet` | `nnds` | State + national, weekly (MMWR) | `stss_cases_weekly`, `stss_cases_cumulative` |
| `abcs_gas.parquet` | `abcs_gas` | National, annual, by age/sex/race | `rate_cases`, `rate_deaths`, `N_cases`, `N_deaths` |
| `abcs_gas_syndromes.parquet` | `abcs_gas` | National, annual | `pct_syndrome_*` (5 syndromes) |
| `abcs_gas_resistance.parquet` | `abcs_gas` | National, annual | `pct_resistant_*` (6 antibiotics), `n_isolates` |
| `abcs_gas_emm.parquet` | `abcs_gas` | National, annual | `emm_pct_*` (16 types + other), isolate count |

All six share the bundle conventions: `geography` holds **state names** (or
`"United States"`), `time` is ISO `YYYY-mm-dd` period-end, and `value` is the
plotting column, keyed by a `measure` identifier column.

## Notes for anyone reading these files

- **`measure` mixes units within `value`.** In `epic_gas.parquet`, `n_*` measures
  are counts while `pct_strep_throat` is a percent; the ABCs files mix rates,
  percents, and isolate counts. Always filter or facet by `measure` before
  plotting.
- **NNDSS is published cumulatively.** The raw
  `streptococcal_toxic_shock_syndrome` column is a *year-to-date running total*
  that resets each MMWR year (national 2024 runs 5 → 647 across weeks 1–52).
  `build.R` de-accumulates it into `stss_cases_weekly`, which is the series to
  plot; `stss_cases_cumulative` retains the published form. The two are not
  additive. NNDSS sometimes revises earlier weeks downward, so a small number of
  weekly increments are negative (27 of 12,376 at the current build); these are
  left as reported rather than clamped, and `build.R` logs the count.
- **Aggregate levels overlap.** `epic_gas.parquet` carries an `age` level of
  `"Total"`, and `abcs_gas.parquet` carries `"Overall"` levels for `age`, `sex`,
  and `race_ethnicity`. Exclude these before summing across a stratification.
- **Geographic coverage is uneven.** Epic and NNDSS cover states plus a national
  total; the ABCs files are national only (a ~35 million person catchment area,
  not the whole US). Territories and non-state NNDSS jurisdictions (e.g. New York
  City) are dropped.
- **Two different strep toxic shock series exist.**
  `nnds_stss.parquet` gives national/state case *counts*, while
  `abcs_gas_syndromes.parquet`'s `pct_syndrome_strep_toxic_shock` gives the
  *percent* of invasive GAS cases in the ABCs catchment presenting as STSS. They
  are not comparable directly.
- **Epic denominators are all encounters, not ED visits.** `n_patients` counts
  patients with any encounter, so `pct_strep_throat` is not an ED visit share.
- **Time spans differ**: Epic 2017-Q1 → 2025-Q4, NNDSS 2022 → present, ABCs
  1997 → 2023. Any cross-source comparison is limited to the overlap.

This is a Data Collection Framework data bundle project, initialized with
`dcf::dcf_add_bundle`.

You can use the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_gas")
```
