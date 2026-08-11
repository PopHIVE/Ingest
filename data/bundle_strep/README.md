# bundle_strep

Group A and Group B Streptococcus surveillance, combining three sources into
**three** long-format parquets under `dist/`.

| Parquet | Source | Rows | Grain |
|---|---|---|---|
| `abcs_strep.parquet` | `abcs` | 6,101 | national, annual — all eight ABCs topics stacked |
| `epic_gas.parquet` | `epic_resp_infections` | 39,312 | state + national, quarterly, by age |
| `nnds_stss.parquet` | `nnds` | 24,856 | state + national, weekly (MMWR) |

Shared columns: `geography` (state name, or `"United States"`), `geography_fips`,
`date`, `year`, `measure`, `value`. Bundles name the time column **`date`**; the
underlying standard files call it `time`.

## `abcs_strep.parquet` — one file, stacked

All eight ABCs topics (rates, counts, resistance, Group A syndromes and emm
types, Group B syndromes, serotypes and ALPH types) sit in one table with a
named column per stratification. **Any dimension a row is not stratified on
carries `"Total"`**, so every column is populated and can be filtered without
handling NA.

```
geography, geography_fips, date, year, pathogen,
age, sex, race_ethnicity, onset, rate_denominator,      <- stratifications
syndrome, antibiotic, emm_type, serotype, alph_type,    <- which entity
measure, value, value_not_reported,
n_type, n_type_status, n_isolates, n_isolates_status
```

`measure` names the quantity: `rate_cases`, `rate_deaths`, `n_cases`,
`n_deaths`, `n_survivals`, `pct_resistant`, `rate_syndrome`, `pct_syndrome`,
`pct_emm_type`, `pct_serotype`, `pct_alph_type`.

**Companion columns, not extra rows.** `n_isolates` is the denominator behind
every percentage and `n_type` the numerator for emm types, both on the same row,
so a tooltip reads *"emm1: 22.5% (99 of 440 isolates)"* from one line. Cases,
deaths and survivals stay separate `measure` levels — they are three plottable
series, not metadata for a single value.

**Every blank is explained.** The two companions are the only columns that can be
blank, and each carries a `_status` saying why, because NA alone conflates two
different things:

| status | meaning | rows |
|---|---|---|
| `reported` | the companion holds CDC's published figure | `n_isolates` 2,504 / `n_type` 550 |
| `not_reported` | the measure has this companion, but CDC published nothing for that row | `n_isolates` 910 / `n_type` 38 |
| `not_applicable` | the measure has no such companion at all | `n_isolates` 2,687 / `n_type` 5,513 |

So `n_type` blank with `value_not_reported = 0` is not a data gap — it means the
row isn't an emm row. Every other column, `value` included, is always populated.

## Notes for anyone reading these files

- **`value` mixes units — always filter or facet by `measure` first.** Within one
  file `measure` can name a rate, a count or a percentage.
- **Check `rate_denominator` before comparing rates.** CDC labels every rate
  `"Per 100,000 population"` while using two bases. `"Stratum population"` is per
  100,000 of the group the row describes; `"Population"` is per 100,000 of the
  whole population regardless of stratum (how CDC reports the infant onset
  rates). For 1997 Group B the `<1` band reads **115.7** against early + late =
  **1.10** — differing in all 28 years, mean absolute difference 66.3. Rows with
  different denominators **must never be summed or plotted on one axis**.
  `"Total"` appears where the row is not a rate.
- **`onset` is not additive.** Onset rows are a subset of infants, not additional
  population. For case counts an explicit `age = "<1"` / `onset = "Total"` row is
  provided (the sum of early + late, which CDC does not publish); for rates no
  such total is derived, because of the denominator difference above.
- **`Total` is the aggregate level** throughout, matching the rest of the
  database. It overlaps the specific levels, so exclude it before summing across
  a stratification.
- **`value_not_reported = 1` means the 0 is not real.** `value` is never blank:
  unreported cells are filled with 0 and flagged. Reading a flagged row as a
  measured zero understates resistance, syndrome rates and type shares, so filter
  on the flag before aggregating. Reasons are structural — a drug off a pathogen's
  panel, an emm type CDC pooled into "other" that year, a Group B serotype CDC
  regrouped that year, a rate not broken out for that stratification.
- **`suppressed = 1`** (Epic only) is a different thing: a small cell Epic
  withheld, imputed as 5.
- **`n_isolates` and `n_type` stay blank rather than zero** where CDC published
  no denominator, since "22.5% of 0 isolates" would read as broken. See the
  status table above.
- **`n_isolates` is the *apparent* denominator, not a documented one.** CDC ships
  it beside each topic's percentages but never defines it, and the column
  descriptions in the source metadata are empty. It is also not portable across
  topics — 2023 emm-typed isolates read 3,930 against 3,908 resistance isolates.
  Coverage is partial: 2006+ for resistance, and Group B publishes one overall
  count rather than one per population stratum.
- **NNDSS is published cumulatively.** The raw
  `streptococcal_toxic_shock_syndrome` column is a year-to-date running total
  that resets each MMWR year (national 2024 runs 5 → 647 across weeks 1–52).
  `build.R` de-accumulates it into `stss_cases_weekly`, which is the series to
  plot; `stss_cases_cumulative` keeps the published form. The two are not
  additive.
- **Negative weekly increments are kept.** NNDSS sometimes revises earlier weeks
  downward, producing a small number of negative values. Following the
  `bundle_measles` convention these are retained for transparency rather than
  clamped — **plots should cut the y axis at 0 instead.**
- **STSS is the only Group A measure NNDSS carries.** It is nationally
  notifiable; invasive Group A disease generally, strep throat, and Group B
  disease are not. So NNDSS gives the severe tip of Group A, not Group A broadly
  — 647 STSS cases in 2024 against ABCs' ~41,400 estimated invasive Group A
  cases.
- **Group A and Group B are different diseases.** Group A causes strep throat and
  invasive disease across all ages; Group B is primarily neonatal sepsis and
  invasive disease in older adults. Do not pool them.
- **The three sources sit at different points on the severity pyramid**, which is
  why they are separate files: Epic covers strep throat diagnoses (state,
  quarterly), NNDSS covers STSS only (state, weekly), and ABCs covers all invasive
  disease (national only, annual, and even then a ~35 million person catchment
  rather than the whole US).
- **Time spans differ**: Epic 2017-Q1 → 2025-Q4, NNDSS 2022 → present, ABCs
  1997 → 2024. Cross-source comparison is limited to the overlap.
- **Epic denominators are all encounters, not ED visits.** `n_patients` counts
  patients with any encounter, so `pct_strep_throat` is not an ED visit share.

This is a Data Collection Framework data bundle project, initialized with
`dcf::dcf_add_bundle`.

```R
dcf::dcf_process("bundle_strep")
```
