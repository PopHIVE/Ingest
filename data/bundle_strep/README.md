# bundle_strep

Group A and Group B Streptococcus surveillance, combining three sources into
**two** long-format parquets under `dist/`.

| Parquet | Source | Rows | Grain |
|---|---|---|---|
| `abcs_strep.parquet` | `abcs` | 6,101 | national, annual — all eight ABCs topics stacked |
| `gas_state.parquet` | `epic_resp_infections`, `nnds` | 64,168 | state + national, quarterly and weekly |

`gas_state.parquet` holds both state-level Group A series, separated by `source`:
Epic Cosmos strep throat diagnoses (quarterly, by age) and NNDSS streptococcal
toxic shock syndrome (weekly). They share a schema, so `age` is `"Total"` on
the NNDSS rows and `suppressed` is blank on them, since it is an Epic mechanism.
`date` is the MMWR week ending Saturday, so no separate week number is carried.

ABCs stays separate. It is national-only and annual, and carries fourteen
dimension and companion columns the other two do not have — folding it in would
leave 62% of the merged table as padding.

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
measure, value,
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

**Every blank is explained, and nothing is filled in.** Three columns can be
blank — `value`, `n_isolates` and `n_type` — and each has a companion saying why.

`value` is blank where CDC published nothing for that cell. **It is never filled
with a zero**, so a 0 in `value` is always a measurement. That matters because both cases genuinely occur side by
side: Group A penicillin resistance is a real 0 in every year, while Group B
tetracycline is simply absent from CDC's panel. Of 6,101 rows, 3,927 carry a
non-zero value, 924 a measured zero, and 1,250 a blank.

The two companions each carry a `_status`, because NA alone conflates two
different things:

| status | meaning | rows |
|---|---|---|
| `reported` | the companion holds CDC's published figure | `n_isolates` 2,504 / `n_type` 550 |
| `not_reported` | the measure has this companion, but CDC published nothing for that row | `n_isolates` 910 / `n_type` 38 |
| `not_applicable` | the measure has no such companion at all | `n_isolates` 2,687 / `n_type` 5,513 |

So `n_type` blank with `n_type_status = "not_applicable"` is not a data gap — it
means the row isn't an emm row.

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
- **A blank `value` means CDC published nothing** for that cell, rather than a
  zero. Reasons are structural: a drug off a pathogen's panel, an emm type CDC
  did not itemise that year, a Group B serotype CDC regrouped that year, a rate
  not broken out for that stratification.
- **`suppressed = 1`** (Epic only) is a different thing: a small cell Epic
  withheld, imputed as 5.
- **`n_isolates` and `n_type` stay blank rather than zero** where CDC published
  no denominator, since "22.5% of 0 isolates" would read as broken. See the
  status table above.
- **`n_isolates` is the reference-lab isolate count.** Invasive isolates are sent
  to CDC's reference laboratory and characterised by whole genome sequencing,
  which yields both the emm type and the predicted MICs behind the resistance
  percentages ([ABCs surveillance reports](https://www.cdc.gov/abcs/reports/)).
  Two cautions. CDC's own 2023 figures disagree slightly, the report citing 3,908
  isolates while the per-type counts sum to 3,930, and the published emm
  percentages reconcile against 3,930 — so the typing and resistance denominators
  are kept as separate rows here rather than shared. And coverage is partial:
  2006 onward for resistance, with Group B publishing one overall count rather
  than one per population stratum.
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
