# bundle_gas

Group A and Group B Streptococcus surveillance, combining three sources into 10
long-format parquets under `dist/` — one per contributing source standard file.

| Parquet | Source | Rows | Grain |
|---|---|---|---|
| `epic_gas.parquet` | `epic_resp_infections` | 39,312 | state + national, quarterly, by age |
| `nnds_stss.parquet` | `nnds` | 24,856 | state + national, weekly (MMWR) |
| `abcs_strep_rates.parquet` | `abcs` | 1,904 | national, annual, by pathogen/age/sex/race/onset |
| `abcs_strep_counts.parquet` | `abcs` | 336 | national, annual, by pathogen/age/onset |
| `abcs_strep_resistance.parquet` | `abcs` | 1,344 | national, annual, by pathogen/age/onset |
| `abcs_gas_syndromes.parquet` | `abcs` | 120 | national, annual |
| `abcs_gas_emm.parquet` | `abcs` | 1,204 | national, annual |
| `abcs_gbs_syndromes.parquet` | `abcs` | 243 | national, annual, by age/onset |
| `abcs_gbs_serotypes.parquet` | `abcs` | 1,540 | national, annual, by age/onset |
| `abcs_gbs_alph.parquet` | `abcs` | 300 | national, annual, by age/onset |

Every parquet shares the same shape:

| Column | Notes |
|---|---|
| `geography` | state name, or `"United States"` for the national total |
| `geography_fips` | matching FIPS code (`"00"` national), kept for joining |
| `time` | ISO `YYYY-mm-dd` period end |
| `measure` | which measure the row reports |
| `value` | the plotting column |
| `not_reported` | 1 where the source never published that measure (ABCs files) |
| `suppressed` | 1 where Epic withheld the cell and it was imputed (Epic file only) |

plus each source's dimension columns (`pathogen`, `age`, `sex`,
`race_ethnicity`, `onset`).

## Notes for anyone reading these files

- **`value` mixes units — always filter or facet by `measure` first.** Within one
  parquet, `measure` can name a count, a percent and a rate. This is the single
  most important thing to get right.
- **`not_reported` vs `suppressed` are different things.** `not_reported = 1`
  means CDC never published that figure — a measure absent for a stratification,
  an antibiotic missing from a pathogen's panel, an emm type not itemised that
  year. `suppressed = 1` means Epic withheld a small cell and the value was
  imputed as 5. Neither is a zero.
- **NNDSS is published cumulatively.** The raw
  `streptococcal_toxic_shock_syndrome` column is a year-to-date running total
  that resets each MMWR year (national 2024 runs 5 → 647 across weeks 1–52).
  `build.R` de-accumulates it into `stss_cases_weekly`, which is the series to
  plot; `stss_cases_cumulative` keeps the published form. The two are not
  additive. NNDSS sometimes revises earlier weeks downward, so a small number of
  weekly increments are negative; these are left as reported rather than clamped,
  and `build.R` logs the count.
- **STSS is the *only* Group A measure NNDSS carries.** Streptococcal toxic shock
  syndrome is nationally notifiable; invasive Group A disease generally, strep
  throat, and Group B disease are not. So NNDSS gives the severe tip of Group A
  disease, not Group A broadly — 647 STSS cases in 2024 against ABCs' ~41,400
  estimated invasive Group A cases.
- **Aggregate levels overlap.** `age` carries `"Total"`; `sex`,
  `race_ethnicity` and `onset` carry `"Overall"`. Exclude these before summing
  across a stratification. `onset` is especially easy to get wrong — its rows are
  a subset of infants, not additional population.
- **Group A and Group B are different diseases.** Group A causes strep throat and
  invasive disease across all ages; Group B is primarily neonatal sepsis and
  invasive disease in older adults. Do not pool them.
- **The three sources sit at different points on the severity pyramid**, which is
  why they are not directly comparable: Epic covers strep throat diagnoses
  (state, quarterly), NNDSS covers STSS only (state, weekly), and ABCs covers all
  invasive disease (national only, annual, and even then a ~35 million person
  catchment rather than the whole US).
- **Time spans differ**: Epic 2017-Q1 → 2025-Q4, NNDSS 2022 → present, ABCs 1997 →
  2024. Cross-source comparison is limited to the overlap.
- **Epic denominators are all encounters, not ED visits.** `n_patients` counts
  patients with any encounter, so `pct_strep_throat` is not an ED visit share.

## Open questions for review

- **Is `age` + `onset` the right shape** for the ABCs strep files, or would a
  single age-like category be preferred? CDC's own labels conflate the two
  (`Infants, early-onset disease`), and decomposing them is what keeps the index
  unique — collapsing onset into age would give two `<1` rows per year.
- **Parquet shape.** These are fully long (one row per measure). Other bundles
  such as `bundle_childhood_immunizations`' `overall_rates_by_source.parquet` and
  `bundle_youth_wellbeing`' `yrbss_state_age_demographics.parquet` are less tall,
  so whether the Epic and NNDSS outputs should be long, semi-wide, or a parquet
  copy of the standard file is unresolved.
- **NNDSS long vs wide**, and what to do about the negative weekly increments.

This is a Data Collection Framework data bundle project, initialized with
`dcf::dcf_add_bundle`.

```R
dcf::dcf_process("bundle_gas")
```
