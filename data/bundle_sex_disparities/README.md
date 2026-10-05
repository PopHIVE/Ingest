# bundle_sex_disparities

Female and male values for every sex-stratified measure in the standardized
PopHIVE sources. Each source gets one tall-format file (`measure` + `value`,
like the other bundles), `<name>_by_sex.parquet`, with `geography`, `time`, the
source's strata columns, `sex` (Female or Male), `measure`, `value` and
`suppressed_flag`. Ratios and differences are not stored; compute them from the
two `sex` rows of a stratum.

Each measure's unit and definition are on the `measure` column's levels in
`measure_info.json`; they are the sources' own definitions.

## Outputs (`dist/`)

| `<name>_by_sex` | Source | Contents | Geography |
|----------|--------|----------|-----------|
| `abcs_strep` | `abcs` | Group A/B strep case rate | national |
| `brfss_prevalence` | `brfss` | Diabetes and obesity prevalence, health coverage | national, state |
| `cms_prevalence` | `cms_mmd` | 45 Medicare FFS chronic conditions | national, state, county |
| `epic_concussion_rate` | `epic_concussions` | Concussion percent of ED encounters | national, state |
| `epic_concussion_count` | `epic_concussions` | Concussion and ED encounter counts | national, state |
| `nccr_incidence` | `nccr` | 15 childhood cancer incidence types | national |
| `neiss_rate` | `neiss` | Injury rates by diagnosis and product | national |
| `neiss_count` | `neiss` | Injury visit counts by diagnosis and product | national |
| `nhtsa_fatalities` | `nhtsa_crash` | Crash deaths and fatal crashes | national, state, county |
| `nis_teen_coverage` | `nis_teen` | Adolescent vaccine coverage | national, state |
| `wisqars_death_rate` | `wisqars` | Injury death rates by cause | state |
| `wisqars_death_count` | `wisqars` | Injury death counts by cause | state |
| `yrbss_behavior` | `yrbss` | 59 high-school risk behaviors | national, state |

Where a file combines several source files (`neiss_*`, `nhtsa_fatalities`), a
`dataset` column says which one a row came from.

## Reading the values

- `value` is as reported in the source's standardized file, never altered.
  Rows with no value are left out, and a missing sex row is never filled with 0.
- `suppressed_flag = 1` means the source suppressed the value or did not ask or
  collect it. Some sources store such values as 0 (YRBSS, NCCR), so leave
  flagged rows out of any ratio or difference.
- To compare, pivot the two `sex` rows of a stratum (all columns except `sex`,
  `value` and `suppressed_flag`) and divide or subtract.
- **Counts are not population-adjusted.** In `epic_concussion_count`,
  `neiss_count`, `nhtsa_fatalities` and `wisqars_death_count`, a female/male
  ratio of raw counts mostly reflects the size of the two populations and is
  not a disparity in risk. Use the rate files for that.

## Not included

- `cdc_wonder_natality`: `sex` there is the infant's sex, and values are birth counts.
- Confidence bounds (`_lcl`/`_ucl`) and sample sizes.
- `yrbss` `pct_no_pe_classes`, `pct_no_condom_last_sex`,
  `pct_no_birth_control_pills`, `pct_never_tested_hiv` and `pct_not_tested_std`:
  no entry in the source's `measure_info.json`. `yrbss/ingest.R` gives wording
  for each, but the wording for `pct_no_pe_classes` is contradictory and the
  base populations are not confirmed. Add them to the `values` pattern in
  `build.R` once documented. (`pct_no_birth_control_pills` is answered from a
  single-choice question about what "you or your partner" used, so "no pills"
  also includes students who used condoms or no method.)
- `nis_teen` `survey_years` (a single year that duplicates `time`).

## Known issues in the source data

- `brfss` `prev_insured_survey` (percent with health coverage; higher is better,
  unlike the other BRFSS measures) has no entry in the source's
  `measure_info.json`. Its definition here comes from `brfss/ingest_survey.R`.
  In 44 fully insured groups the value is `100.00000000000004` (floating-point
  rounding in the source's calculation); it is not altered here.
- Some CMS geography codes are not in `resources/all_fips.csv.gz`: 31 codes of
  the form `xx990` in `cms_mmd` (36 in the raw file, labelled only "County").
  Their meaning is not confirmed with CMS, and they are passed through unchanged.
- `nhtsa_crash` `data_crash_type` keeps FARS placeholder county codes (997/998,
  which are not counties); they are dropped here. The source script should
  filter them.
- County-level NHTSA counts include small values (fatal-crash counts are
  public, and no suppression is applied here). CMS applies its own rule of
  hiding groups of fewer than 11 beneficiaries.
