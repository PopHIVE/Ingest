# STI Bundle

Sexually transmitted infection surveillance, screening, HIV outcomes, and
high school sexual behaviors, combined for the PopHIVE platform. Files are
split by time resolution so each has a single grain.

## Data Sources

- **County Health Rankings**: annual chlamydia incidence, HIV prevalence, and
  teen birth rates, state and county.
- **CMS Medicaid Core Set** (`medicaid_quality`): annual chlamydia screening
  rates among women 16-20 and 21-24 enrolled in Medicaid, state only, with the
  national 25th and 75th percentile benchmarks.
- **CMS Mapping Medicare Disparities** (`cms_mmd`): annual share of Medicare
  Fee-for-Service beneficiaries screened for STIs, national, state, and county.
- **NCHS VSRR** (`nchs_mortality`): quarterly age-adjusted HIV disease death
  rate, state and national.
- **CDC NNDSS** (`nnds`): weekly case counts for chlamydia, gonorrhea,
  syphilis, chancroid, mpox, and hepatitis B and C, national and state.
- **CDC YRBSS** (`yrbss`): biennial sexual behavior questions from the Youth
  Risk Behavior Survey, national and state.

## Output Files

All files are long format with a `measure` column naming the source variable
(names match the source `standard/` columns) and a `source` column naming the
dataset. See `measure_info.json` for definitions and units.

### sti_state.parquet, sti_county.parquet

Annual. `geography` is a 2-digit state FIPS (`00` = national) or 5-digit
county FIPS; `time` is the year end (`YYYY-12-31`). County Health Rankings,
Medicaid Core Set, and Medicare FFS measures. Medicaid Core Set is state only.

### sti_quarterly.parquet

Quarterly HIV disease death rate per 100,000, state and national. `time` is
the last day of the quarter.

### sti_weekly.parquet

Weekly NNDSS case counts by MMWR week-ending date, national and
state/territory. NNDSS publishes cumulative year-to-date counts; this file
differences consecutive weeks within each MMWR year, so negative values are
downward revisions. Hepatitis B and C reporting categories changed at the start
of 2024 (for example `hepatitis_b_acute` through 2023, then
`hepatitis_b_acute_confirmed` and `_probable`); each column covers only the
years it was reported.

### sti_youth.parquet

YRBSS estimates by survey year with `age` (14-17 or Overall), `sex`, and
`race_ethnicity`. Strata are marginal, not crossed. Includes `value_lcl`,
`value_ucl`, and `suppressed_flag` (1 = CDC suppressed the estimate; `value`
and CI are NA). Questions not asked in a jurisdiction-year are omitted. A
small number of estimates come from the API as 0.0 with a 0.0 to 0.0 interval
(usually small race/ethnicity strata); they are kept as published.

## Building the Bundle

From the project root:

```r
dcf::dcf_process("bundle_sti", ".")
```
