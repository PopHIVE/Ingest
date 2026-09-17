# Adolescent Vaccination Bundle

Vaccination coverage among adolescents from Medicaid quality measures, the
NIS-Teen survey, and state school-entry assessments for the adolescent
grades, combined for the PopHIVE platform. Files are split by grain.

## Data Sources

- **CMS Medicaid Core Set** (`medicaid_quality`): annual Immunizations for
  Adolescents (IMA) rates and the 2014-2016 standalone HPV measure among
  Medicaid beneficiaries, state only, with the national 25th and 75th
  percentile benchmarks.
- **CDC NIS-Teen** (`nis_teen`): annual survey estimates of vaccination
  coverage among adolescents 13-17 by vaccine and dose (HPV also by sex),
  state and national, plus pooled 2018-2022 estimates by insurance, poverty,
  race/ethnicity, and urbanicity.
- **State school immunization assessments**
  (`school_immunizations_adolescent`): Tdap, MenACWY, HPV, complete-series,
  and exemption rates for the grade at which states require adolescent
  vaccines, county and state, for the eight states that publish them (AK, CT,
  IN, LA, MA, ND, TX, WA).

## Output Files

All files are long format with a `measure` column naming the source variable
(names match the source `standard/` columns, or are built from them for
NIS-Teen) and a `source` column naming the dataset. See `measure_info.json`
for definitions and units.

### adolescent_vax_state.parquet

Annual. `geography` is a 2-digit state FIPS (`00` = national); `time` is the
year end (`YYYY-12-31`). Medicaid Core Set, NIS-Teen (ages 13-17), and the
state rows of the school assessments, whose `time` is the year the school
year starts (2024-12-31 is the 2024-25 school year) and whose measure names
end in the grade assessed (`school_adol_pct_tdap_7th`). NIS-Teen rows carry
`value_lcl`, `value_ucl`, and `sample_size`; the other sources leave them NA.

The Medicaid IMA measure has two components from FFY 2017: `medicaid_ima_ch_rate`
is Combination 1 (one MenACWY and one Tdap dose by the 13th birthday) in every
year, and `medicaid_ima_ch_hpv_rate` is HPV series completion by the 13th
birthday. `medicaid_hpv_ch_rate` is the earlier standalone HPV measure (three
doses among females, FFY 2014-2016).

### adolescent_vax_county.parquet

Annual by school year. `geography` is a 5-digit county FIPS; Connecticut uses
county codes (09001-09015) through the 2023-24 school year and planning
region codes (09110-09190) from 2024-25. `time` is the year the school year
starts. `grade` is `6th`, `7th`, or `Adolescent` (Alaska's registry series,
ages 13-17); Washington assessed 6th grade through 2019-20 and 7th grade
from 2020-21, and Indiana reports both grades. `suppressed_flag` is 1 where
the state withheld a small cell (Texas); the value is NA, not imputed. Rates
are comparable within a state over time, not across states, because series
definitions, grades, and denominators differ.

### adolescent_vax_demographics.parquet

NIS-Teen estimates by `stratum_type` and `stratum`: `age` (13-17 and 13-15,
annual) and the pooled 2018-2022 estimates by `insurance`, `poverty`,
`race_ethnicity`, and `urban`, each with an `Overall` reference level. Pooled
rows are dated `2022-12-31` with `survey_years = "2018-2022"`. Strata are
marginal, not crossed.

## Building the Bundle

From the project root:

```r
dcf::dcf_process("bundle_adolescent_vaccination", ".")
```
