---
editor_options: 
  markdown: 
    wrap: 72
---

# bundle_preventative_services

This bundle combines preventative services data from Medicare and
Medicaid sources for the PopHIVE platform.

## Data Sources

-   **Medicaid and CHIP Adult Core Set Quality Measures**: State-level
    preventative services rates for Medicaid and CHIP beneficiaries,
    voluntarily reported by states to CMS (2014–2023)
-   **CMS Mapping Medicare Disparities (MMD) by Population Tool**:
    Preventative services utilization rates for Medicare Fee-for-Service
    beneficiaries, stratified by state, age, sex, and race/ethnicity

## Output Files

### medicaid_preventative_services.parquet

State-level preventative services rates for Medicaid and CHIP
beneficiaries.

**Columns:** - `geography`: State name or "District of Columbia" -
`year`: Calendar year - `age`: Age group category - `sex`: Sex
category - `race_ethnicity`: Race/ethnicity category - `outcome_name`:
Preventative service type (Influenza Vaccine, Chlamydia Screening,
Diabetes Screening, Depression Screening, Cardiovascular Disease
Screening) - `source`: Data source ("Medicaid") - `value`: Service rate
(percent)

### cms_preventative_services_state.parquet

State-level preventative services utilization rates for Medicare FFS
beneficiaries by age group.

**Columns:** - `geography`: State name or "United States" - `fips`:
2-digit FIPS code - `year`: Calendar year - `age`: Age group category -
`outcome_name`: Preventative service type (Influenza Vaccine,
Cardiovascular Disease Screening, Diabetes Screening, Depression
Screening, Chlamydia Screening, Pneumococcal Vaccine, Annual Wellness
Visit, Pelvic Exam) - `source`: Data source ("Medicare FFS") - `value`:
Service rate (percent)

### cms_preventative_services_by_sex.parquet

State-level preventative services utilization rates for Medicare FFS
beneficiaries stratified by sex.

**Columns:** - `geography`: State name or "United States" - `fips`:
2-digit FIPS code - `year`: Calendar year - `age`: Age group category -
`sex`: Sex category - `outcome_name`: Preventative service type -
`source`: Data source ("Medicare FFS") - `value`: Service rate (percent)

### cms_preventative_services_by_race.parquet

State-level preventative services utilization rates for Medicare FFS
beneficiaries stratified by race and ethnicity.

**Columns:** - `geography`: State name or "United States" - `fips`:
2-digit FIPS code - `year`: Calendar year - `age`: Age group category -
`race_ethnicity`: Race/ethnicity category - `outcome_name`: Preventative
service type - `source`: Data source ("Medicare FFS") - `value`: Service
rate (percent)

### combined_preventative_services.parquet

Combined Medicare and Medicaid preventative services rates for
side-by-side reference.

**Columns:** - `geography`: State name or "United States" - `fips`:
2-digit FIPS code - `year`: Calendar year - `outcome_name`: Preventative
service type - `value_medicare`: Medicare FFS service rate (percent,
Total age group) - `value_medicaid`: Medicaid service rate (percent,
averaged across reporting programs)

## Building the Bundle

This is a Data Collection Framework data bundle project, initialized
with `dcf::dcf_add_bundle`.

You can use the `dcf` package to rebuild the bundle:

``` r
dcf::dcf_process("bundle_preventative_services", "..")
```
