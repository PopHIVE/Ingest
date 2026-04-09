---
editor_options: 
  markdown: 
    wrap: 72
---

# bundle_youth_wellbeing

This bundle combines youth wellbeing data from Medicare and Medicaid
sources for the PopHIVE platform.

## Data Sources

-   **Medicaid and CHIP Child Core Set Quality Measures**: State-level
    youth wellbeing rates for Medicaid and CHIP child beneficiaries,
    voluntarily reported by states to CMS (2014–2023)
-   **CMS Mapping Medicare Disparities (MMD) by Population Tool**: Youth
    wellbeing condition prevalence for Medicare Fee-for-Service
    beneficiaries, stratified by state, age, sex, and race/ethnicity

## Output Files

### medicaid_youth_wellbeing.parquet

State-level youth wellbeing rates for Medicaid and CHIP child
beneficiaries.

**Columns:** - `geography`: State name or "District of Columbia" -
`year`: Calendar year - `age`: Age group category - `sex`: Sex
category - `race_ethnicity`: Race/ethnicity category - `outcome_name`:
Youth wellbeing measure (ADHD Medication Management, Follow-Up After ED
Visit for Mental Illness, Follow-Up After Hospitalization for Mental
Illness, Developmental Screening, Weight Assessment for Children,
Adolescent Well-Care Visits, Well-Child Visits (First 15 Months),
Well-Child Visits (First 30 Months), Children's Access to Primary
Care) - `source`: Data source ("Medicaid") - `value`: Service rate
(percent)

### cms_youth_wellbeing_state.parquet

State-level youth wellbeing condition prevalence for Medicare FFS
beneficiaries by age group.

**Columns:** - `geography`: State name or "United States" - `fips`:
2-digit FIPS code - `year`: Calendar year - `age`: Age group category -
`outcome_name`: Condition (ADHD, Anxiety, Depression, Depressive
Disorder) - `source`: Data source ("Medicare FFS") - `value`: Prevalence
(percent)

### cms_youth_wellbeing_by_sex.parquet

State-level youth wellbeing condition prevalence for Medicare FFS
beneficiaries stratified by sex.

**Columns:** - `geography`: State name or "United States" - `fips`:
2-digit FIPS code - `year`: Calendar year - `age`: Age group category -
`sex`: Sex category - `outcome_name`: Condition (ADHD, Anxiety,
Depression, Depressive Disorder) - `source`: Data source ("Medicare
FFS") - `value`: P
