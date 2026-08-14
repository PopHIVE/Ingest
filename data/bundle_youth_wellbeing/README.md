---
editor_options: 
  markdown: 
    wrap: 72
---

# bundle_youth_wellbeing

This bundle combines youth wellbeing data from Medicare, Medicaid, and
County Health Rankings sources for the PopHIVE platform.

## Data Sources

-   **Medicaid and CHIP Child Core Set Quality Measures**: State-level
    youth wellbeing rates for Medicaid and CHIP child beneficiaries,
    voluntarily reported by states to CMS (2014–2023)
-   **CMS Mapping Medicare Disparities (MMD) by Population Tool**: Youth
    wellbeing condition prevalence for Medicare Fee-for-Service
    beneficiaries, stratified by state, age, sex, and race/ethnicity
-   **County Health Rankings & Roadmaps**: State- and county-level
    social determinants of health relevant to youth and family
    wellbeing (economic security, education, mental health/social
    support, nutrition and exercise access, environmental health, and
    housing), 2010–present

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
FFS") - `value`: Prevalence (percent)

### chr_youth_wellbeing_state.parquet

State-level social determinants of health from County Health Rankings
& Roadmaps, in tall format.

**Columns:** - `geography`: 2-digit state FIPS code ("00" = national) -
`time`: Year (end-of-period date) - `measure`: SDOH indicator (22
measures categorized as environmental_health, nutrition_and_exercise,
preventative_health, or demographic — see `measure_info.json` for the
full list, definitions, and category assignments) - `source`: Data
source ("County Health Rankings") - `value`: Indicator value (units
vary by measure)

### chr_youth_wellbeing_county.parquet

County-level social determinants of health from County Health
Rankings & Roadmaps, in tall format.

**Columns:** - `geography`: 5-digit county FIPS code - `time`: Year
(end-of-period date) - `measure`: SDOH indicator (23 measures; adds
`adverse_climate_events`, which County Health Rankings reports at
county level only) - `source`: Data source ("County Health Rankings") -
`value`: Indicator value (units vary by measure)
