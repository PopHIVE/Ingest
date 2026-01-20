# Washington Post School Vaccination Data

## Overview

This directory contains ingestion scripts for The Washington Post's school vaccination rates database, which includes both county-level and school-level vaccination and exemption data.

## Data Source

**Repository**: https://github.com/washingtonpost/data-school-vaccination-rates

**Attribution**: The Washington Post compiled this data from state health departments across the United States through public records requests and state immunization dashboards.

**Coverage**:
- County-level data: 41 states/territories
- School-level data: 36 states/territories
- School years: 2018-2019, 2019-2020, 2023-2024, 2024-2025

## Files

### Raw Data
- `raw/wapo_vaxrates_counties.csv` - County-level vaccination rates
- `raw/wapo_vaxrates_schools.csv` - School-level vaccination and exemption rates
- `raw/ijqb-a7ye.csv.xz` - CDC SchoolVaxView data (Socrata API)

### Standardized Output
- `standard/data.csv.gz` - CDC SchoolVaxView state-level data
- `standard/data_exemptions.csv.gz` - CDC SchoolVaxView exemptions
- `standard/data_wapo_counties.csv.gz` - Washington Post county-level data
- `standard/data_wapo_schools.csv.gz` - Washington Post school-level data

### Scripts
- `ingest.R` - **Single ingestion script** processing both CDC and Washington Post data
- `measure_info.json` - Variable metadata with attribution for both sources

## Running the Ingestion

The Washington Post data is processed as part of the main ingestion pipeline:

```r
# From project root using dcf (recommended)
dcf::dcf_process("schoolvaxview", "..")

# Or navigate to the directory and source directly
setwd("data/schoolvaxview")
source("ingest.R")
setwd("../..")
```

**Note**: The `ingest.R` script processes **both** CDC SchoolVaxView and Washington Post data in a single execution. This follows the dcf convention of one `ingest.R` per data source directory.

## Output Format

### County-level Data (`data_wapo_counties.csv.gz`)

| Column | Description | Type |
|--------|-------------|------|
| `geography` | 5-digit county FIPS code | string |
| `time` | School year start date (MM-DD-YYYY format, September 1st) | string |
| `wapo_county_vax_rate` | MMR or overall vaccination rate (0-100) | numeric |
| `wapo_prepand_herd` | Pre-pandemic herd immunity status (y/n) | string |
| `wapo_postpand_herd` | Post-pandemic herd immunity status (y/n) | string |

**Note**: Vaccination rate represents either MMR-specific or overall compliance depending on state reporting.

### School-level Data (`data_wapo_schools.csv.gz`)

| Column | Description | Type |
|--------|-------------|------|
| `geography` | 2-digit state FIPS code | string |
| `time` | School year start date (MM-DD-YYYY format, September 1st) | string |
| `wapo_school_name` | School name | string |
| `wapo_school_type` | School type (public/private/other/district) | string |
| `wapo_school_address` | School address | string |
| `wapo_students_enrolled` | Number of students surveyed | numeric |
| `wapo_school_mmr_rate` | MMR vaccination rate (2 doses) | numeric |
| `wapo_school_overall_rate` | Overall vaccination compliance rate | numeric |
| `wapo_school_medical_exemption_rate` | Medical exemption rate | numeric |
| `wapo_school_religious_exemption_rate` | Religious exemption rate | numeric |
| `wapo_school_personal_exemption_rate` | Personal exemption rate | numeric |
| `wapo_school_nonmedical_exemption_rate` | Non-medical exemption rate | numeric |
| `wapo_school_overall_exemption_rate` | Overall exemption rate | numeric |
| `wapo_school_lat` | Latitude | numeric |
| `wapo_school_lon` | Longitude | numeric |
| `wapo_school_county` | County name | string |
| `wapo_school_state` | State abbreviation | string |
| `wapo_school_grade` | Grade level | string |

## Variable Naming Convention

All variables from the Washington Post dataset use the `wapo_` prefix to:
1. Avoid conflicts with existing CDC SchoolVaxView variables
2. Clearly identify the data source
3. Maintain proper attribution

## Data Processing Notes

1. **Time Format**: School years are converted to September 1st dates (e.g., "2023-2024" becomes "09-01-2023")

2. **Geography**:
   - County data uses 5-digit county FIPS codes
   - School data uses 2-digit state FIPS codes (county-level FIPS not available for schools)

3. **Herd Immunity**: Pre/post-pandemic indicators show whether counties met the 95% vaccination threshold

4. **Data Changes**: The script uses MD5 hashing to detect when source files have been updated

## Relationship to Existing Data

The Washington Post data complements the existing CDC SchoolVaxView data in this folder:
- **CDC SchoolVaxView** (`ingest.R`, `data.csv.gz`, `data_exemptions.csv.gz`): State-level kindergarten vaccination coverage from CDC
- **Washington Post** (`ingest_wapo.R`, `data_wapo_counties.csv.gz`, `data_wapo_schools.csv.gz`): County and school-level data compiled from state sources

Both datasets can be used together to provide comprehensive vaccination coverage analysis at multiple geographic scales.
