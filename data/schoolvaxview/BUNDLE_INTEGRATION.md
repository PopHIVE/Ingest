# Washington Post Data Bundle Integration

## Summary

The Washington Post school vaccination data has been successfully integrated into both the **Childhood Immunizations Bundle** and the **Measles Bundle**.

---

## Files Modified

### Childhood Immunizations Bundle

**Location**: `data/bundle_childhood_immunizations/`

#### 1. [build.R](../bundle_childhood_immunizations/build.R)
Added two new output files at the end of the script:
- `dist/wapo_vax_counties.parquet` - County-level vaccination rates
- `dist/wapo_vax_schools.parquet` - School-level vaccination and exemption rates

#### 2. [process.json](../bundle_childhood_immunizations/process.json)
Updated `source_files` array to include:
- `schoolvaxview/standard/data_wapo_counties.csv.gz`
- `schoolvaxview/standard/data_wapo_schools.csv.gz`

---

### Measles Bundle

**Location**: `data/bundle_measles/`

#### 1. [build.R](../bundle_measles/build.R)

**Data Loading** (Section 1):
- Added loading of Washington Post county and school data

**New Views Created**:

**Section 16 - Washington Post County Vaccination Rates**
- Output: `dist/measles_wapo_counties.parquet` and `.csv.gz`
- Contains: County-level vaccination rates, herd immunity indicators, by school year
- Columns: `county_fips`, `state_fips`, `date`, `wapo_county_vax_rate`, `wapo_prepand_herd`, `wapo_postpand_herd`

**Section 17 - Washington Post School Vaccination Rates**
- Output: `dist/measles_wapo_schools.parquet` and `.csv.gz`
- Contains: School-level MMR and overall vaccination rates, exemption rates, geographic coordinates
- Includes all vaccination and exemption metrics from the Washington Post dataset

**Section 18 - Combined View: Counties with JHU Cases**
- Output: `dist/measles_wapo_counties_with_cases.parquet` and `.csv.gz`
- Combines: Washington Post county vaccination rates + latest JHU measles case data
- Purpose: Enables analysis of relationship between vaccination rates and measles outbreaks

#### 2. [process.json](../bundle_measles/process.json)
Updated `source_files` object to include Washington Post data and their output files.

---

## Output Files Summary

### Childhood Immunizations Bundle Outputs

| File | Description | Granularity |
|------|-------------|-------------|
| `wapo_vax_counties.parquet` | County vaccination rates (2018-2025) | County-level |
| `wapo_vax_schools.parquet` | School vaccination and exemption rates | School-level |

### Measles Bundle Outputs

| File | Description | Granularity |
|------|-------------|-------------|
| `measles_wapo_counties.parquet` | County vaccination rates with herd immunity indicators | County-level |
| `measles_wapo_schools.parquet` | School vaccination rates, exemptions, and coordinates | School-level |
| `measles_wapo_counties_with_cases.parquet` | County vaccination rates combined with latest JHU measles case data | County-level |

All outputs are available in both Parquet (optimized for web) and CSV.gz (compatibility) formats.

---

## Data Relationships

### In Childhood Immunizations Bundle

The Washington Post data complements existing sources:
- **CDC SchoolVaxView**: State-level kindergarten data from CDC
- **NIS**: National Immunization Survey estimates
- **Epic Cosmos**: EHR-based vaccination rates
- **Washington Post**: County and school-level data from state sources

### In Measles Bundle

The Washington Post data integrates with:
- **Wastewater Detection**: Environmental surveillance for measles
- **Vaccine Exemptions**: MMR exemption rates by state
- **JHU Cases**: Confirmed measles case counts
- **HealthMap MMR Coverage**: Geographic MMR coverage estimates
- **Washington Post**: School-level vaccination compliance and exemptions

---

## Usage Notes

### Running the Bundles

To rebuild the bundles after Washington Post data is updated:

```r
# From project root
dcf::dcf_process("bundle_childhood_immunizations", ".")
dcf::dcf_process("bundle_measles", ".")
```

Or rebuild all bundles:

```r
dcf::dcf_build()
```

### Data Dependencies

The bundles depend on:
1. SchoolVaxView ingestion script ([ingest.R](ingest.R)) being run first
   - This single script processes **both** CDC SchoolVaxView and Washington Post data
2. Standardized files existing in `schoolvaxview/standard/`:
   - `data.csv.gz` (CDC SchoolVaxView)
   - `data_exemptions.csv.gz` (CDC SchoolVaxView)
   - `data_wapo_counties.csv.gz` (Washington Post)
   - `data_wapo_schools.csv.gz` (Washington Post)

### Key Variables Available

**County-level**:
- `wapo_county_vax_rate`: MMR or overall vaccination rate
- `wapo_prepand_herd`: Pre-pandemic herd immunity status (y/n)
- `wapo_postpand_herd`: Post-pandemic herd immunity status (y/n)

**School-level**:
- `wapo_school_mmr_rate`: MMR vaccination rate
- `wapo_school_overall_rate`: Overall vaccination compliance
- `wapo_school_medical_exemption_rate`: Medical exemptions
- `wapo_school_religious_exemption_rate`: Religious exemptions
- `wapo_school_personal_exemption_rate`: Personal exemptions
- `wapo_school_nonmedical_exemption_rate`: Non-medical exemptions
- `wapo_school_overall_exemption_rate`: All exemptions
- Geographic coordinates, enrollment, grade level

---

## Attribution

All variables are prefixed with `wapo_` to:
1. Clearly identify the data source
2. Maintain proper attribution to The Washington Post
3. Avoid naming conflicts with other vaccination data sources

Full attribution details are available in [measure_info.json](measure_info.json) under the `WashingtonPost` source entry.

---

## Next Steps

After running the Washington Post ingestion script:
1. Rebuild both bundles to include the new data
2. The bundle outputs will be available in the respective `dist/` directories
3. Data can be consumed by PopHIVE visualization tools
