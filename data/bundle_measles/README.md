# Measles Bundle

This bundle combines measles surveillance data from various sources for the PopHIVE platform.

## Data Sources

Currently includes:
- **CDC NWSS Wastewater Measles Surveillance**: Measles virus detection in wastewater samples
- **Vaccine Exemptions (Kiang et al. 2025)**: Medical exemptions from childhood vaccination requirements

Future sources may include:
- Clinical case reporting data
- Additional surveillance streams

## Output Files

### measles_overall_trends.parquet
Time series of measles detection rates with smoothed values and scaled metrics.

**Columns:**
- `geography`: State name or "United States"
- `fips`: 2-digit FIPS code
- `date`: Date of observation
- `detection_rate`: Percentage of samples with measles detection
- `detection_count`: Number of detections
- `sample_count`: Number of samples tested
- `sewershed_count`: Number of sewersheds reporting
- `population_served`: Population covered by surveillance
- `detection_rate_smooth`: 3-period moving average of detection rate
- `detection_rate_smooth_scale`: Scaled detection rate (0-100)

### measles_geographic_summary.parquet
Latest measles detection data by state.

**Columns:**
- `geography`: State name or "United States"
- `fips`: 2-digit FIPS code
- `date`: Date of latest observation
- `detection_rate`: Latest detection rate
- `detection_count`: Latest detection count
- `sample_count`: Latest sample count
- `sewershed_count`: Number of sewersheds reporting
- `population_served`: Population covered

### measles_detection_status.parquet
Time series of detection status categories.

**Columns:**
- `geography`: State name or "United States"
- `fips`: 2-digit FIPS code
- `date`: Date of observation
- `detection_status`: "No Data", "No Detection", or "Detection"
- `detection_count`: Number of detections
- `sample_count`: Number of samples tested
- `sewershed_count`: Number of sewersheds reporting

### measles_exemptions_trends.parquet
Time series of medical vaccine exemption rates.

**Columns:**
- `geography`: State name or "United States"
- `fips`: 2-digit FIPS code
- `date`: Date (September 1 of year - school entry)
- `year`: Calendar year
- `exemption_rate_mmr`: Percentage of kindergartners with medical exemptions

### measles_exemptions_with_wastewater.parquet
Combined view of exemption rates with latest wastewater detection status.

**Columns:**
- `geography`: State name or "United States"
- `fips`: 2-digit FIPS code
- `date`: Date of exemption data (September 1 of year - school entry)
- `year`: Calendar year
- `exemption_rate_mmr`: Percentage of kindergartners with medical exemptions
- `latest_detection_date`: Most recent wastewater surveillance date
- `detection_flag`: Binary indicator (1=detected, 0=not detected)
- `detection_rate`: Latest wastewater detection rate
- `detection_count`: Latest detection count

## Building the Bundle

From the project root:
```r
dcf::dcf_process("bundle_measles", ".")
```

Or from this directory:
```r
source("build.R")
```
