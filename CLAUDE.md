# PopHIVE/Ingest - Claude Code Configuration

## Project Overview

This repository standardizes public health surveillance data for the PopHIVE platform (pophive.org). Data sources are transformed into a consistent format and combined into bundles for visualization. The project uses the `dcf` R package from Yale's Data-Intensive Social Science Center (DISSC) for workflow management.

**Repository**: https://github.com/PopHIVE/Ingest  
**Documentation**: https://pophive.github.io/processing-documentation/  
**Data Status**: https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest

---

## Standard Data Format Specification

All standardized output files must conform to these column specifications:

### Required Columns

| Column | Description | Format | Examples |
|--------|-------------|--------|----------|
| `geography` | Geographic identifier | FIPS code string | `"00"` (national), `"06"` (California), `"06037"` (LA County) |
| `time` | Time period end date | `MM-DD-YYYY` | `"01-04-2025"` (Saturday for weekly data) |

### Common Optional Columns

| Column | Description | Values |
|--------|-------------|--------|
| `age` | Age group | `"0-4"`, `"5-17"`, `"18-49"`, `"50-64"`, `"65+"`, `"Overall"` |
| `race_ethnicity` | Race/ethnicity category | `"White"`, `"Black"`, `"Hispanic"`, `"Asian"`, `"Overall"` |
| `sex` | Sex category | `"Male"`, `"Female"`, `"Overall"` |
| `virus` | Pathogen (respiratory data) | `"rsv"`, `"influenza"`, `"covid"` |

### Value Columns

| Column | Description |
|--------|-------------|
| `value` | Primary measure (closest to source data) |
| `value_smooth` | 3-week moving average |
| `value_smooth_scale` | Smoothed value scaled 0-100 |
| `suppressed_flag` | `1` if value was suppressed and imputed, `0` otherwise |

### Geography Standards

- **National**: Use `"00"` (not `"US"` or `"0"`)
- **State**: 2-digit FIPS code as string (`"06"` not `6`)
- **County**: 5-digit FIPS code as string (`"06037"`)
- Convert state names/abbreviations using `cdlTools::fips(state, to='FIPS')`

### Time Standards

- **Format**: `MM-DD-YYYY` (with leading zeros)
- **Weekly data**: Use Saturday at end of week (epiweek convention)
- **Monthly data**: Use last day of month
- **Annual data**: Use `12-31-YYYY`

---

## Directory Structure

```
PopHIVE/Ingest/
├── data/
│   ├── {source_name}/           # Individual data source
│   │   ├── raw/                 # Downloaded source files (compressed)
│   │   ├── standard/            # Standardized output files
│   │   │   ├── data.csv.gz      # Main standardized file
│   │   │   ├── data_state.csv.gz    # State-level (if separate)
│   │   │   └── data_county.csv.gz   # County-level (if separate)
│   │   ├── ingest.R             # Transformation script
│   │   ├── measure_info.json    # Variable metadata
│   │   └── process.json         # Processing state (auto-generated)
│   │
│   ├── bundle_{category}/       # Combined datasets
│   │   ├── build.R              # Bundle assembly script
│   │   ├── process.json         # Lists source files used
│   │   └── dist/                # Final outputs for visualization
│   │       ├── *.parquet        # Primary format for web
│   │       └── *.csv.gz         # Alternative format
│   │
│   ├── epic/                    # Epic Cosmos data
│   ├── gtrends/                 # Google Health Trends
│   ├── wastewater/              # CDC NWSS
│   ├── nssp/                    # CDC NSSP ED visits
│   ├── respnet/                 # CDC RESP-NET hospitalizations
│   ├── abcs/                    # CDC ABCs pneumococcal
│   ├── NREVSS/                  # CDC lab testing
│   └── nis/                     # National Immunization Survey
│
├── scripts/                     # Utility scripts
├── resources/                   # Reference files (FIPS codes, etc.)
├── settings.json               # Project configuration
├── file_log.json               # File tracking
└── renv.lock                   # R package versions
```

---

## Key dcf Package Functions

```r
# Create new data source folder structure
dcf::dcf_add_source("source_name")

# Initialize processing record for tracking changes
process <- dcf::dcf_process_record()

# Download data from CDC data.gov (Socrata API)
raw_state <- dcf::dcf_download_cdc(
  "dataset-id",      # e.g., "kvib-3txy"
  "raw",             # output directory
  process$raw_state  # previous state for change detection
)

# Update processing record after changes
process$raw_state <- raw_state
dcf::dcf_process_record(updated = process)

# Create or update a bundle
dcf::dcf_process("bundle_respiratory", ".")

# Build all sources (run from project root)
dcf::dcf_build()
```

---

## ingest.R Template

```r
# =============================================================================
# {SOURCE_NAME} Data Ingestion
# Source: {URL or description}
# =============================================================================

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
raw_state <- dcf::dcf_download_cdc(
  "{dataset-id}",
  "raw",
  process$raw_state
)

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {
  
  # ---------------------------------------------------------------------------
  # 2. Read and transform data
  # ---------------------------------------------------------------------------
  data_raw <- vroom::vroom("raw/{dataset-id}.csv.xz")
  
  data_standard <- data_raw %>%
    # Filter to relevant subset
    filter(
      Type == "Unadjusted Rate",
      Sex == "Overall",
      `Race/Ethnicity` == "Overall"
    ) %>%
    # Rename to standard columns
    rename(
      time = `Week Ending Date`,
      age = `Age group`,
      state = Site
    ) %>%
    # Transform geography
    mutate(
      geography = case_when(
        state == "Overall" ~ "00",
        TRUE ~ cdlTools::fips(state, to = "FIPS")
      )
    ) %>%
    # Format time
    mutate(
      time = format(as.Date(time), "%m-%d-%Y")
    ) %>%
    # Select and order columns
    select(geography, time, age, value = `Weekly Rate`) %>%
    # Remove intermediate columns
    select(-state)
  
  # ---------------------------------------------------------------------------
  # 3. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    delim = ","
  )
  
  # ---------------------------------------------------------------------------
  # 4. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
```

---

## measure_info.json Template

```json
{
  "variable_name": {
    "id": "variable_name",
    "short_name": "Brief description (< 100 chars)",
    "long_name": "Full descriptive name",
    "category": "respiratory|immunization|chronic|injury",
    "short_description": "One sentence description",
    "long_description": "Detailed description with methodology notes",
    "statement": "Template for narrative: 'In {location}, {value} cases were reported'",
    "measure_type": "Incidence|Prevalence|Rate|Percent|Count",
    "unit": "Cases per 100,000|Percent|Count",
    "time_resolution": "Week|Month|Year",
    "restrictions": "Non-commercial purposes|Attribution required|None",
    "sources": [
      {
        "name": "Source organization",
        "url": "https://data.source.url"
      }
    ],
    "citations": [
      {
        "title": "Publication title",
        "url": "https://doi.org/..."
      }
    ]
  }
}
```

---

## Common Data Source Patterns

### CDC data.gov (Socrata API)

```r
# Dataset ID is in the URL: data.cdc.gov/d/{dataset-id}
raw_state <- dcf::dcf_download_cdc("kvib-3txy", "raw", process$raw_state)
data <- vroom::vroom("raw/kvib-3txy.csv.xz")
```

### Epic Cosmos SlicerDicer Exports

Epic data requires special handling for suppression:
```r
data <- data %>%
  mutate(
    suppressed_flag = if_else(count < 10, 1, 0),
    # Impute suppressed values as halfway between 0 and minimum
    value = if_else(
      suppressed_flag == 1,
      min(value[suppressed_flag == 0], na.rm = TRUE) / 2,
      value
    )
  )
```

### Google Health Trends API

Requires adjustment for vaccination-related searches:
```r
# Adjusted RSV searches (removing vaccine signal)
data <- data %>%
  mutate(
    value_adjusted = rsv_volume - season * 2.72 * vax_volume - 
                     (1 - season) * 3.41 * vax_volume
  )
```

### National Averages from State Data

When national totals aren't provided, calculate population-weighted average:
```r
# Load state populations
state_pop <- read.csv("resources/state_populations.csv")

data_national <- data %>%
  left_join(state_pop, by = "geography") %>%
  group_by(time, age) %>%
  summarize(
    value = weighted.mean(value, population, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(geography = "00")

data_final <- bind_rows(data, data_national)
```

---

## Bundle build.R Template

```r
# =============================================================================
# Bundle: {bundle_name}
# Combines: {list of sources}
# =============================================================================

library(dplyr)
library(arrow)

# -----------------------------------------------------------------------------
# 1. Load standardized source files
# -----------------------------------------------------------------------------
epic <- vroom::vroom("../epic/standard/weekly.csv.gz")
nssp <- vroom::vroom("../nssp/standard/data.csv.gz")
wastewater <- vroom::vroom("../wastewater/standard/data.csv.gz")

# -----------------------------------------------------------------------------
# 2. Harmonize and combine
# -----------------------------------------------------------------------------
combined <- bind_rows(
  epic %>% mutate(source = "epic_cosmos"),
  nssp %>% mutate(source = "nssp_ed"),
  wastewater %>% mutate(source = "wastewater")
)

# -----------------------------------------------------------------------------
# 3. Create derived measures
# -----------------------------------------------------------------------------
combined <- combined %>%
  group_by(geography) %>%
  arrange(time) %>%
  mutate(
    # 3-week moving average
    value_smooth = zoo::rollmean(value, k = 3, fill = NA, align = "right"),
    # Scale to 0-100
    value_smooth_scale = scales::rescale(value_smooth, to = c(0, 100))
  ) %>%
  ungroup()

# -----------------------------------------------------------------------------
# 4. Write outputs
# -----------------------------------------------------------------------------
arrow::write_parquet(
  combined,
  "dist/overall_trends.parquet",
  compression = "snappy"  # Use "gzip" for smaller files
)

# Also write CSV for compatibility
vroom::vroom_write(combined, "dist/overall_trends.csv.gz", ",")
```

---

## Validation Checklist

When creating or reviewing ingestion scripts, verify:

- [ ] **Geography**: All values are valid FIPS codes; national = `"00"`
- [ ] **Time**: Format is `MM-DD-YYYY`; weekly data uses Saturday
- [ ] **Column names**: Use standard names (lowercase, underscores)
- [ ] **Missing data**: Handled appropriately (NA, not empty strings)
- [ ] **Suppression**: Flagged with `suppressed_flag` column if imputed
- [ ] **measure_info.json**: Entry exists for each variable
- [ ] **Compression**: Output files are gzip compressed (`.csv.gz`)
- [ ] **process.json**: Updated in bundle with source file paths

---

## Common Issues and Solutions

### Issue: State names instead of FIPS codes
```r
# Solution: Use cdlTools
mutate(geography = cdlTools::fips(state_name, to = "FIPS"))
```

### Issue: Date in wrong format
```r
# Solution: Parse and reformat
mutate(time = format(as.Date(time, "%Y-%m-%d"), "%m-%d-%Y"))
```

### Issue: National data missing
```r
# Solution: Calculate population-weighted average (see pattern above)
```

### Issue: Weekly dates not on Saturday
```r
# Solution: Adjust to end-of-week Saturday
mutate(time = ceiling_date(as.Date(time), "week", week_start = 7) - 1)
```

### Issue: Multiple records per geography/time
```r
# Solution: Check for duplicates, aggregate if needed
data %>% 
  group_by(geography, time, age) %>%
  summarize(value = sum(value), .groups = "drop")
```

---

## Quick Reference Commands

```r
# Check current data status
dcf::dcf_status()

# Rebuild single source
source("data/source_name/ingest.R")

# Rebuild single bundle
dcf::dcf_process("bundle_respiratory", ".")

# Full rebuild
dcf::dcf_build()

# Validate standard file format
source("scripts/validate_standard.R")
validate_standard_file("data/source_name/standard/data.csv.gz")
```

---

## Adding a New Data Source: Step-by-Step

1. **Create folder structure**
   ```r
   dcf::dcf_add_source("new_source")
   ```

2. **Edit `ingest.R`**: Follow template above, adapting for source format

3. **Create `measure_info.json`**: Add entries for all output variables

4. **Test transformation**
   ```r
   source("data/new_source/ingest.R")
   ```

5. **Validate output**
   ```r
   validate_standard_file("data/new_source/standard/data.csv.gz")
   ```

6. **Add to bundle**: Update relevant `bundle_*/build.R` and `process.json`

7. **Rebuild bundle**
   ```r
   dcf::dcf_process("bundle_category", ".")
   ```

8. **Commit changes**: Include raw data sample, ingest.R, measure_info.json, standard output

---

## Contact and Resources

- **Processing Documentation**: https://pophive.github.io/processing-documentation/
- **dcf Package**: https://dissc-yale.github.io/dcf/
- **Data Status Report**: https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest
- **Feedback Form**: https://docs.google.com/forms/d/e/1FAIpQLSchAasiq7ovCCNz9ussb7C2ntkZ-8Rjc7-tNSglkf5boS-A0w/viewform