---
name: ingest-source
description: Ingest a new data source into the PopHIVE/Ingest repository — creates the folder structure, writes an ingest.R script that standardizes raw data into wide format, and generates measure_info.json. Use when the user wants to add a new CDC/Socrata/URL/file-based data source, mentions "ingest", "new data source", or provides a dataset ID to onboard.
---

# ingest-source

Ingest a new data source: create the folder structure, write the ingest.R script to standardize raw data, and create the measure_info.json.

## Usage

```
/ingest-source <source_name> [description of data source and where to get it]
```

## Description

End-to-end skill for adding and ingesting a new data source into the PopHIVE/Ingest repository. This skill:

1. Creates the folder structure via `dcf::dcf_add_source()`
2. Examines the raw data to understand its structure
3. Writes an `ingest.R` script that transforms raw data into the standard wide format
4. Creates a `measure_info.json` documenting all output variables

## Instructions

When the user invokes this skill:

### Phase 1: Create Folder Structure

Initialize the directory structure for the new source by running `dcf::dcf_add_source()`.

1. **Validate the source name**:
   - Must be lowercase with underscores (e.g., `cdc_flu_data`, `epic_diabetes`)
   - No spaces or special characters
   - Should be descriptive of the data source

2. **Detect the R installation** (Windows only). Find available R versions:
   ```bash
   powershell -Command "Get-ChildItem 'C:\Program Files\R' | Select-Object Name"
   ```
   Use the most recent version found (e.g., `R-4.3.0`).

3. **Run the dcf command** from the project root:

   **On Windows**, use PowerShell with the detected R version:
   ```bash
   cd "<project_root>" && powershell -Command "& 'C:\Program Files\R\<R_VERSION>\bin\Rscript.exe' -e \"dcf::dcf_add_source('<source_name>')\""
   ```
   Replace `<R_VERSION>` with the detected version (e.g., `R-4.3.0`).

   **On macOS/Linux**:
   ```bash
   cd "<project_root>" && Rscript -e 'dcf::dcf_add_source("<source_name>")'
   ```

4. **Verify the created structure**:
   ```
   data/<source_name>/
   ├── raw/                  # For downloaded source files
   ├── standard/             # For standardized output files
   ├── ingest.R              # Transformation script (filled in below)
   ├── measure_info.json     # Variable metadata (filled in below)
   └── process.json          # Processing state (auto-generated)
   ```

### Phase 2: Gather Information

Ask the user (if not already provided):
- **Data source URL or file location**: Where is the raw data? (CDC Socrata dataset ID, direct URL, API, or local file)
- **What does the data measure?**: Brief description of the outcomes/variables
- **Geographic level**: National, state, county, or multiple?
- **Time resolution**: Weekly, monthly, annual?
- **Demographic breakdowns**: Age, race/ethnicity, sex, other?

If the user has already placed raw files in the `raw/` directory, examine them directly. If the user provides a URL or dataset ID, note it for the download step in ingest.R.

### Phase 3: Examine Raw Data

Before writing any code, understand the raw data structure:

1. **If raw files exist**: Read the first 20-30 rows to understand columns, types, and values
2. **If a CDC dataset ID is provided**: Note it for `dcf::dcf_download_cdc()` — the raw file will be at `raw/{dataset-id}.csv.xz`
3. **If a URL is provided**: Note it for `download.file()` in ingest.R

Identify:
- All column names and their meanings
- Which columns map to `geography`, `time`, and demographic dimensions (`age`, `sex`, `race_ethnicity`)
- Which columns contain outcome/measure values
- Any filtering needed (e.g., selecting specific record types, removing aggregates)
- Geographic format (state names, abbreviations, FIPS codes, county names)
- Date format in the raw data

### Phase 4: Write ingest.R

Write the `ingest.R` script at `data/<source_name>/ingest.R` following these rules:

#### Script Structure

```r
# =============================================================================
# {SOURCE_NAME} Data Ingestion
# Source: {URL or description}
# =============================================================================

library(dplyr)

# Initialize process record
process <- dcf::dcf_process_record()

# --- 1. Download raw data ---
# (Use dcf::dcf_download_cdc(), download.file(), or other method)

# --- 2. Check for changes ---
if (!identical(process$raw_state, raw_state)) {

  # --- 3. Read raw data ---
  # --- 4. Transform to standard wide format ---
  # --- 5. Write standardized output ---
  # --- 6. Update process record ---

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
```

#### Output Format: Standard Wide Format

The standardized output MUST be in **wide format** with:

- **Index columns**: `geography`, `time`, and optionally `age`, `sex`, `race_ethnicity` (one row per unique combination)
- **Value columns**: Each unique outcome variable gets its own column

#### Column Naming Convention

All value columns MUST follow this naming pattern:

```
{prefix}_{descriptive_name}
```

Where:
- **`{prefix}`**: A short identifier for the data source (e.g., `wastewater`, `nssp`, `acs`, `respnet`, `epic`). This should match or abbreviate the source directory name. Use the same prefix for ALL value columns from this source.
- **`{descriptive_name}`**: A short, descriptive name for the specific measure (e.g., `covid`, `flu`, `rsv`, `hospitalization_rate`, `pct_vaccinated`)

Examples of good column names:
- `wastewater_covid`, `wastewater_flua`, `wastewater_rsv`
- `nssp_pct_visits_covid`, `nssp_pct_visits_flu`, `nssp_pct_visits_rsv`
- `respnet_rate_covid`, `respnet_rate_rsv`, `respnet_rate_flu`
- `brfss_pct_obesity`, `brfss_pct_diabetes`, `brfss_pct_depression`
- `acs_pop_total`, `acs_pop_male`, `acs_pct_poverty`

Rules:
- All lowercase with underscores
- Prefix is consistent across all columns from the same source
- Names should be short but unambiguous
- Avoid redundancy (don't repeat "rate" if measure_type already says it's a rate)

#### Geography Handling

- Convert state names/abbreviations to FIPS codes using `resources/all_fips.csv.gz` (preferred, fast)
- National level = `"00"`
- State = 2-digit FIPS string (e.g., `"06"`)
- County = 5-digit FIPS string (e.g., `"06037"`)
- See CLAUDE.md for FIPS lookup patterns

```r
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

# For state abbreviations:
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, state)

# For state names:
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)
```

#### Time Handling

- Format as `YYYY-mm-dd`
- Weekly data: use Saturday at end of epiweek
- Monthly data: use last day of month
- Annual data: use `YYYY-12-31`

#### National Averages

If the raw data does not include national-level aggregates, calculate population-weighted averages for state-level data and append with `geography = "00"`.

#### Data Quality

- Handle suppressed values: flag with `suppressed_flag` column if imputing
- Remove or filter irrelevant rows (totals that would cause double-counting, non-standard geographies)
- Ensure no duplicate rows per (geography, time, demographic) combination

#### Output Writing

```r
vroom::vroom_write(data_standard, "standard/data.csv.gz", ",")
```

If state and county data are separate, write to `standard/data_state.csv.gz` and `standard/data_county.csv.gz`, or combine into a single file.

### Phase 5: Write measure_info.json

Create `data/<source_name>/measure_info.json` with an entry for every value column in the standardized output. Follow the schema from CLAUDE.md.

#### For Each Value Column

```json
{
  "column_name": {
    "id": "column_name",
    "short_name": "Human-readable short name",
    "long_name": "Full descriptive name",
    "category": "respiratory|immunization|chronic|injury|demographic",
    "short_description": "One sentence description.",
    "long_description": "Detailed description with methodology notes.",
    "statement": "Template: 'In {location}, the {measure} was {value}.'",
    "measure_type": "Incidence|Prevalence|Rate|Percent|Count",
    "unit": "Cases per 100,000|Percent|Count",
    "time_resolution": "Week|Month|Year",
    "sources": [{ "id": "source_id" }]
  }
}
```

#### Use Variants When Columns Follow a Pattern

If multiple columns share the same structure differing only by a variant (e.g., `wastewater_covid`, `wastewater_flu`, `wastewater_rsv`), use the `variants` mechanism:

```json
{
  "{prefix}_{variant}": {
    "short_name": "{prefix}: {variant.short_name}",
    "long_name": "Full name of {variant.short_name}",
    "variants": {
      "covid": { "short_name": "COVID-19" },
      "flu":   { "short_name": "Influenza" },
      "rsv":   { "short_name": "RSV" }
    },
    ...
  }
}
```

#### Always Include `_sources`

```json
{
  "_sources": {
    "source_id": {
      "name": "Full source name",
      "url": "https://...",
      "organization": "Organization name",
      "organization_url": "https://...",
      "description": "Detailed narrative description.",
      "restrictions": "License and usage restrictions."
    }
  }
}
```

### Phase 6: Validate and Report

After writing all files:

1. **Check file structure**: Verify `ingest.R`, `measure_info.json`, `process.json` all exist
2. **If raw data is available**: Offer to run the ingest.R script to test
3. **Report what was created**:
   - Source directory path
   - List of standardized output columns (prefix + name)
   - Geographic levels covered
   - Time resolution
   - Next steps (run ingest, add to bundle, etc.)

## Example

User: `/ingest-source nssp_ili CDC NSSP ILI data, dataset ID abc-1234, state and county level weekly ED visits for ILI`

The skill would:
1. Run `dcf::dcf_add_source("nssp_ili")`
2. Write `ingest.R` that downloads via `dcf::dcf_download_cdc("abc-1234", ...)`, transforms to wide format with columns like `nssp_ili_pct_visits`
3. Write `measure_info.json` with entries for each output column
4. Report the created structure and suggest next steps
