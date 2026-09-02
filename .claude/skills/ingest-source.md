---
name: ingest-source
description: Ingest a new data source into the PopHIVE/Ingest repository — creates the folder structure via the dcf R package (required), writes an ingest.R script that standardizes raw data into wide format, and generates measure_info.json including the _catalog block that drives the website data-sources index. Use when the user wants to add a new CDC/Socrata/URL/file-based data source, mentions "ingest", "new data source", or provides a dataset ID to onboard.
---

# ingest-source

Ingest a new data source: create the folder structure, write the ingest.R script to standardize raw data, and create the measure_info.json.

## Usage

```
/ingest-source <source_name> [description of data source and where to get it]
```

## Description

End-to-end skill for adding and ingesting a new data source into the PopHIVE/Ingest repository. This skill:

1. Creates the folder structure **exclusively via `dcf::dcf_add_source()`** — never by hand
2. Examines the raw data to understand its structure
3. Writes an `ingest.R` script that transforms raw data into the standard wide format
4. Creates a `measure_info.json` documenting all output variables **and a `_catalog` block**
   carrying the catalog text (`summary`, `search_terms`, `bucket`, per-file stratification)
   that the website data-sources page shows
5. Runs `scripts/build_docs.R`, which generates `docs/data_sources_index.json` from that
   `_catalog` block

## Instructions

When the user invokes this skill:

### Phase 1: Create Folder Structure

**CRITICAL — this phase is non-negotiable.** The directory and **every file inside it** MUST be
created by the `dcf` R package via `dcf::dcf_add_source()`. This is the only supported way to
initialize a source.

**You MUST NOT:**
- Create `data/<source_name>/` (or any subdirectory) with `mkdir`, `New-Item`, or the Write tool
- Hand-write `process.json`, or copy one from another source or from a `bundle_*` directory
- Scaffold empty `ingest.R` / `measure_info.json` files before running `dcf_add_source()`
- Work around a missing/broken `dcf` installation by writing the structure manually

**Why `process.json` in particular:** `dcf_add_source()` writes `process.json` with the exact
`name`, `type: "source"`, and `scripts: [{path: "ingest.R", ...}]` fields that `dcf_build()` and
`dcf_process()` depend on. A hand-written or copied `process.json` causes the source to be
silently skipped or misidentified as a bundle during `dcf_build()` (symptoms: "no standard data
files found", "processing bundle", or `process file process.json does not exist`). These failures
are quiet and easy to miss, so there is no acceptable shortcut.

**If `dcf_add_source()` fails** (package not installed, R not found, permissions), STOP and report
the error to the user. Do not proceed to Phase 4/5 and do not fabricate the structure — fix the
`dcf` installation first (`install.packages("dcf")` or `remotes::install_github("dissc-yale/dcf")`).

Steps:

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

4. **Verify the created structure** — confirm `dcf` actually produced all of it:
   ```
   data/<source_name>/
   ├── raw/                  # For downloaded source files
   ├── standard/             # For standardized output files
   ├── ingest.R              # Transformation script (filled in below)
   ├── measure_info.json     # Variable metadata (filled in below)
   └── process.json          # Processing state (dcf-generated — DO NOT hand-edit or create)
   ```

5. **Confirm `process.json` is correct** before continuing. Read it and verify:
   - `"name"` matches the source directory name exactly
   - `"type"` is `"source"` (not `"bundle"`)
   - `"scripts"` references `"ingest.R"` (not `"build.R"`)

   If any of these are wrong, the source was not initialized properly — re-run
   `dcf::dcf_add_source()` rather than patching the file by hand.

From this point on, only `ingest.R` and `measure_info.json` are edited by you. `process.json` is
owned by `dcf` and is updated at runtime through `dcf::dcf_process_record()` inside `ingest.R`.

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

#### Always Include `_catalog`

`_catalog` is the **source of truth for the website data-sources catalog**.
`scripts/build_docs.R` reads it to generate each dataset's entry in
`docs/data_sources_index.json` — that file is fully generated output and must never be
hand-edited. Write `_catalog` as the last top-level key, after `_sources`:

```json
{
  "_catalog": {
    "name": "Display name (OPTIONAL — omit unless it must differ from _sources[].name)",
    "summary": "One to two plain-English sentences describing the dataset.",
    "search_terms": ["Respiratory", "flu", "rsv"],
    "bucket": [],
    "files": {
      "data.csv.gz": "How this file is stratified beyond time and geography",
      "data_county.csv.gz": "..."
    }
  }
}
```

Everything else in the index entry (`github_folder`, `data_url`, `data_dictionary`,
`latest_date`, and the `dataset_link` URLs) is computed from the repo on every build.

**`name`** — omit it. `build_docs.R` falls back to `_sources[].name`, which is right
almost always. Only set it when one source directory needs a label more specific than its
source name — e.g. the `epic_*` directories, which would otherwise all read "Epic Cosmos"
(`epic_concussions` → `"Epic Concussions"`, `epic_injury` → `"Epic Injury"`).

**`summary`** — one to two sentences, **~25 words** (the existing catalog runs 12–70).
Write at roughly a 15-year-old reading level: short sentences, everyday words, one idea
each. Derive it from the `_sources` `description` field(s) — read **all** `_sources`
entries, since a multi-source dataset covers several things — but compress hard; the
catalog summary is a "should I click this?" blurb, not the methodology. Cut model names,
survey-instrument citations, ICD code lists, catchment sizes, and lag times; those stay in
`_sources.description`. Expand an acronym on first use unless it is in the dataset name.
No preamble, no quotes, no markdown.

The house pattern is *`<source short name> <verb> <what it measures>`*:

- `"The NSSP records the percentage of emergency department patient visits for RSV, flu, and COVID-19."`
- `"NARMS tracks antimicrobial resistance in bacteria infecting people and animals in the food supply chain."`
- `"Annual average county unemployment rate from the BLS's LAUS program, covering the labor force ages 16 and older."`
- `"SchoolVaxView monitors vaccination coverage among U.S. school-aged children. Data are collected annually by states, territories, and select local jurisdictions through school vaccination assessments, which review student vaccination records at kindergarten entry."`

**`search_terms`** — the website search box matches on these. **Typically 3–4 terms**
(range 1–9), ordered *topic label(s) first, then specific keywords*:

1. **Topic labels**, capitalized. Start from the labels of the bundles that consume this
   source (`bundle_chronic_diseases` → `"Chronic diseases"`), then reuse the established
   vocabulary rather than inventing a synonym: `Antimicrobial resistance`, `Cancer
   screening`, `Childhood immunizations`, `Chronic diseases`, `County access`, `Enteric
   diseases`, `Injury and overdose`, `Maternal health`, `Measles`, `Preventative
   services`, `Respiratory`, `Rural health`, `Vector borne`, `Youth wellbeing`.
2. **Specific keywords**, lowercase — how a visitor would actually type it: disease and
   pathogen names, colloquial synonyms, and the domain words behind the measures
   (`flu`, `rsv`, `overdose`, `gun`, `firearm`, `hep b`, `diarrhea`, `unemployment`,
   `heat`, `food access`, `traumatic brain injury`, `maternal mortality`).

Examples: `["Respiratory", "flu", "rsv", "Covid"]`, `["Childhood immunizations", "Measles"]`,
`["Rural health", "unemployment", "labor force", "economic determinants"]`,
`["Maternal health", "maternal mortality", "maternal deaths", "pregnancy-related deaths"]`.

**`bucket`** — the site-navigation grouping. **Every dataset currently has `[]`**; the
grouping has not been assigned yet, so write `"bucket": []` unless the user names one.
(`bucket` and `search_terms` are independent fields. Omitting either from `_catalog`
re-derives it from bundle membership on the next build; an explicit `[]` is respected as
an intentional "none" and sticks.)

**`files`** — one key per `standard/*.csv.gz` the ingest produces, mapping **file name**
(not path or URL) to a short blurb saying what that file holds and how it is stratified
**beyond time and geography**. The stratifiers are exactly the non-`time`, non-`geography`,
non-value columns (`age`, `sex`, `race_ethnicity`, `serotype`, `vaccine`, `virus`,
`grade`, …); read a header with `vroom::vroom(path, n_max = 0)`.

Keep each to a **noun phrase of ~5 words** (the existing catalog runs 2–16, no trailing
period). Multi-file sources: say what distinguishes each file from its siblings.
Single-file sources: still name the stratifiers — do not leave a `"Stratified by <tokens>."`
or `"Overall; no stratification beyond time and geography."` placeholder. Only when a file
genuinely has no dimension beyond time and geography, name its measure instead.

```json
"files": {
  "data.csv.gz":            "Vaccination uptake by vaccine and age",
  "data_insurance.csv.gz":  "Vaccination uptake by vaccine and insurance coverage type",
  "data_urban.csv.gz":      "Vaccination uptake by vaccine and urban/rural residence"
}
```

More: `"County level unemployment rates"`, `"School-level vaccination and exemption rates"`,
`"Antimicrobial resistance in people by antimicrobial agent"`, `"Rt data by state"`,
`"Weekly measles case counts"`.

### Phase 6: Validate and Report

After writing all files:

1. **Check file structure**: Verify `ingest.R`, `measure_info.json`, and the dcf-generated
   `process.json` all exist, and that `process.json` still has `type: "source"`, the correct
   `name`, and `scripts: ["ingest.R"]`. If `process.json` is missing, the source was not created
   with `dcf::dcf_add_source()` — go back to Phase 1 and do so; do not write the file yourself.
2. **If raw data is available**: Offer to run the ingest.R script to test
3. **Run the visual QA report for each standardized output file**: For every `standard/data*.csv.gz` file the ingest produced (e.g. `standard/data.csv.gz`, plus `standard/data_state.csv.gz` / `standard/data_county.csv.gz` if split), render `scripts/validate_dataset.Rmd` against it and open the result as a pop-up browser window — do **not** let the report be written into the repo. Render to the OS temp directory and open it with `browseURL()` instead of the default in-place output:

   ```r
   standard_files <- list.files(
     "data/<source_name>/standard",
     pattern = "\\.csv\\.gz$", full.names = TRUE
   )

   for (f in standard_files) {
     rel_path <- sub("^.*(data/.*)$", "\\1", f)  # path relative to project root
     report <- rmarkdown::render(
       "scripts/validate_dataset.Rmd",
       output_file = tempfile("validate_", fileext = ".html"),
       output_dir  = tempdir(),
       params  = list(data_file = rel_path),
       envir   = new.env(),
       quiet   = TRUE
     )
     utils::browseURL(report)
   }
   ```

   Run this from the project root (the Rmd's `project_root: ".."` param resolves `data_file` relative to it). Because `output_file`/`output_dir` point at `tempdir()`, nothing lands in `scripts/` or `data/<source_name>/`; `rmarkdown::render()`'s default `clean = TRUE` also removes any intermediate knitting artifacts, and `browseURL()` pops the finished HTML report open in the user's default browser as its own window/tab. Requires pandoc to be set up per the "Pandoc requirement" section of CLAUDE.md — if rendering fails with a pandoc-not-found error, point the user there. Do this for each standardized file even if `git_compare` finds no prior version (a brand-new source has no history yet, so the report just skips that section).
4. **Report what was created**:
   - Source directory path
   - List of standardized output columns (prefix + name)
   - Geographic levels covered
   - Time resolution
   - Next steps (run ingest, add to bundle, etc.)

### Phase 7: Regenerate the Docs and Data Sources Index

Once the source's `standard/*.csv.gz` files exist and `measure_info.json` has its
`_catalog` block (Phase 5), the website catalog entry is just a build away.
`docs/data_sources_index.json` is **fully generated** by `scripts/build_docs.R`
(which also runs automatically every day and on every `measure_info.json`
change) — **never hand-edit it**; edit `_catalog` in `measure_info.json` and
rebuild.

1. **Rebuild.** From the repo **root** (not `docs/`):
   ```powershell
   Rscript scripts/build_docs.R
   ```
   This rewrites `docs/index.html`, `resources/data_manifest.json`, and
   `docs/data_sources_index.json`.

2. **Watch for the fallback warning.** If `_catalog.summary` is missing or empty,
   the build prints:
   ```
   WARNING: <dataset> has no hand-written summary in measure_info.json _catalog
   ```
   and falls back to a mechanical first-sentence stub, which reads as an abrupt
   fragment. Treat that line as a stop signal: write a real `_catalog.summary`
   and rebuild.

3. **Verify** the new entry in `docs/data_sources_index.json`: the `summary`,
   `search_terms`, and `bucket` match what you wrote in `_catalog`; `latest_date`
   looks right; and `files` has one entry per `standard/*.csv.gz`, each with a
   `dataset_link` pointing at a real file and the `dataset_stratification` you
   wrote. A `"Stratified by <tokens>."` blurb means `_catalog.files` is missing
   that file name (the keys are bare file names — no path, no URL).

**Commit** `data/<source_name>/measure_info.json` together with the regenerated
`docs/` and `resources/data_manifest.json`.

## Example

User: `/ingest-source nssp_ili CDC NSSP ILI data, dataset ID abc-1234, state and county level weekly ED visits for ILI`

The skill would:
1. Run `dcf::dcf_add_source("nssp_ili")` to create the directory and all of its contents, then
   verify the generated `process.json`
2. Write `ingest.R` that downloads via `dcf::dcf_download_cdc("abc-1234", ...)`, transforms to wide format with columns like `nssp_ili_pct_visits`
3. Write `measure_info.json` with entries for each output column, plus a `_catalog` block
   holding the dataset `summary`, `search_terms`, `bucket`, and a per-file stratification blurb
4. Run `Rscript scripts/build_docs.R` to regenerate `docs/data_sources_index.json` from that
   `_catalog`, verify the entry, and commit
5. Report the created structure and suggest next steps
