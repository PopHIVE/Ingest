# add-source

Add a new data source to the PopHIVE/Ingest repository.

## Usage

```
/add-source <source_name>
```

## Description

Creates a new data source folder structure using `dcf::dcf_add_source()`. This initializes all required files and directories for a new data ingestion pipeline.

## Instructions

When the user invokes this skill with a source name:

1. **Validate the source name**:
   - Must be lowercase with underscores (e.g., `cdc_flu_data`, `epic_diabetes`)
   - No spaces or special characters
   - Should be descriptive of the data source

2. **Detect the R installation** (Windows only):
   First, find available R versions:
   ```bash
   powershell -Command "Get-ChildItem 'C:\Program Files\R' | Select-Object Name"
   ```
   Use the most recent version found (e.g., `R-4.3.0`).

3. **Run the dcf command** from the project root:
   ```r
   dcf::dcf_add_source("<source_name>")
   ```

   **On Windows**, use PowerShell with the detected R version:
   ```bash
   cd "<project_root>" && powershell -Command "& 'C:\Program Files\R\<R_VERSION>\bin\Rscript.exe' -e \"dcf::dcf_add_source('<source_name>')\""
   ```
   Replace `<R_VERSION>` with the detected version (e.g., `R-4.3.0`).

   **On macOS/Linux**, use:
   ```bash
   cd "<project_root>" && Rscript -e 'dcf::dcf_add_source("<source_name>")'
   ```

4. **Verify the created structure**:
   The command should create:
   ```
   data/<source_name>/
   ├── raw/           # For downloaded source files
   ├── standard/      # For standardized output files
   ├── ingest.R       # Transformation script (needs editing)
   ├── measure_info.json  # Variable metadata (needs editing)
   └── process.json   # Processing state (auto-generated)
   ```

5. **Report success** and remind the user of next steps:
   - Edit `ingest.R` to implement the data transformation
   - Edit `measure_info.json` to document variables
   - Follow the templates in CLAUDE.md

## Example

User: `/add-source wastewater_influenza`

Response: Create the data source folder, then provide guidance on next steps.
