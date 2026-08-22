# Rural Health Bundle

This bundle combines county- and state-level social, economic, environmental,
and health-resource determinants relevant to rural health for the PopHIVE
platform. It gathers the sources that were previously only available as
standalone standardized files (and therefore not reachable through bundle
consumers such as the MCP server).

## Data Sources

- **HUD CHAS** (`hud_chas`): Share of occupied housing units with at least one of four severe housing problems (ACS 5-year vintage, 2022). County and state.
- **HRSA Area Health Resource File** (`area_health_resource_file`): Annual county/state/national health workforce, facility, population, environmental, and Medicare utilization measures (1999-2025), including HPSA shortage designations and the USDA Rural-Urban Continuum Code.
- **BLS LAUS** (`bls_laus`): Annual average unemployment rate (2025). County and state.
- **USDA Food Access Research Atlas** (`usda_food_access`): Share of the county population that is low-income with limited grocery-store access (2019). County only.

## Output Files

Both files are long format with one row per geography x time x measure.

### rural_health_county.parquet

- `geography`: 5-digit county FIPS code (string)
- `time`: Year-end date of the data vintage
- `outcome_name`: Measure identifier (matches the source column name, e.g. `hud_pct_severe_housing_problems`, `ahrf_pcp`, `bls_pct_unemployment`, `usda_pct_limited_access_low_income`)
- `value`: Measure value (units vary by measure; see `measure_info.json`)
- `source`: "HUD CHAS", "HRSA AHRF", "BLS LAUS", or "USDA Food Access Research Atlas"

### rural_health_state.parquet

Same columns; `geography` is a 2-digit state FIPS code or "00" for national
(AHRF only). USDA food access is county-only and not included here.
