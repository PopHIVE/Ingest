# Enteric Diseases Bundle

This bundle combines NNDSS-reported enteric/gastrointestinal disease case
counts, CDC BEAM Dashboard enteric pathogen isolate surveillance, and NARMS
antimicrobial resistance surveillance data for the PopHIVE platform.

## Data Sources

- **CDC NNDSS**: Weekly cumulative year-to-date case counts for campylobacteriosis, cholera, giardiasis, salmonellosis (excluding Typhi/Paratyphi), cyclosporiasis, typhoid fever (Salmonella Typhi), paratyphoid fever (Salmonella Paratyphi), Shiga toxin-producing E. coli (STEC), and shigellosis.
- **CDC BEAM Dashboard**: Monthly isolate counts, outbreak-associated isolate counts, and isolate rates per 100,000 population, by state and pathogen (Campylobacter, Salmonella, Shigella, STEC, Vibrio).
- **NARMS**: Antimicrobial resistance surveillance across human clinical isolates (NARMS Now), FDA retail meats, FDA animal pathogen, and FDA food-producing animal (HACCP, Cecal, Minor Species) programs.

## Output Files

### enteric_diseases.parquet

Long-format case/isolate counts by state, disease/pathogen, and source.

**Columns:**
- `geography`: State name or "United States"
- `date`: Week-ending date (NNDSS) or month-ending date (BEAM)
- `measure`: Disease/pathogen identifier (see `measure_info.json` for definitions)
- `value`: Cumulative year-to-date case count (NNDSS) or monthly isolate count/rate (BEAM)
- `source`: Data source ("CDC NNDSS" or "CDC BEAM Dashboard")

### resistance_by_agent.parquet

Long-format percent resistance to individual antimicrobial agents, combined
across all NARMS programs.

**Columns:**
- `geography`: State name or "United States"
- `time`: Year-end date
- `source`: Program the isolate was drawn from ("NARMS Now (Human Clinical)", "FDA Retail Meats", "FDA Animal Pathogen", "FDA Food Animals (HACCP/Cecal/Minor Species)")
- `genus`: Pathogen genus
- `species_serotype`, `antimicrobial_class`, `antimicrobial`, `test_method`, `meat_source`, `host_species`, `collection_source`, `source_type`: descriptive columns populated where applicable to the source program, NA otherwise
- `pct_resistant`, `n_resistant`, `n_tested`: resistance percentage and counts
- `mic50`, `mic90`: minimum inhibitory concentration percentiles (not available for NARMS Now human clinical data)

### resistance_by_pattern.parquet

Long-format multi-drug resistance patterns, human clinical isolates (NARMS Now) only.

**Columns:**
- `geography`: State name or "United States"
- `time`: Year-end date
- `source`: Data source ("NARMS Now (Human Clinical)")
- `genus`, `species_serotype`, `pattern`, `test_method`: descriptive columns
- `pct_resistant`, `n_resistant`, `n_tested`: resistance percentage and counts

## Building the Bundle

From the project root:
```r
dcf::dcf_process("bundle_enteric_diseases", ".")
```

Or from this directory:
```r
source("build.R")
```
