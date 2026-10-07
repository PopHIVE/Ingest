# epic_concussions

Emergency department encounter counts with a concussion diagnosis from Epic Cosmos, by
state, month, age at time of visit, and evaluated sex. The numerator counts ED
encounters with Epic's "Concussion" ED Diagnoses grouping (ICD-10-CA `S06.0`); the
denominator is the total ED encounter count (any diagnosis) for the same
state/month/age/sex cell.

**Population base** (SlicerDicer session `2852165`, "Number of ED Encounters by Age at
Time of Visit Range and State of Residence and ED Diagnoses and Evaluated Sex"): data
model `ED Encounters`, population base `All ED Encounters`, criteria
`Country of Residence = United States of America`. This **is** restricted to emergency
department visits (unlike `cosmos_gas`, which uses `All Patients`).

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national (the SlicerDicer `Total` row). This export has no District of Columbia block - DC is absent from the source session, not dropped by the ingest. |
| `time` | `YYYY-mm-dd`, the **last day of the month** |
| `age` | Epic's own "Age at Time of Visit" buckets: `<1 Years`, `1-4 Years`, `5-9 Years`, `10-13 Years`, `14-17 Years`, `18-29 Years`, `30-44 Years`, `45-64 Years`, `65+ Years`, `Overall`. As of the `2852165` export (2026-08-27) these buckets are contiguous; the earlier `2851311` export used `10-12 Years` and had a gap at age 13, which this session closed. |
| `sex` | `Female`, `Male`, `Overall`. Epic's "Ambiguous" evaluated-sex bucket is dropped (not a standard sex category); its counts are still included in `Overall` via Epic's own Total column. |
| `epic_n_concussion` | Count of ED encounters with a concussion diagnosis |
| `epic_n_concussion_suppressed_flag` | Suppression flag for the numerator — covers **both** `epic_n_concussion` and `epic_pct_concussion` |
| `epic_pct_concussion` | **Percent** of ED encounters, `n / denominator * 100` (not a rate per 100,000) |
| `epic_n_ed_encounters` | Total ED encounters, any diagnosis (denominator) |
| `epic_n_ed_encounters_suppressed_flag` | Suppression flag for the denominator |

## Updating

1. Re-run SlicerDicer session `2852165` and export the crosstab (rows: Year, Month,
   State of Residence, ED Diagnoses -> `Concussion( ICD-10-CA: S06.0 )` and `Total`;
   columns: Age at Time of Visit Range -> Evaluated Sex; measure: Number of ED
   Encounters).
2. Drop the export into `raw/staging/`, **replacing** the previous file, so the same
   months are not read twice. This ingest expects exactly one staging file.
3. From the project root: `Rscript -e "dcf::dcf_process('epic_concussions')"`, or
   while iterating: `setwd("data/epic_concussions"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) — SlicerDicer
  xlsx exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`
  (`python -m pip install msoffcrypto-tool`).

## Notes

- **Layout**: this export nests row dimensions two deep (State of Residence ->
  ED Diagnoses -> Year -> Month) and column dimensions two deep (Age at Time of Visit
  -> Evaluated Sex). The ingest locates the row-dimension header dynamically (the row
  where column A equals `"State of Residence"`) rather than hardcoding a row number, and
  `stop()`s if columns B/C/D aren't `ED Diagnoses`/`Year`/`Month` on that row. The age
  and sex column headers sit two and one rows above it respectively. Grouping cells
  (state, diagnosis, year, age-group, and sex-group labels) are merged and must be
  filled down/right - if the session adds or reorders stratifications, update
  `standardize_age()` or the `sex_std` mapping in `ingest.R`.
- **Age label phrasing**: this session mixes two phrasings for the same kind of bound
  pair - `"≥ X and < Y Years"` for the first two buckets and
  `"X Years or more and less than Y Years"` for the middle buckets. `standardize_age()`
  normalizes the worded form to the symbolic one before extracting bounds and
  subtracting 1 from the exclusive upper bound.
- **Dropped rows**: `None of the above` (unknown state of residence) is dropped, with a
  `message()` reporting the count. The trailing partial period at the end of the
  exported range (e.g. `Jun 1 - Jun 22`) is dropped as well, since it isn't a full
  month.
- **Suppression**: a count of 10 or fewer arrives as `"10 or fewer"` or as a blank cell;
  it is imputed as 5 and the corresponding flag set to 1. Flags are computed before
  imputation, so they record what Epic withheld rather than what the ingest wrote.
  There are two independently suppressible cells per row, so two flags:
  `epic_n_concussion_suppressed_flag` for the numerator (covering `epic_n_concussion`
  **and** `epic_pct_concussion`, since the percent is derived from that same cell) and
  `epic_n_ed_encounters_suppressed_flag` for the denominator.
- Where the *denominator* was suppressed, `epic_pct_concussion` is left `NA` rather than
  the meaningless `5 / 5 * 100`, following the `cosmos_vaccines`/`cosmos_gas`
  precedent. The script asserts that the percent is missing exactly where the
  denominator flag is 1.
- The script validates geography, time, duplicate index rows, percentage range, and the
  flag invariants before writing, and reports per-measure imputation counts.
- This ingest expects a **single** staging file and `stop()`s if more than one is
  present, since the export already covers the full history in one file. Extend
  `ingest.R` if a future update needs to combine multiple exports.
