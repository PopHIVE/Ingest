# nccr — National Childhood Cancer Registry (NCCR*Explorer)

Childhood, adolescent, and young-adult (ages 0–39) cancer **incidence** statistics
from NCI's [NCCR*Explorer](https://nccrexplorer.ccdi.cancer.gov/), pooled from
~29 U.S. cancer registries (~76% of the U.S. population), diagnosed 2001 forward.

## What is ingested

`ingest.R` harvests the NCCR*Explorer application data API
(`render_region_5.php`, incidence / trends-over-time), one request per ICCC
cancer site, and reshapes the result into the standard wide format.

- **Geography:** national only (`geography = "00"`). NCCR*Explorer does **not**
  provide state- or registry-level breakdowns.
- **Time:** annual, `YYYY-12-31`, 2001–2022.
- **Dimensions:** `age` (12 NCCR groupings: <1, 1-4, 5-9, 10-14, 15-19, 0-19,
  20-24, 25-29, 30-39, 15-39, 20-39, and 0-39 = full NCCR population total),
  `sex` (Overall/Male/Female),
  `race_ethnicity` (Overall, White, Black, Asian/Pacific Islander,
  American Indian/Alaska Native, Hispanic).
- **Measures:** one age-adjusted incidence rate (per 1,000,000) column per ICCC
  site, plus 95% CI bounds — `nccr_<site>`, `nccr_<site>_lcl`, `nccr_<site>_ucl`.
  Sites are restricted to **All ICCC Sites Combined** plus the 14 top-level ICCC
  category groups (I. Leukemias, II. Lymphomas, III. CNS Neoplasms Malignant &
  Non-Malignant, IV. Neuroblastoma, V. Retinoblastoma, VI. Renal, VII. Hepatic,
  VIII. Bone, IX. Soft Tissue, X. Germ Cell Malignant & Non-Malignant,
  XI. Epithelial/Melanomas, XII. Other) — 15 sites total. The lettered ICCC
  subcategories (I.a, I.b, …) are not included. Edit `GROUP_SITES` in `ingest.R`
  to change scope.

## Output

`standard/data.csv.gz` — 4,752 index rows × 50 columns
(5 index columns + 15 ICCC sites × 3 measures).

## Notes

- The data API returns double-encoded JSON; code↔label lookups come from
  `get_var_formats.php` (saved as `raw/var_formats.json`).
- Change detection hashes all raw files; the standard file is only rebuilt when
  the upstream data changes.
- Column slugs are derived deterministically from ICCC short names and are kept
  in sync between `ingest.R` and `measure_info.json`.
