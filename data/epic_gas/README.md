# epic_gas

Group A Streptococcus (GAS) patient counts from Epic Cosmos, by state, age group,
and calendar quarter. The numerator counts patients with a strep throat diagnosis
(ICD-10 `J02.0` streptococcal pharyngitis, `J03.00` acute streptococcal tonsillitis
unspecified, `J03.01` acute recurrent streptococcal tonsillitis); the denominator is
the total patient count for the same state/quarter/age cell.

`ingest.R` pulls the pre-processed standard file from
[PopHIVE/epic_preprocessing](https://github.com/PopHIVE/epic_preprocessing/tree/main/data/cosmos_gas),
where the SlicerDicer export is parsed and standardized. There is no `raw/`
directory here — the raw password-protected xlsx lives upstream.

**Population base** (SlicerDicer session `2809857`): data model `Patients`,
population base `All Patients`, criteria `Country of Care = United States of America`
and `Has Any Encounters`. This is **not** restricted to emergency department visits —
the denominator is all patients with any encounter.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national |
| `time` | `YYYY-mm-dd`, the **last day of the quarter** (e.g. `2025-03-31`) |
| `age` | `<1 Years`, `1-4 Years`, `5-17 Years`, `18-49 Years`, `50-64 Years`, `65+ Years`, `Total` |
| `epic_n_strep_throat` | Count of patients with a strep throat diagnosis |
| `epic_pct_strep_throat` | **Percent** of patients, `n / denominator * 100` |
| `epic_strep_throat_suppressed_flag` | Suppression flag for the numerator — covers **both** measures above |
| `epic_n_patients` | Total patients (denominator) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |

The measure is a percentage, not a rate per 100,000.

## Notes

- **Suppression**: counts of 10 or fewer are withheld by Epic and imputed as 5, with
  the corresponding flag set to 1. Flags are computed before imputation, so they
  record what Epic withheld. Where the *denominator* was suppressed,
  `epic_pct_strep_throat` is left `NA` rather than computed from an imputed
  denominator. For "was the percentage affected by suppression at all?", take the OR
  of the two flags.
- **Partial quarters**: quarters only partially covered by the upstream export are
  dropped, so every value covers a full calendar quarter.
- **Overlap**: `epic_resp_infections/standard/quarterly_gas.csv.gz` is an older,
  pre-standardization copy of this same upstream source (unprefixed columns,
  `MM-DD-YYYY` dates, rate per 100,000). Prefer this source.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
