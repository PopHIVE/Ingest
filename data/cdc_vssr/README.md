# cdc_vssr

CDC NCHS **VSRR Provisional Maternal Death Counts and Rates**
(national-only, monthly, by age and race/Hispanic origin).

The actual download from CDC (Socrata dataset `e2d5-ggg7`) and transform to
the standard wide format happen in the
[`PopHIVE/cdc_vssr`](https://github.com/PopHIVE/cdc_vssr) repository.
`ingest.R` here just pulls the pre-standardized `standard/data.csv.gz` and
`measure_info.json` from that repo's `main` branch — it does not re-derive
anything from CDC.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
