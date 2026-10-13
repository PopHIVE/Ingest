# neiss

CPSC **National Electronic Injury Surveillance System (NEISS)** emergency
department injury estimates (national-only, annual, by age, sex, race and
ethnicity, and by injury diagnosis or consumer product group).

The actual download from CPSC and transform to the standard wide format happen
in the [`PopHIVE/neiss`](https://github.com/PopHIVE/neiss) repository.
`ingest.R` here just pulls the pre-standardized `standard/data_*.csv.gz` files
and `measure_info.json` from that repo's `main` branch — it does not re-derive
anything from CPSC.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
