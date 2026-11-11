# delphi_nhsn

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can us the `dcf` package to check the project:

```R
dcf_check_source("delphi_nhsn", "..")
```

And process it:

```R
dcf_process("delphi_nhsn", "..")
```

## Geographies that stopped updating

- **All geographies, 2026-09-12 to 2026-11:** the source was frozen at the week
  ending 2026-09-12 because Delphi's legacy COVIDcast endpoint stopped
  updating on 2026-09-18. `ingest.R` now reads the Epidata v5 API, which
  restores the missing weeks (data through 2026-10-31 as of 2026-11-11).
- **American Samoa (60):** no data after the week ending 2026-02-28 (all three
  viruses).
- **Northern Mariana Islands (69):** reports intermittently; 138 weeks of
  COVID-19 data are missing between 2020-08-08 and 2026-10-24.

Smaller internal gaps in COVID-19 weeks: Virgin Islands (78) 19 weeks,
Massachusetts (25) 17, Minnesota (27) 14, West Virginia (54) 5, Guam (66) 1.
