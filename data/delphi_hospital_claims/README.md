# delphi_hospital_claims

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can us the `dcf` package to check the project:

```R
dcf_check_source("delphi_hospital_claims", "..")
```

And process it:

```R
dcf_process("delphi_hospital_claims", "..")
```

## Geographies that stopped updating

As of the 2026-11-10 build, the latest week in the standard file is the week
ending 2026-11-07. Delphi publishes low-volume states only intermittently, and
the standard file keeps only week-ending Saturdays, so the states below have no
recent weeks. The gaps are in Delphi's raw data, not introduced by `ingest.R`:
the last week in the standard file is always the last Saturday in the raw data.

| State | COVID last week | Flu last week | Last raw day (any weekday) |
|-------|-----------------|---------------|----------------------------|
| Alaska (02) | 2022-08-27 | 2022-08-27 | 2022-09-01 |
| South Dakota (46) | 2026-08-01 | 2026-08-01 | 2026-10-19 |
| North Dakota (38) | 2026-09-12 | 2026-09-12 | 2026-10-15 |
| Vermont (50) | 2026-09-26 | 2026-09-26 | 2026-10-02 |
| Montana (30) | 2026-10-03 | 2026-10-03 | 2026-10-04 |
| Rhode Island (44) | 2026-10-03 | 2026-10-03 | 2026-11-09 |
| Michigan (26) | 2026-10-17 | current | 2026-11-09 |
| Wyoming (56) | 2026-10-17 | 2026-09-26 | 2026-11-04 |
| District of Columbia (11) | current | 2026-10-03 | 2026-11-09 |
| Iowa (19) | current | 2026-10-03 | 2026-10-06 |
| Nebraska (31) | current | 2026-10-03 | 2026-10-06 |
| New Mexico (35) | current | 2026-10-17 | 2026-11-09 |

States with a recent raw day but no recent Saturday (e.g. Rhode Island,
Michigan) report on scattered days only. At the county level, 1,430 of 1,634
counties had no value in the last two weeks of the 2026-11-10 build.
