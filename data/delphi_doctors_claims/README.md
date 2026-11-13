# delphi_doctors_claims

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can us the `dcf` package to check the project:

```R
dcf_check_source("delphi_doctors_claims", "..")
```

And process it:

```R
dcf_process("delphi_doctors_claims", "..")
```

## Geographies that stopped updating

As of the 2026-11-10 build, the latest week in the standard file is the week
ending 2026-11-07. Delphi publishes low-volume geographies only intermittently,
and the standard file keeps only week-ending Saturdays, so the states below
have no recent weeks. The gaps are in Delphi's raw data, not introduced by
`ingest.R`: the last week in the standard file is always the last Saturday in
the raw data.

| State | Last week | Last raw day (any weekday) |
|-------|-----------|----------------------------|
| Puerto Rico (72) | 2022-10-29 | 2022-10-31 |
| Alaska (02) | 2026-10-03 | 2026-10-06 |
| North Dakota (38) | 2026-10-17 | 2026-10-18 |

At the county level, 1,259 of 2,593 counties had no value in the last two
weeks of the 2026-11-10 build.
