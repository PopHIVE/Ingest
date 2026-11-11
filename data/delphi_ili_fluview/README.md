# delphi_ili_fluview

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can us the `dcf` package to check the project:

```R
dcf_check_source("delphi_ili_fluview", "..")
```

And process it:

```R
dcf_process("delphi_ili_fluview", "..")
```

## Geographies with missing weeks

As of 2026-11-11 every geography has data for the latest week (ending
2026-10-31), but eight states are missing the whole 2025-26 season. These
weeks are absent from both Delphi endpoints (the V4 FluView endpoint and the
v5 `fluview_ilinet` source), so the gap is in the CDC data, not `ingest.R`:

| State | Missing weeks |
|-------|---------------|
| Alaska (02) | 2025-09-27 to 2026-10-03 (54 weeks) |
| Connecticut (09), Hawaii (15), New York (36), Oklahoma (40), Oregon (41), South Dakota (46), Utah (49) | 2025-10-04 to 2026-10-03 (53 weeks) |

Older gaps: national data have no summer weeks in 1998-2002 (ILINet did not
report off-season then); Delaware is missing 2011-06-11 to 2011-10-01 and DC
2015-08-08 to 2015-10-10. Weeks in which providers saw no patients (42
state-weeks, e.g. Delaware in 2011) have counts but no ILI percentage.

Puerto Rico (72, from 2013) and the U.S. Virgin Islands (78, from 2015) were
added when the source moved to the v5 API in November 2026.
