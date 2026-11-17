# narms

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can use the `dcf` package to check the project:

```R
dcf_check_source("narms", "..")
```

And process it:

```R
dcf_process("narms", "..")
```

## NARMS Now scrape modes

The CDC NARMS Now human data come from a Power BI dashboard and take about
6,250 queries (2-3 hours) to scrape in full. `ingest.R` therefore reads the
`NARMS_NOW_MODE` environment variable:

| Mode | What it does | Who runs it |
|------|--------------|-------------|
| `standardize` (default) | Never queries CDC. Rebuilds the standard files from the raw files already in `raw/` and refreshes the FDA xlsx sources. | Tue/Fri `build.yaml` |
| `check` | National-only pre-pass (130 queries, ~18 minutes), compared with the stored national rows. Only the organism, test method and year combinations whose national rows changed are re-scraped for every state, which is typically a few dozen organism-years and 20-30 minutes, so a typical run is 45-60 minutes; if nothing changed it stops after the pre-pass. A full scrape runs instead when the last one is more than 6 months old, which bounds anything the national comparison cannot see, or when the changed set would cost more than a full scrape. | Monthly `update_narms.yaml` |
| `full` | Re-scrapes every year since 1999 for every state (about 3 hours). | `update_narms.yaml` with `force = true`, or a local backfill |

For a quick local test without a multi-hour run:

```sh
NARMS_NOW_MODE=full NARMS_NOW_LIMIT_ORGANISMS=2 NARMS_NOW_LIMIT_SITES=3 \
  NARMS_NOW_OUT_DIR=/tmp/narms_test Rscript -e "dcf::dcf_process('narms')"
```

`NARMS_NOW_OUT_DIR` writes the scraped raw files elsewhere and leaves
`process.json` untouched. A scrape that still has failed queries after retries
writes `raw/narms_now_scrape_errors.json`, keeps the previous raw files and
fails the run.
