# epic_health_alerts

Ingests **all** conditions from the Epic Research Health Alerts page
(https://www.epicresearch.org/health-alerts/).

The page is a live, server-rendered snapshot of *currently active* alerts (no
API, no historical archive). `ingest.R` downloads the page, iterates over every
condition section, and appends the current snapshot to `standard/data.csv.gz`,
keyed on `geography` + `condition` + `time` (week ending Saturday), so repeated
runs accumulate a weekly time series.

## Output: `standard/data.csv.gz`

| Column | Description |
|--------|-------------|
| `geography` | FIPS code (`"01"` state-wide, `"06037"` county) |
| `time` | Week ending Saturday of the scrape, `MM-DD-YYYY` |
| `condition` | Alerted condition (e.g. Cyclosporiasis, Heat Illness) |
| `estimated_onset` | Epic Research estimated onset date, `MM-DD-YYYY` |
| `value` | Cases per 100,000 for the latest week |
| `partial_week_flag` | `1` if rate is from a partial week (trailing `*` on page), else `0` |
| `date_scraped` | Date the page was downloaded, `MM-DD-YYYY` |
| `date_epic_updated` | Page-wide "Last updated" date, `MM-DD-YYYY` |

This is a dcf data source project. Check with `dcf_check()` and process with
`dcf_process()`.
