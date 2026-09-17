# crisis_trends

This is a dcf data source project, initialized with `dcf::dcf_add_source`. It
ingests the `crisistrends_monthly_aggregate` dataset from Crisis Text Line's
CTL Data Engineering team via the
[ctl_data_client](https://github.com/CrisisTextLine/ctl-data-client) Python
package, called from R through `reticulate`.

Adapted from a standalone reference implementation; see `ingest.R`'s header
comment for the full transform description. Two differences from that
reference:

- It also produces tag-combination (AND filter), annual-aggregate, and
  unsuppressed internal QC files. This source deliberately produces only
  `standard/data.csv.gz`.
- `pophive.config.json` is not committed here. It's supplied at runtime from
  a GitHub Actions secret -- see **Setup** below.

## Setup

1. **Get `pophive.config.json` from CTL Data Engineering.** It's
   `auth_type: "public"` (not a credential that grants write access), but it
   is user/project-specific and carries an expiration reminder date (see its
   base64 `payload`), so it needs to be refreshed with CTL Data Engineering
   periodically.
2. **Store its contents as a GitHub Actions secret** named
   `CRISISTRENDS_POPHIVE_CONFIG` (repo Settings -> Secrets and variables ->
   Actions -> New repository secret). Paste the whole JSON file contents as
   the secret value.
3. **Reference the secret in the daily-update workflow**
   (`.github/workflows/update_daily_data.yaml`): add it to the job's `env:`
   block, e.g.
   ```yaml
   env:
     CRISISTRENDS_POPHIVE_CONFIG: ${{ secrets.CRISISTRENDS_POPHIVE_CONFIG }}
   ```
   and add a processing step alongside the other sources:
   ```yaml
   - run: Rscript -e "dcf::dcf_process('crisis_trends')"
   ```
   `ingest.R` writes the secret's contents to `pophive.config.json` on disk
   itself (step 1) before calling `ctl_data_client`, so no extra shell step is
   needed to materialize the file.
4. `pophive.config.json` is listed in `.gitignore` so it's never committed
   locally. Raw data is never written to disk at all: `ingest.R` downloads the
   dataset's parquet to a temp file, reads it, and deletes it before finishing
   (even on error), so there's no local archived copy to fall back to --
   `pophive.config.json` (or the `CRISISTRENDS_POPHIVE_CONFIG` secret) is
   required on every run.
5. Make sure Python is available to `reticulate`; `ingest.R` installs
   `ctl_data_client` automatically on first run via `pip` (only needed when
   the config file is present).
6. Install the R package `GaussSuppression` (cell suppression) in addition to
   `dplyr`, `tidyr`, `arrow` and `vroom`.
7. Run `dcf::dcf_process("crisis_trends")` (or `source("ingest.R")` from this
   directory).

The transform re-runs when CTL publishes a new release, when the output file
is missing, when `TRANSFORM_VERSION` at the top of `ingest.R` has been bumped
(bump it whenever you change the transform), or when the environment variable
`CTL_FORCE_TRANSFORM` is set.

## Output

Only `standard/data.csv.gz` is published -- one row per (`geography`,
`time`, `age`):

- `geography`: state FIPS code, `"00"` for a national rollup (includes
  conversations where state wasn't reported), or `"Missing"` (state not
  reported, as its own visible bucket)
- `time`: last day of the month
- `age`: a specific age bin, `"Overall"` for all ages (includes conversations
  where age wasn't reported), or `"Missing"` (age not reported, as its own
  visible bucket)
- `crisistrends_<tag>`: count of conversations that month mentioning that
  issue tag anywhere in `issue_tags`. A conversation can carry multiple tags
  (e.g. "Isolation / Loneliness, School" counts towards both), so tag columns
  overlap and won't sum to a total. The 4 abuse subtype tags (Emotional,
  Physical, Sexual, Unspecified) are collapsed into a single
  `crisistrends_abuse` column. Only 12 of the 25 raw tags are surfaced as
  columns -- see `tag_slugs` and `abuse_tags` in `ingest.R`.
- `crisistrends_n_tagged`: count of conversations that month with at least
  one issue tag recorded, counted once per conversation no matter how many
  tags it carries -- the correct denominator for a topic's share of tagged
  conversations (unlike the per-tag columns, which double-count a
  multi-tagged conversation).
- `crisistrends_total`: count of all conversations that month, tagged or not.
  There is no separate column for untagged conversations alone -- they're
  only reflected here.
- `crisistrends_<col>_reported_total`: for every column above, the sum of its
  visible (post-suppression) values across the real states, excluding `"00"`
  and `"Missing"`, for the same month and age group -- the denominator for a
  state's share when a map only plots real states. Repeated on every
  geography row.

Neither the raw parquet (never written to disk, see **Setup** above) nor an
unsuppressed internal copy is kept in this repo -- only the
disclosure-controlled `standard/data.csv.gz`.

### Suppression

Every `crisistrends_*` count is suppressed (published as a blank/`NA` cell,
via the [GaussSuppression](https://cran.r-project.org/package=GaussSuppression)
package) if it's too small to publish safely:

- a cell for a specific `geography` and specific `age` is suppressed if its
  count is `<= 4`
- any total -- across state (`geography == "00"`), across age
  (`age == "Overall"`), or across issue (`crisistrends_n_tagged` /
  `crisistrends_total`) -- is suppressed if its count is `<= 24`
- a cell is also suppressed if its calendar-year raw total, or its all-time
  visible total, is `<= 24`

A true count of `0` is never suppressed (it discloses nothing). Beyond these
thresholds, GaussSuppression also applies complementary ("secondary")
suppression, blanking a few additional, otherwise-safe cells wherever a
suppressed value could otherwise be recomputed from the state/age hierarchy.
The reference project this was adapted from left the year/all-time rules at
`<= 25` (inherited from its first suppression pass) while the margin/total
rule was `<= 24`; here `YEAR_MAX` is unified with `TOTAL_MAX` at `<= 24` (see
the constants at the top of `ingest.R`).

## Windows gotcha (if you see "DLL load failed while importing lib")

R's `arrow` package and Python's `pyarrow` (pulled in by `ctl_data_client`)
bundle incompatible Arrow C++ builds, and loading both into the same process
in the wrong order crashes with `"DLL load failed while importing lib"` on
Windows. `ingest.R` avoids this by loading `reticulate` and doing all the
Python/`ctl_data_client` work *before* `library(arrow)` is ever called --
keep that order if you edit this script.

## Usage

```R
dcf::dcf_check()
dcf::dcf_process("crisis_trends")
```
