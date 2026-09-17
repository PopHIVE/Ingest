# crisis_text_line

Crisis Text Line's Crisis Trends data (monthly conversation counts by state,
age group, and issue tag), pulled as a pre-processed, cell-suppressed standard
file from https://github.com/PopHIVE/crisis-text-line.

That upstream repo is **private** -- it holds the raw, unsuppressed
conversation data, which must never be exposed here. `ingest.R` only pulls
`standard/data.csv.gz` and `measure_info.json` from it via the authenticated
GitHub API; the upstream repo's `raw/` data and its QC-only
`standard/data_unsuppressed.csv.gz` are never touched.

## Setup

`ingest.R` needs a GitHub PAT with read access to the private
`PopHIVE/crisis-text-line` repo, set as the `CRISIS_TEXT_LINE_TOKEN`
environment variable (locally, and as a repo secret for the
`build.yaml` GitHub Action).

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
