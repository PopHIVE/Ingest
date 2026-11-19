# Bugs to look for in ingest.R and build.R

Most of these have reached main. The commit is given where there is one. For every changed hunk, go through the list. The cue is what to search for in the diff, the question is what to answer before moving on.

## Reading the raw data

**Numbers with thousands separators read as missing.** CDC downloads on the build server write `1,674`, and a numeric column type turns that into NA. The NSSP lab counts lost every value of 1,000 or more this way (`0650b7c2`), and wastewater lost `Population_Served` (`b0089348`).
- Cue: `vroom(`, `read_csv(`, `col_double()`, `as.numeric(` on a count column.
- Question: are counts read with `vroom::col_number()` or `readr::parse_number()`, or are commas stripped first?

**Dates in a format the script did not expect.** The same CDC date column can arrive as `2025-02-08`, `02/08/2025` or `2025 Feb 08`, depending on which dcf version did the download, and a laptop and the build server differ. A single `as.Date(x, format = ...)` returns NA without an error (`571885ca`).
- Cue: `as.Date(`, `mdy(`, `ymd(`, `format =`.
- Question: does the script try all three formats, and does it stop when a date fails to parse?

**Markers coerced without being counted.** `suppressWarnings(as.numeric(x))` turns `NR`, `NReq`, `*`, `Suppressed` and `<10` into NA and hides how many there were.
- Cue: `suppressWarnings(as.numeric`, `na = c(`.
- Question: does the script list the markers it expects and stop on one it does not know (see `data/fluvaxview/ingest.R`)? Is a suppressed cell told apart from a not-reported one?

**The source renumbered or renamed its codes.** After CDC's 2025 YRBSS release most columns held another question's numbers (`d5715399`). Two Medicaid measures shared one abbreviation and the script kept the first row (`42f4929c`).
- Cue: lookups by position or by code (`Q12`, column indexes), `distinct(` or `slice(1)` after a filter on a short code.
- Question: are columns matched by a stable name, and is there a check that each code still means what the script assumes?

## Missing and suppressed values

**Missing turned into zero.** `replace_na(0)`, `if_else(is.na(x), 0, x)`, `coalesce(x, 0)` and `sum(na.rm = TRUE)` over an all-missing group publish "not reported" as a true zero. This has been sent back in review more than once: weeks with no reporting providers must be missing, not 0%.
- Cue: `replace_na`, `coalesce(`, `is.na(` inside `if_else`, `na.rm = TRUE` inside `summarize`.
- Question: is a zero here an observed zero? If not, leave it missing or flag it.

**Imputed values without a flag.** A suppressed count replaced with 5 (or any placeholder) needs a suppression flag column, and the flag has to survive into the bundle. Rates for suppressed cells are computed from the placeholder so plots keep working.
- Cue: `<- 5`, `== "Suppressed"`, `suppressed`.
- Question: is there a flag column, does `measure_info.json` say what it means, and does `build.R` carry it into `dist/`?

**Placeholder numbers.** Every reader of the file treats `-999` in a value column as a real number.
- Cue: `-999`, `9999`, `SENTINEL`.
- Question: why not NA?

**Comments that disagree with the code.** `data/nssp/ingest.R` imputes suppressed lab counts as 5, while a comment in `data/bundle_flu/build.R` says they are NA and not imputed.
- Question: for every comment about suppression or imputation near the diff, is it still true?

## Geography

**Name joins that drop or blank rows.** `resources/all_fips.csv.gz` has no `geography_name` for territories, so a join on the state name leaves Puerto Rico, Guam and the Virgin Islands with NA geography (`d88f350f`). A typo in a name does the same to one state.
- Cue: `left_join(` on a name column, followed or not by `filter(!is.na(geography))`.
- Question: does the script check for unmatched names and stop or report them, instead of dropping them silently?

**New York City reported separately.** Several CDC files list NYC apart from New York State, which undercounts the state (`03439229`).
- Cue: `New York City`, `NYC`.
- Question: is NYC added into the state total, and is the state flagged when either part is suppressed?

**FIPS codes read as numbers.** Leading zeros vanish (`6` instead of `"06"`).
- Cue: a read without `col_types` followed by a join on `geography`.
- Question: is geography read and kept as character?

**Retired and replaced counties.** Connecticut planning regions (`09110`-`09190`) against the old counties, Oglala Lakota County (`46102`, formerly `46113`).
- Question: which vintage does the source use, and does it match the population file it is joined to?

## Time

**Week dates.** MMWR week to date (`MMWRweek2Date(...) + 6`), `ceiling_date(x, "week", week_start = 7) - 1`, or `+ 6` on a week start. An off-by-one here shifts every series by a day or a week.
- Question: is the result a Saturday for every row, and does the script assert it?

**Future or impossible dates copied to every row.** One bad future date in the raw file became a zero-filled row for every county once `tidyr::complete()` ran (`c428e003`).
- Cue: `complete(`, `expand(`, `seq(min(`.
- Question: are dates validated before the grid is built?

**Code that depends on today's date.** `Sys.Date()` deciding which year or season to keep gives different output on different days. A `Sys.Date()` upper bound that only drops future dates is fine.

## Reshaping and joining

**Duplicates before a pivot.** A duplicate row made `reshape2::dcast` return a row count instead of the value in every cell (`f2ae2383`). `pivot_wider` makes list columns in the same situation.
- Cue: `dcast(`, `pivot_wider(`, `values_fn`, `fun.aggregate`.
- Question: is the key checked for uniqueness first (a `check_unique()` or `stopifnot(!anyDuplicated(...))`), and if duplicates are dropped, is it confirmed they are true duplicates and not rows that differ in a column that was thrown away?

**`full_join` where `left_join` was meant**, or the reverse (`b353da4e`). Rows appear with missing keys, or rows disappear.
- Question: what should happen to rows present on one side only?

**Totals counted twice.** A file that holds both the breakdown and its total, summed without filtering, doubles the national figure.
- Cue: `summarize(` or `sum(` over a column that includes `Total`, `Overall` or `United States`.

**Weighted averages.** National values built from states should weight only the states that report, and should not treat a missing population as weight 1 (`b0089348`).

## Scale and units

**Percent stored as a proportion**, or multiplied by 100 twice. CHR 2013 infant mortality was off by a factor of 100 (`a9892998`).
- Cue: `* 100`, `/ 100`, `rate`, `pct`.
- Question: does the range of the output match the `unit` in `measure_info.json`?

**A changed scale in an existing column.** Readers of the dist file are not told. This needs a change log entry.

## Run tracking

**State recorded before all outputs are written.** `data/gtrends/ingest.R` records `raw_state` and then writes the DMA files. If that part fails, the next run sees no change and skips it.
- Cue: `dcf::dcf_process_record(updated = process)` with write calls after it.

**A failed download hidden.** In the pull scripts a `tryCatch` turns a failed download into a message, the old file stays in place, and the run is recorded as fine.
- Cue: `tryCatch(` around `download.file(`.
- Question: after a failure, can stale data be published as current?

**Downloading straight onto the published file.** `data/county_health_rankings/ingest.R` downloaded into `standard/` directly. A local build stopped mid-transfer left the first 1.5 MB of a 21 MB `data_county.csv.gz`, which vroom read as 9,661 rows instead of 50,270 with only a parsing warning, and it was committed in #306. A failed download (HTTP 404) deletes the existing file the same way. `dcf_download_cdc` likewise leaves a partial `raw/<id>.csv` behind when it fails (#306 committed a 56 MB BRFSS one).
- Cue: `download.file(url, dest` or `curl_download(` where `dest` is in `standard/` or is the file the script later reads.
- Question: does the download go to a temporary `.part` file that is renamed into place only after it finishes (`tmp <- tempfile("download-", tmpdir = "standard", fileext = ".part")`, then `file.rename(tmp, dest)`, with `unlink(tmp)` in `finally`), and is the size or row count checked before the state is recorded?

**A failed or killed local run committed.** A `process.json` whose `last_status.success` is false, logs with `C:/Users/...` paths, state keys that are absolute paths (dcf run from another directory), and hashes taken on a CRLF checkout all mean the PR carries local run state that CI will never match. If many unrelated folders changed, the author probably ran a full `dcf_build()` locally.
- Question: was every non-code file in the diff meant to be there? Restore anything else from the base and leave regeneration to CI.

**Code changed but the data did not.** An ingest skips processing when the raw data is unchanged, so a PR that edits `ingest.R` and shows no change in `standard/` was probably not re-run. For a bundle, `build.R` changes do not trigger a rebuild at all: it needs `dcf::dcf_process("<bundle>", clear_state = TRUE)`.

**Bundle `source_files` not updated.** dcf never derives it from `build.R`. A file that `build.R` reads but `process.json` does not list will not trigger a rebuild when it changes.

## Left in by accident

- Commented-out code. In `data/nssp/ingest.R` a commented-out `bind_rows` leaves the national lab rows out of `data_lab.csv.gz` (since `0650b7c2`), and nothing in the file says whether that is intended.
- Hard-coded years, cut-off dates, coefficients and caps. Ask whether they can be read from the data or the metadata, and whether older years should be ingested too.
- `setwd()`, absolute paths, `View()`, `browser()`, package installs.
- Changes to files outside the source or bundle the PR is about.
