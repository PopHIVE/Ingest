# SOP checklist

How each item of the team's "Quality control on new data" SOP is checked, what the check IDs in `review_checks.R` mean, and the conventions they rely on.

## SOP coverage table

Use these rows, in this order, for the coverage table in the report. "Who decides" is the script, the skill (whoever runs this review), the author of the change, a person looking with their own eyes, or the team.

| # | SOP item | Checks | What to do | Who decides |
|---|---|---|---|---|
| 1 | Added with dcf, folder has `ingest.R`, `process.json`, `measure_info.json` | S01, S03 | Confirm the folder was made with `dcf::dcf_add_source()` or `dcf_add_bundle()`, one script per folder | script |
| 2 | Code runs with no warnings or errors | S02, S04, B11, `check_data_changes.py` (failed-run, archive, raw-file) | Read `last_status` in `process.json`. The ingest is not re-run here, so ask the author whether they ran it line by line. A code change with no change in `standard/` or `dist/` means it was not re-run | author |
| 3 | Missing and suppressed counts match between raw and standard | R01, R02, R04 | Reconcile the tallies with the recipe below | skill |
| 4 | All dates present for all strata, imputed values carry a flag | D06, D07 | Confirm gaps are real (seasonal, source started later). Any imputed value needs a suppression flag column | skill |
| 5 | Age categories fit the project | D09 | Read the levels against CLAUDE.md. To see what the bundle uses, run the script on the bundle with `--base none` and read its `B00` and `B07` lines | person |
| 6 | Dates: weekly Saturday, monthly, yearly | D03, D04 | See Conventions | script, team for conflicts |
| 7 | Other categories (race/ethnicity, income) consistent with other datasets | D09 | Read the levels against Conventions and against the bundle, as for row 5. `D09` lists only the usual demographic columns, so read other category columns from the file | person |
| 8 | Percentages per 100, not per 1 | M08 | Confirm against the raw values | script |
| 9 | Plots: gaps, outliers, jumps | D06, `render_plots.R` | Hand over the html paths and the `HUMAN D06` lines. Do not interpret the plots | person |
| 10 | `measure_info.json`: descriptions, units, source, restrictions | M01-M05, M08, M10 | Read every new or changed entry and trace each statement to the source's own documentation (step 8 of the skill). A statement that cannot be traced is a finding. The item cannot pass on plausible wording alone | skill, person for wording |
| 11 | `process.json` properly formatted | S01, S02, S04, S09, `check_data_changes.py` (local-path, crlf-hash) | | script |
| 12 | First `_sources` description covers the whole source | M06 | Read it | skill |
| 13 | Large raw data or proprietary data kept out of Ingest | S06, `check_data_changes.py` (file-size, raw-file) | The script warns at 50 MB of raw data in a folder, or 10 MB added. Ask about redistribution rights when raw files are added | person |
| 14 | Branch and PR, squash merge, branch deleted | steps 2 and 4 of the skill | A PR number means the work is on a branch. With no PR, check the current branch is not main. Remind the author to squash and delete the branch | person |
| 15 | General logic reviewed | step 5 of the skill, `r-ingest-bug-patterns.md` | Read the changed code | skill |
| 16 | Datasets feeding pophive.org checked | C07, C01-C05 on `dist/` | Any `dist/` change, and any source listed in a bundle's `source_files`, feeds the site. Review the bundle too. Remind the author to look at the site after the merge | skill, author |
| 17 | Change log updated after a major fix | C05, C08 | If published values changed, the README needs an entry (format below) | person |
| 18 | Bundle in the format the MCP server expects | B01-B10 | See MCP bundle rules | script, MCP team |
| 19 | CLAUDE.md format rules (not in the SOP) | S05, S08, M07, M09, D01, D02, D05, D08, D10, D11 | | script |

## Check IDs

Numbers are not continuous. There is no S07 and no R03.

Structure and run state

| ID | Raised when |
|---|---|
| S01 | folder not found, `process.json` missing or unparseable, `name`/`type`/`scripts` do not match the folder, script missing, extra R scripts. Also marks a pull script |
| S02 | last recorded run failed, or its log has warnings or errors |
| S03 | no `dcf_process_record()`, state never written back, or state written before the last output |
| S04 | md5 in `process.json` differs from a data file (changed without a recorded run), `measure_info.json` edited after the last run (INFO), or `ingest.R` changed while every standard file is identical. For bundles, also lists source files changed since the last build |
| S05 | file is not `.csv.gz`, not really gzip, gzip stream truncated or corrupt, parquet footer broken, tab-delimited, has no rows, cannot be read, or the folder has no data files |
| S06 | raw data size (warns at 50 MB in the folder or 10 MB added), raw files added |
| S08 | conflict or duplicate copies (`file 2.csv.gz`, `(1)`, `.orig`) |
| S09 | absolute paths in the recorded state |
| X00 | a check stopped with an R error (exit status 3). Its remaining checks did not run, so say so in the report |

Metadata

| ID | Raised when |
|---|---|
| M01 | `measure_info.json` missing, unparseable or empty |
| M02 | a column has no entry. Names are matched as `dcf_check` does (column, `file\|column` or `source_id`). The standard columns and, in sources, flag columns are exempt |
| M03 | an entry matches no column |
| M04 | a value entry lacks `short_name`, `measure_type` or `unit` |
| M05 | a `_sources` entry lacks name, url, organization, organization_url, description or restrictions |
| M06 | prints the first `_sources` description for reading. WARN when it is under 150 characters and there are several sources |
| M07 | `_catalog` missing, summary empty, `files` keys do not match `standard/`, placeholder blurbs |
| M08 | percent column with every value at or below 1, or values above 100. `SKIP` lists value columns with no unit, whose scale is checked only when the name says `pct` or `percent` |
| M09 | negative values |
| M10 | `unit` or `measure_type` changed against base, restrictions text changed |

Data (standard files)

| ID | Raised when |
|---|---|
| D00 | context: how columns were classified, value ranges, row limit reached. Its `PASS` line lists the checks that raised nothing on the file |
| D01 | no `geography` or no time column |
| D02 | geography that is not `00`, 2 digits or 5 digits (region files may hold `hhs_N`, substate files text codes), codes not in `resources/all_fips.csv.gz` |
| D03 | time not `YYYY-mm-dd`, a date that does not exist, missing, in the future, or the latest date is earlier than on base |
| D04 | weekly dates not Saturday, monthly dates on mixed days or not on the CLAUDE.md day, annual dates not `YYYY-12-31` |
| D05 | duplicate rows on geography + time + strata, or rows unique only when a numeric column is read as a stratifier. Also raised on dist files |
| D06 | dates missing inside a series, runs of missing values, one-point jumps of more than tenfold (national and state series). Values constant for many periods are INFO. Says which descriptive columns were left out of the series key. Skipped when the file has duplicate rows |
| D07 | suppression flags and what the flagged cells hold, suppressed cells stored as 0, placeholder numbers such as -999, counts of exactly 5 with no flag, one exact value filling far more cells than any other, a flag column dropped while its cells were filled |
| D08 | columns entirely missing, value columns identical to each other |
| D09 | levels of age, sex, race/ethnicity and similar columns, `Total` and `Overall` in one column |
| D10 | text, thousands separators or percent signs in a numeric column, text that is not valid UTF-8 |
| D11 | column names not lowercase with underscores |

Comparison with base

| ID | Raised when |
|---|---|
| C00 | context: rows and columns against base, new file |
| C01 | file or folder deleted. For a deleted folder, says whether a bundle still lists or reads its files |
| C02 | columns dropped, added, changed type, or numeric on base and text now |
| C03 | category levels or geographies dropped (added ones are INFO) |
| C04 | rows on base no longer present, or every date relabelled. Rows added, and rows that only rolled off the start or the most recent periods, are INFO |
| C05 | values on shared rows changed: rescaled, a column emptied, a series rewritten, flags flipped, values now missing, changes outside the most recent periods. WARN only when code changed in the same diff or values were rescaled or emptied. Otherwise INFO, "revised by the source" |
| C06 | rows not unique on the key, so values were not compared |
| C07 | lists the bundles that read this source, and says when one was not rebuilt |
| C08 | published values, rows or levels changed, a change log entry may be needed |

Raw against standard

| ID | Raised when |
|---|---|
| R01 | tallies of missing cells, non-numeric markers and flags on both sides |
| R02 | raw has suppression or not-reported markers and standard has no flag column, or the markers are carried into the standard file as text |
| R04 | raw numbers carry thousands separators and the script has no number parser |

Bundles

| ID | Raised when |
|---|---|
| B00 | context: schema of the dist file, value range by measure. Its `PASS` line lists the checks that raised nothing on the file |
| B01 | a file in `dist/` is not parquet |
| B02 | file has no rows, or a column name the MCP server rejects |
| B03 | no time column, dates stored as text, cadence not detectable |
| B04 | no geography column, missing geography, names with no FIPS column, or FIPS stored as a number |
| B05 | no `source` column, or a source label the MCP server rejects |
| B06 | no numeric `value`, or extra numeric columns (wide file) |
| B07 | a stratifier column with no all-groups row, or more than 60 levels |
| B08 | no measure column, measure values with no level in `measure_info.json`, levels without `measure_type` and `unit` |
| B09 | `build.R` reads a file not in `source_files`, listed files not read or missing, raw data read in a bundle |
| B10 | the `_bundle` block of the bundle's `measure_info.json` is missing, or its `dist_files` list does not match `dist/` |
| B11 | `build.R` changed but every dist file is identical to base |

## Conventions

The script reads the monthly and annual rules from the Time Standards section of `CLAUDE.md`, so a change there changes the check. The Saturday rule is fixed in the script.

- Weekly: Saturday at the end of the week.
- Monthly: last day of the month.
- Annual: `YYYY-12-31`. School-year files use `YYYY-09-01`. The script accepts that (`INFO D04`) when the file has a `grade` column or the folder name contains `school` or `exemption`.
- Geography: `"00"` national, 2-digit state, 5-digit county, as strings. HHS regions go in a `data_region` file as `hhs_1` to `hhs_10`, substate areas in a `data_substate` file.
- Percent: 0 to 100.
- Suppressed counts: either left missing, or replaced with a placeholder of 5 with a suppression flag column equal to 1. Never 0. Rates for suppressed cells are computed from the placeholder (5 / population) so plots do not break, and the flag marks them. If a column has no missing or suppressed values it does not need a flag.
- All-groups label: CLAUDE.md says `Overall`. Many existing files use `Total` for age. A new source should use `Overall` unless the bundle it feeds already uses `Total`. One file must not use both.
- Age bands: CLAUDE.md lists `0-4`, `5-17`, `18-49`, `50-64`, `65+`. Sources often keep their own bands. Check that bands do not overlap, that the same band is not written two ways (`65+`, `65+ Years`, `>=65 Years`), and that the format matches the bundle the source feeds.
- Race and ethnicity: CLAUDE.md lists `White`, `Black`, `Hispanic`, `Asian`, `Overall`. Check new labels against the files the source will be combined with.

Where the documents disagree, raise a question and name both sides. Do not settle it in the review. "Guardrail rule" means a rule of `.claude/skills/pophive-code-review-SKILL.md`.

| Topic | SOP | CLAUDE.md and the repo |
|---|---|---|
| Monthly date | first day of the month | last day (most files). `nchs_mortality`, `epic_injury` and `epic_resp_infections` use the first, and `nis` mixes both |
| Yearly date | not filled in | `YYYY-12-31`. Some older files use `01-01` |
| Suppressed values | placeholder of 5 with a flag | the Epic example in CLAUDE.md imputes half the minimum. The files use 5 |
| All-groups label | not stated | `Overall` in CLAUDE.md, `Total` in many age columns |
| Bundle time column | not stated | `date`, `week_end` or an integer `year` in older bundles, `time` in recent ones |
| New source location | inside Ingest with dcf | guardrail rule 3 says one repository per source |
| Raw files | commit unless large or proprietary | guardrail rule 10 says never commit them |
| Bundle shape | not stated | guardrail rules 5 and 11 and the MCP server expect long, the `create-bundle` skill allows wide |

## Reconciling raw and standard counts (SOP item 3)

The script prints tallies for both sides but cannot assert they match, because each ingest filters rows differently. To reconcile, apply the filters from `ingest.R` to the raw file and count again. Feed this to `Rscript -` on stdin and adapt the filter:

```r
raw <- vroom::vroom("data/<source>/raw/<id>.csv.xz", col_types = vroom::cols(.default = "c"),
                    altrep = FALSE, show_col_types = FALSE)
raw <- raw[raw$Type == "Unadjusted Rate" & raw$Sex == "Overall", ]   # same filters as ingest.R
x <- raw[["<raw value column>"]]
is_marker <- !is.na(x) & is.na(suppressWarnings(as.numeric(gsub(",", "", x))))
cat("raw rows", nrow(raw), "missing", sum(is.na(x)), "markers", sum(is_marker), "\n")
print(table(x[is_marker]))

std <- vroom::vroom("data/<source>/standard/data.csv.gz", col_types = vroom::cols(.default = "c"),
                    altrep = FALSE, show_col_types = FALSE)
v <- std[["<value column>"]]
f <- std[["suppressed_flag"]] %in% "1"
left_as_text <- !is.na(v) & is.na(suppressWarnings(as.numeric(v)))
cat("standard rows", nrow(std), "missing", sum(is.na(v)), "flagged", sum(f),
    "missing or flagged", sum(is.na(v) | f), "markers left as text", sum(left_as_text), "\n")
```

The item passes when raw missing cells plus raw markers equal the standard cells that are missing or flagged, counting a cell once when it is both, and no marker has turned into 0 or stayed in the file as text. If rows were dropped or aggregated, account for them before comparing.

## Feeds pophive.org (SOP item 16)

The site reads `data/bundle_*/dist/`. Treat as feeding the site:

- every change under a bundle's `dist/`
- every source file listed in some bundle's `process.json` `source_files` (the script lists these as `C07`)

The `bundle_youth_wellbeing` dist files also feed the youth wellbeing dashboard on the site through their `section` and `focus_area` columns, so new values there need a note to the website team.

## Change log (SOP item 17)

Entries go at the end of the `# Major change log` section of `README.md`. The section sits in the middle of the file and ends where "Guide to adding data and rebuilding the bundle" starts. Format: a bold date line (`**November 13, 2026**`), then a short paragraph saying what changed, why, and how much (which files, how many rows or what size of difference).

An entry is needed when values already published change for a reason other than routine revision by the source: a parsing fix, a changed definition, a rescale, dropped strata.

## MCP bundle rules (SOP item 18)

The MCP repository has no written checklist. These rules come from `verify/onboarding.py` (`draft_file`) and `docs/upstream_data_issues.md` in `DISSC-yale/sp-pophive-mcp`, at commit `12a610b` (October 2026). The MCP server can add a new dist file on its own only when:

- the path uses letters, digits, `_`, `.` and `-`, column names start with a letter and use letters, digits and `_` (64 characters at most), and the file has rows (B01, B02)
- there is a `time`, `date`, `week_end` or `year` column, and the most common gap between dates is daily, weekly, monthly, quarterly or annual (B03). A file with a single date fails, and so does one whose most common gap is outside those bands (YRBSS, every two years)
- there is a `geography`, `fips` or `geography_fips` column with FIPS codes, state names or state abbreviations. County names with no FIPS column fail, and so does a file with only national rows (B04). Missing geography does not stop the server, but it is a known data issue, so the script fails it
- there is a `source` column whose labels are 90 characters or fewer and use only letters, digits, spaces and `, . % ( ) / + & ' - :`. An underscore or `*` fails. So do a URL, `@`, a bracket or brace, the words `system`, `prompt`, `instruction`, `ignore` or `override`, and phrases such as "the official", "the primary", "the standard", "authoritative", "gold standard" or "definitive" (B05)
- there is one numeric `value` column. Other numeric columns are limited to `value_lcl`/`value_ucl` (or `lcl`/`ucl`), `sample_size` or `n`, `suppressed_flag` or `suppressed`, and `is_state_estimate`. Anything else named `value*`, `pct_*`, `rate_*`, `n_*` or `percent_*` makes the file wide (B06). A `stratum_type` + `stratum` pair is not accepted
- every stratifier column with more than one level (age, sex, race, ethnicity, vaccine, dose, grade, insurance, urban, payer, cause, type and similar) has 60 levels or fewer and contains one of `Total`, `Overall`, `All`, `All ages`, `All Ages`, `Both sexes`, `All races` (B07)
- the file has a `measure`, `outcome_name` or `condition` column, and its values have a level in the bundle `measure_info.json` with `measure_type` or `unit` written on the level itself. The server does not follow `source_id`. A file with none of these columns is not added, whatever the `value` entry says. A value with no units is left out, and the file fails only when no value, or no value of one source, has units. Recognised: Percent or `%`, a unit with `per <number>`, Count, cases, deaths, births, Cumulative Count, `Proportion (0-1)`. A `measure_type` of Rate with no `per <number>` in the unit is not recognised (B08). The script asks for both `measure_type` and `unit` on each level, which is this repo's own rule
- the file is listed in `resources/data_manifest.json`, which `scripts/build_docs.R` regenerates

Files the server already serves are configured by hand on its side, so a `B` finding on an existing file is for information.

What must not happen to an existing dist file (each has broken the server before):

- deleting, renaming or moving it
- renaming, merging or dropping columns
- dropping category levels, age bands or the national rows
- changing the stored scale (0-1 to 0-100) without a change log entry

The MCP team asks only for additions: a `source` column, all-groups rows, units on levels, and a complete `dist_files` list in the `_bundle` block. Do not rename or reshape a file to satisfy the MCP.

When a source or dist file is removed or renamed on purpose, the MCP repository's index needs a matching change. Say so in the report.

Optional local probe, only when the MCP repository is cloned next to Ingest with its `.venv`, the reviewed version is the working tree, and the user agrees. It may fetch from GitHub and writes a cache under `~/.cache/pophive-mcp`. A file not yet in `resources/data_manifest.json` is reported as deferred.

```bash
cd ../sp-pophive-mcp && POPHIVE_DATA_DIR="$OLDPWD/data" .venv/bin/python -I -B - <<'EOF'
import json, os, sys
sys.path[:0] = ["verify", "src"]
from pathlib import Path
import onboarding as o
from pophive_mcp.catalog import load_catalog
from pophive_mcp.aliases import alias_index
D = Path(os.environ["POPHIVE_DATA_DIR"])
rel = "bundle_<name>/dist/<file>.parquet"
manifest = json.loads((D.parent / "resources/data_manifest.json").read_text())
r = o.draft_file(rel, o.profile_parquet(D / rel), manifest, load_catalog(), alias_index(), bundle_is_new=False)
print(getattr(r, "reason", r))
EOF
```

## Merging (SOP item 14)

Work happens on a branch and goes through a PR. A branch with several commits is squash-merged, and the branch is deleted after the merge. The review can only check the first part. Remind the author of the rest.

## Tools that do not help here

- `scripts/validate_standard.R` expects `MM-DD-YYYY` dates, so its time check fails every correct file and its Saturday check never runs. Its value-column and duplicate checks assume columns named `value` and a fixed key. Do not use it for the review.
- `scripts/validate_dataset.Rmd` is good for plots. Its own git comparison fails on macOS and Linux (the `|` in its `git log --format` call is read as a shell pipe), and where it runs it compares with the file's previous commit, not with the merge-base. `render_plots.R` extracts the base file and passes it as `old_data_file`.
