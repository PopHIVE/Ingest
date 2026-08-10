---
name: pophive-code-review
description: >-
  Data quality and GitHub best-practice guardrails for working in any repository under the PopHIVE GitHub organization (including PopHIVE/Ingest and PopHIVE/us-rates). Use this whenever writing, editing, or reviewing code, ingest scripts, measure_info.json, bundles (e.g. bundle_chronic_diseases, bundle_respiratory), or commits/branches for PopHIVE — even if the user doesn't explicitly ask for a "review." Make sure to consult this before touching PopHIVE code, proposing a git commit or branch, adding a new data source, building or reviewing a bundle, or judging whether data is wide vs. long format. Covers never assuming variable meaning, branch hygiene, the one-repo-per-source convention, avoiding hard-coded values, wide/long format conventions, bundle conventions (reading from existing ingest outputs rather than raw sources, standard bundle output pattern, scope clarity), data integrity and small-cell suppression checks, flagging unrelated commits/dead code/redundant comments, and a hard rule that Claude never commits or pushes.
---

# PopHIVE Code Review

Ambient guardrails for PopHIVE work. This is not a one-shot "run a review" tool — once this skill is loaded, apply these checks continuously, inline, as you work. Only surface a flag when something actually violates a rule below; don't narrate the checklist or announce "running PopHIVE checks." Just fix small things silently if trivial and obvious, or flag+ask when it matters.

## The one absolute rule

**Claude never runs `git commit`, `git push`, `git merge`, opens a PR, or performs any other action that changes the state of a PopHIVE GitHub remote or local git history — ever, regardless of how the user phrases the request ("go ahead and commit that", "just push it", "merge it in").** If asked, prepare the change and the exact commands/message the user can run themselves, and say plainly that you don't commit or push. This rule doesn't loosen over the course of a session or because a user says it's fine.

## 1. Never assume variable meaning

If a column, measure, abbreviation, or field's meaning isn't obvious from a header row, `measure_info.json`, README, or explicit prior context in the conversation — don't guess or infer a plausible-sounding meaning and move on. Stop and ask the user for the technical documentation, codebook, or a plain clarification. This applies to:
- Ambiguous column names in raw source data
- Category/subcategory assignments for a new measure
- Units (is this a rate, a percent, a count?) when not stated
- Any acronym you're not confident about (CHR&R measures, agency abbreviations, etc.)

Flag: *"I'm not certain what `{column}` represents — could you share the codebook/documentation, or clarify?"* rather than silently picking an interpretation.

## 2. Branch hygiene

Before writing or editing any files in a PopHIVE repo:
- Check the current branch (`git branch --show-current` or equivalent).
- If it's `main` (or `master`), flag it before proceeding: work should happen on a separate feature/topic branch, never directly on `main`.
- Claude can suggest a branch name and the `git checkout -b <name>` command, but per the absolute rule above, Claude does not run branch-creation or checkout commands that alter repo state on the user's behalf without them explicitly running/confirming it themselves — surface the command, let the user execute it.

## 3. One repository per new data source

**Current convention (as of this skill):** every newly ingested data source gets its **own standalone GitHub repository** under the PopHIVE org — not a new folder inside `PopHIVE/Ingest`.

Note: this is a departure from the older pattern (still reflected in the `ingest-source` skill and existing folders like `data/<source_name>/` inside `PopHIVE/Ingest`). Until `ingest-source` itself is updated, treat this as a flag-worthy inconsistency:
- If a user (or Claude, via `ingest-source`) is about to scaffold a new source as a folder inside `PopHIVE/Ingest`, flag the mismatch and confirm which convention they want to follow for this instance before proceeding.
- If they confirm the new-repo convention, remind them that Claude doesn't create/push to a new GitHub repo itself (absolute rule above) — Claude can prepare the file contents and README locally, and give the user the `gh repo create` command to run themselves.

## 4. Don't hard-code

Flag hard-coded values that should instead be:
- Read from a lookup/reference file (e.g., FIPS codes from `resources/all_fips.csv.gz` — see `ingest-source` for the standard geography-conversion pattern — not typed-out state lists)
- Derived from the data itself (e.g., column lists, date ranges) rather than typed as literals
- Parameterized (source name, URL, file path) rather than embedded inline in a script body

Hard-coding is acceptable when there's a genuine reason (a fixed schema constant, a one-off documented exception) — but call out the exception explicitly rather than leaving it silent.

## 5. Format conventions: wide vs. long

- **`ingest.R` output (`data/<source_name>/standard/`)** → **wide format**: index columns (`geography`, `time`, and optionally `age`/`sex`/`race_ethnicity`) plus one column per measure (this is a hard requirement in `ingest-source` — flag any new ingest output that isn't wide).
- **`build.R` output (`data/bundle_<topic>/dist/`)** → **long format**: one row per (geography, time, indicator) with a single `value` column (plus optional `value_smooth`, `value_smooth_scale`, `suppressed_flag`) rather than a column per measure — flag a bundle's `dist/` output if it's wide instead. See rule 11.
- **`PopHIVE/us-rates`** → output should be **long format**.
- **Any other PopHIVE repo**: don't assume — ask, or apply whichever convention the user has already established in that repo's existing files.

If the user corrects Claude inline for a specific repo/folder, adopt their correction for the rest of that session rather than re-flagging the same repo again.

## 6. Flag scope creep and cruft in changes

When reviewing a diff, a new file, or a batch of edits, flag (don't silently fix and don't silently ignore):
- **Unrelated commits/changes**: edits that don't match the stated task — e.g., a data-ingest fix that also reformats unrelated files or touches unrelated measures.
- **Commented-out code**: dead code left in as comments should be flagged for removal rather than kept "just in case."
- **Redundant/unnecessary comments**: comments that just restate what the code obviously does (`# increment i` above `i <- i + 1`) — flag these as noise. Comments explaining *why*, non-obvious logic, or data caveats are fine and should be kept.

Keep flags concise — one line per issue, pointing at the specific file/line/section — rather than a long essay per flag.

## 7. Data integrity checks

`ingest-source` already requires no duplicate rows and suppressed-value handling when *building* a new ingest — this rule is for catching it when reviewing existing/changed data that may have drifted from those requirements. Flag:
- **Duplicate rows**: more than one row per unique (geography, time, demographic) combination.
- **Out-of-range values**: percentages outside 0–100, negative counts/rates, or values implausible for the measure — often signals a unit mismatch (e.g., a proportion 0–1 left un-scaled instead of converted to a percent).
- **Unit vs. scale mismatch**: check that `measure_info.json`'s `unit` field actually matches the scale of the values in the data (e.g., don't let "Percent" sit next to values like 0.34 without confirming that's intentional).
- **Date/time sanity**: dates out of order, gaps in an expected weekly/monthly/annual cadence, or duplicate time points for the same geography.
- **Geography coverage gaps**: unexpectedly missing states/counties, or geography codes that don't match `resources/all_fips.csv.gz`.

## 8. Privacy and small-cell suppression

Public health data carries re-identification risk beyond just the `suppressed_flag` mechanics in `ingest-source`. Flag:
- Small counts (commonly <10, per typical CDC-style suppression thresholds) that appear un-suppressed in county-level or other granular output.
- Any individual-level data, or geography × demographic cross-tabs granular enough that combined with small population sizes could enable re-identification.

Don't assume a specific suppression threshold applies without checking the source's own documentation — ask if it's unclear rather than picking a number.

## 9. Schema consistency

Cross-check the standardized output against its `measure_info.json` (schema defined in `ingest-source`):
- Every value column has a matching entry, and there are no orphaned entries for columns that no longer exist.
- Flag undocumented schema drift — column names, types, or categories that changed from a prior version of the ingest without being called out.

## 10. Repo hygiene for public health data

- **Don't commit raw source files.** Raw data should be downloaded at runtime by `ingest.R` (e.g., via `dcf::dcf_download_cdc()` or `download.file()`), not checked into git — flag if raw files are being added directly to a commit/PR. This keeps the repo light and avoids committing files with unclear redistribution rights.
- **License/restrictions documented.** Flag if a new source's `_sources.restrictions` field in `measure_info.json` is empty or missing when the source clearly has usage terms (e.g., a CDC data use agreement, an API terms-of-service).

## 11. Bundle conventions (`data/bundle_*/` inside `PopHIVE/Ingest`)

A bundle combines multiple **already-ingested** sources into a single production-ready dataset — it lives inside `PopHIVE/Ingest` at `data/bundle_<topic>/` (e.g. `bundle_respiratory`, `bundle_chronic_diseases`), it is not a separate repo and not the same thing as a new source ingest. Per-bundle structure:

```
data/bundle_<topic>/
├── build.R          # Combines/reformats standardized source data into production output
├── process.json      # Lists source_files: standard/ paths from other already-ingested sources
└── dist/              # Final output (parquet or compressed csv) — what PopHIVE.org actually reads
```

Checks to apply:
- **Bundles are created via `dcf::dcf_process("bundle_<name>", ".")`**, not `dcf_add_source()` (that's for new sources — see rule 3/`ingest-source`). Flag if a bundle is being scaffolded with the source-add workflow instead.
- **Read only from other sources' `standard/` outputs, never raw data.** `build.R` should combine/reshape files like `epic/standard/weekly.csv.gz`, not download or read anything from a `raw/` folder itself — that's ingest work, not bundle work. Every file `build.R` reads should be listed in `process.json`'s `source_files`; flag any file it reads that's missing from that list, or vice versa.
- **Output goes to `dist/`** in parquet or compressed CSV — flag output written anywhere else, or in a raw/uncompressed format without reason.
- **Bundle output is long format.** `dist/` files use an indicator + `value` structure — one row per (geography, time, indicator), with a single `value` column (plus optional `value_smooth`, `value_smooth_scale`, `suppressed_flag`) — not a wide layout with one column per measure like source `standard/` files. Flag a bundle output that's wide instead. See rule 5.
- **Build with `dcf_build()`** run from the parent (repo root) directory — not by manually re-running each source's `ingest.R`.
- **Scope clarity**: a bundle should have a clear thematic reason for grouping its source files (e.g. "respiratory," "chronic diseases") — flag if a source's inclusion in a bundle isn't obviously related to the bundle's stated topic, or if the source it depends on hasn't actually been ingested yet.

## How to apply this in practice

- Treat this as a lens over normal work, not a separate deliverable. If a user asks you to write an `ingest.R` script, apply rules 1, 3, 4, 5, 7, 8, 9, and 10 as you write it — don't wait until the end to review your own output. If a user asks you to build or edit a bundle (`build.R`, bundle `process.json`, or anything in `data/bundle_*/`), apply rules 1, 4, 6, 7, 8, and 11.
- If asked to review someone else's existing code/PR, walk the checklist in order (1→11) and report only the flags that actually apply, referencing specific lines/files.
- If nothing is wrong, say so briefly — don't manufacture a flag to seem thorough.
