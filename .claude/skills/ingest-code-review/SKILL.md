---
name: ingest-code-review
description: QC review of a PopHIVE/Ingest pull request, branch or data folder against the team's quality control SOP before it is merged to main. Use when asked to QC or review an ingest PR or branch against the SOP, to check whether a source folder under data/ or a bundle is ready to merge, or to run the SOP checks on a new or changed data source. Reads the diff, runs read-only checks on the standard, raw and dist files, compares them with main, and renders the validation plots outside the repo. It does not re-run ingests and does not commit, push or post anything.
argument-hint: "[PR number | branch | data/<dir>]"
---

# ingest-code-review

Review a pull request, a branch or a `data/<dir>` folder against the QC SOP. Main is production and public, and no CI runs on pull requests, so nothing checks a PR automatically before it is merged.

```
/ingest-code-review [PR number | branch | data/<dir>]
```

With no argument, review the current branch against main.

In this file "the user" is whoever asked for the review, "the author" is whoever wrote the change, and "a person" means someone has to look with their own eyes.

## Rules

- This review is read-only. Do not run `ingest.R`, `build.R`, `dcf::dcf_process()`, `dcf::dcf_check()` (it rewrites `process.json`), `dcf::dcf_build()` or `scripts/build_docs.R`.
- Do not checkout, switch, stash, commit or push, and do not post a comment or review on the PR. Do not write any file inside the repository.
- If the user asks for a fix after the review, that is a new task. The review itself changes nothing.

## Steps

### 1. Read the conventions

- Run `git status --porcelain` and keep the output. The last step compares against it.
- Read only these sections of `CLAUDE.md`: Standard Data Format Specification (columns, geography, time), `_sources Field Requirements`, and Bundle measure_info.json Schema if a bundle is touched.
- Read [references/sop-checklist.md](references/sop-checklist.md).
- Read `.claude/skills/pophive-code-review-SKILL.md` (the guardrail skill) and apply its general coding rules: never assume what a variable means, no hard-coding, scope creep, small-cell suppression. Do not repeat it in the report. It disagrees with the SOP or with repo practice on three points: one repository per source, never committing raw files, and bundles always long. The checklist names both sides of each. Report them as questions.

### 2. Resolve what is being reviewed

| Argument | Reviewed version | Compared with |
|---|---|---|
| none, on a branch | working tree | `git merge-base origin/main HEAD` |
| none, on main, tree has changes | working tree | `HEAD` |
| none, on main, clean tree | stop: list open PRs with `gh pr list` and ask which one | |
| PR number | see below | merge-base of `origin/main` and the PR head |
| branch name | that branch (`--head <branch>`) unless it is checked out | merge-base with `origin/main` |
| `data/<dir>` | working tree, folder audit with `--base none`. Skip steps 3 and 4 | nothing |

Compare with the merge-base. Bots commit data to main every day, and a comparison with the tip of main would show their changes as if the PR had made them.

For a PR number, run `gh pr view <n> --json title,body,state,baseRefName,headRefName,headRefOid,mergeCommit,commits,files` (if that fails, `gh api repos/PopHIVE/Ingest/pulls/<n>`). Then:

1. If the PR is open and its head commit is the current `HEAD` with a clean tree, review the working tree.
2. If the PR is open and `git cat-file -e <headRefOid>^{commit}` succeeds, review that commit with `--head <headRefOid>`. The script reads the files straight from git.
3. If the PR is merged, say so. Review the merge commit against its parent: `--head <mergeCommit.oid> --base <mergeCommit.oid>^`. A commit that is already on main must not be passed as `--head` with the default base, because the base is then the same commit and nothing is compared.
4. If the commit is not in the local repository, ask before running `git fetch origin pull/<n>/head`, then pass `--head FETCH_HEAD`. If the answer is no, review the diff only (`gh pr diff <n>`) and mark the data checks "not run".

If the PR does not target main, say so.

If local main has commits that are not on `origin/main` (`git rev-list --count origin/main..main`), say so first: the SOP requires a branch and a PR.

Print the date of `origin/main` (`git log -1 --format='%h %cd' origin/main`). If it is more than a day old, say the comparison may be stale. Do not fetch without asking.

### 3. Scope the change

- Working tree: `git diff --name-status <base>` plus `git ls-files --others --exclude-standard data/`.
- Commit or branch: `git diff --name-status <base>...<head>`.

Group the paths: code (`ingest.R`, `build.R`), metadata (`measure_info.json`, `process.json`), `standard/`, `dist/`, `raw/`, generated (`datapackage.json`, `docs/`, `resources/*.json`, `file_log.json`, `report.json.gz`, `status.md`), and anything else. The folders to review are every touched `data/<dir>`, including the ones unrelated to the stated change. Do not narrow the review to the folders the PR title names: a local `dcf_build()` that was stopped part way leaves truncated or failed-run files in folders nobody meant to touch, and that is where they hide. If there are too many folders to run 6b on all of them, run it on every folder that 6a flags, every folder with a changed `standard/` or `dist/` file, and say which ones were skipped.

### 4. Check the PR itself

From the diff and the commit list, look for:

- files that do not belong to the stated change (another source, a workflow file, an unrelated bundle's parquet)
- files also changed on main since the base: `git diff --name-only <base> origin/main -- <changed paths>`. This is a conflict risk, and for data files the usual fix is to take main's version
- merge commits from main or from the same branch (`git log --merges --oneline <base>..<head>`) and conflict markers (`git diff <base>...<head> | grep -nE '^\+(<<<<<<<|=======|>>>>>>>)'`)
- stray files: `.xlsx` outside `raw/`, `.DS_Store`, html, root `settings.json`, a hand-edited `docs/data_sources_index.json`
- commented-out code added by the diff (added lines starting with `#` that contain `<-`, `%>%` or `|>`)
- `ingest.R` changed with no file under `standard/` changed. Ask the author how the ingest was re-run, since an unchanged source is skipped unless the state is cleared
- whether `README.md` was touched inside the `# Major change log` section: `git diff <base>...<head> -- README.md`

### 5. Read the code

Read every changed hunk of `ingest.R` and `build.R` in full, with enough of the file around it to follow the logic. Check it against [references/r-ingest-bug-patterns.md](references/r-ingest-bug-patterns.md). For a new source read the whole script. This is the general-logic review the SOP asks for.

### 6. Run the checks

#### 6a. Whole-diff data checks (run first, on every review)

```
py -I "${CLAUDE_SKILL_DIR}/scripts/check_data_changes.py" --base <base> --head <head>
```

(`python3` in place of `py -I` off Windows.) This checks **every** file the diff touches, not only the folders the PR is about, and reads them from git objects, so it needs no checkout. It catches the damage a killed or failed local run leaves behind, which the per-folder checks miss when that folder is outside the stated change:

| Check | Catches |
|---|---|
| `archive` | `.gz`/`.xz` that fail to decompress to the end, parquet with a broken footer: a truncated write or download |
| `file-size` | files over 50 MB (warning over 25 MB) |
| `raw-file` | large uncompressed files under `raw/`, and ones without a final newline (a partial download) |
| `failed-run` | `process.json` whose new `last_status` records a failed run |
| `local-path` | `C:/Users/...` paths in `process.json` state keys: dcf was run with an absolute path, so CI will never match the state |
| `crlf-hash` | `process.json` hashes taken on a CRLF (Windows autocrlf) checkout, so every CI run looks like a change |
| `row-drop` | `datapackage.json` `row_count` down more than 20%, or to 0 |
| `scope` | more than 5 data folders touched: usually the output of a full local `dcf_build()` |

Run it on a committed head. For working-tree changes that are not committed, it cannot see them: `review_checks.R` (`S05`) decompresses every changed standard and dist file in full, which covers truncation in the folders it is run on, so run 6b on every touched folder.

Every `ERROR` here is a `FAIL` in the report and goes at the top of Findings. A truncated file, a failed-run `process.json` or a partial raw download means the PR must not merge as is: the fix is to restore those paths from the base (`git checkout <base> -- <paths>`) and leave regeneration to the scheduled CI build. A `scope` warning means asking the author to revert every folder unrelated to the change. A PR made only of reverts gets the state checks as warnings.

Folders flagged here are in scope for 6b even when the PR is about something else.

#### 6b. Per-folder checks

```
Rscript "${CLAUDE_SKILL_DIR}/scripts/review_checks.R" data/<dir> [data/<dir> ...] --base <base> [--head <ref>]
```

The scripts are in `.claude/skills/ingest-code-review/scripts/`. `S05` fully decompresses each changed `.gz` and checks each parquet footer, so it reports a truncated file directly instead of leaving it to show up as a `C04` row loss. Use `Rscript` from the PATH (on Windows it may be `C:\Program Files\R\R-x.y.z\bin\Rscript.exe`). For a folder audit pass `--base none`. `--files a.csv.gz,b.parquet` limits a run to those files, which is useful when confirming one finding.

The output starts with a header line, then for each folder a `@@ <dir> kind=... head=... base=...` line, the findings, and a `== SUMMARY` line with the counts. `check_data_changes.py` prints `ERROR`/`WARNING [check] path: message` lines and a count, and exits 1 when it found an error; a Python traceback means it did not finish, which the report must say. For `review_checks.R`, `ENV FAIL | <reason>` means the script could not run (missing R package, unknown ref, base and head the same commit). Stop and tell the user. `FAIL X00` means one check stopped with an R error. The rest of the output is still valid, and the report must say which check did not finish.

Each finding is `<LEVEL> <ID> <path>[:<column>] | <message> | <details>`.

| Level | Meaning |
|---|---|
| `FAIL` | breaks a rule in CLAUDE.md or the SOP, or breaks something downstream |
| `WARN` | likely a problem, needs a look |
| `ASK` | the rules disagree or the answer depends on the team. Put it to the author |
| `HUMAN` | something only a person can judge, with where to look |
| `INFO`, `PASS`, `SKIP` | context, what passed, what was not checked and why |

`FAIL`, `WARN` and `ASK` are raised for problems that are new or worse than on the base version. A problem that was already there is printed as `INFO` and marked "already on base". Mention those in one sentence at most.

Changed values are a `WARN` only when `ingest.R`, `build.R` or a script changed in the same diff, or when a column was rescaled or emptied. With no code change they are `INFO`, "revised by the source".

A check with no line did not run. Passed checks are on the `PASS` lines. The check IDs are explained in `references/sop-checklist.md`.

If a changed source feeds a bundle (`C07`) and that bundle's `dist/` is not in the diff, say the bundle was not rebuilt in this change.

### 7. Confirm before reporting

For each `FAIL` and `WARN`, open the code or the data and confirm it. The script guesses. Column roles come from names and values, and the series checks do not know about seasons. For a `D06` finding, first check which columns form the series key: a descriptive column that changes over time splits one series into many. Drop anything you can disprove and say what you checked. To look at data, feed R a script on stdin (`Rscript - <<'EOF'`), and only read.

For a source with raw files, reconcile the `R01` tallies: apply the row filters from `ingest.R` to the raw file and compare the number of missing and suppressed cells with the standard file. The recipe is in `references/sop-checklist.md`. If the counts cannot be reconciled, report it as a finding.

### 8. Read the metadata

Read the `measure_info.json` diff. For each new or changed entry, check that the description says what the column is, that the unit matches the range of the values (the `D00` and `B00` lines print the ranges), and that `measure_type` is right. Read the first `_sources` description (`M06`): it is the only one shown on the site, so it has to cover every file in the folder. Check `restrictions` against the wording in CLAUDE.md.

Everything in `measure_info.json` has to come from the source, not from a guess. For each new or changed entry, find where the source itself states the definition, the unit, the measure type, the time resolution and the population or denominator. Places to look, in this order:

- the metadata file dcf saves next to a CDC download (`raw/<dataset-id>.json`: dataset description and column descriptions)
- the source's data dictionary, codebook or technical notes, at the `url` and `location_url` given in `_sources`
- column headers and footnotes in the raw file
- documentation the author links in the PR

Do the same for the `_sources` description and the `restrictions` text. Compare them with the source's own "about" and terms-of-use pages.

A statement you cannot find in any of these is a finding: quote the sentence and say it is not in the source documentation. This covers definitions that only restate the column name, units worked out from the size of the values, qualifiers the source does not use, and descriptions copied from a similar measure in another source. Do not judge an entry by whether it sounds plausible, and do not suggest replacement wording from your own knowledge of the topic. If the documentation is not in the repo and not at the URLs given, ask the author for it. Also flag text that reads as machine-written.

### 9. Bundles

For a touched bundle, read each `B` line against "MCP bundle rules" in `references/sop-checklist.md`. An existing dist file must not be renamed, reshaped or deleted, and its category levels must not be dropped, because pophive.org and the MCP server read it by name. A change of scale needs a change log entry.

### 10. Render the plots

```
Rscript "${CLAUDE_SKILL_DIR}/scripts/render_plots.R" data/<source>/standard/<file>.csv.gz [...] --base <base> [--head <ref>] [--maps]
```

Run it on the changed standard files, four at most unless asked for more: files with a `FAIL` or `WARN` first, then the largest. Add `--maps` when the file has state or county rows (it needs internet). Give the user the html paths. Do not open them and do not describe what they show: looking at the plots is for a person.

The script reads csv only. For a change that touches only `dist/` there is nothing to plot, so say that under "Not checked". If a render fails, pass on the `SKIP` reason and check whether a `D05` or `D10` finding on that file explains it.

### 11. Report

Report in the chat, in this order:

1. One line: what was reviewed, head and base short SHAs, folders checked.
2. **Findings**, most serious first. Each one: `path:line` or `file:column`, one sentence on what is wrong, the evidence (numbers, the check ID), and a one-line fix.
3. **Questions** for the author: `ASK` lines and anything where the SOP, CLAUDE.md and the guardrail skill disagree.
4. **SOP coverage**: the table from `references/sop-checklist.md`, one row per item, marked `passed`, `issue`, `not applicable` or `needs human eyes`, with a few words of evidence. Plots, age and category choices, wording of descriptions, proprietary status, whether a change is major, and squash merge and branch deletion are always `needs human eyes`.
5. **For a person to look at**: the html paths from step 10 and the `HUMAN` lines, ten at most per file.
6. **Not checked**, with the reason (ingest not re-run, raw files in a format the script does not read, file over the row limit, no plots for a bundle).
7. **Draft PR comment** in a fenced block: two to five short plain sentences, no headers, lists or bold, naming the file and what the problem does to the plots or the dashboard. This is a draft for the user to post. Never post it.

If nothing is wrong, say so in one line, give the coverage table and the list for a person, and leave out the draft comment.

Finish by running `git status --porcelain` and confirming it matches the output from step 1.
