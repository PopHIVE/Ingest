---
name: update-data-sources-index
description: >-
  Populate the website data-sources catalog for PopHIVE/Ingest data sources (new
  or existing). Use when a new data source is added under data/, when a source's
  standard files change, or when the user asks to refresh the data sources index
  / the website data page. Write the catalog text (summary, search_terms,
  bucket, per-file stratification) into the "_catalog" block of the source's
  measure_info.json, then run scripts/build_docs.R to regenerate
  docs/data_sources_index.json from it.
---

# Update the data sources index

`docs/data_sources_index.json` is a lightweight catalog of every standardized
data source in this repo. It is **fully generated** by `scripts/build_docs.R`,
which runs automatically every day and on every `measure_info.json` change.

**Never hand-edit `docs/data_sources_index.json`.** The hand-written text lives
in each source's `measure_info.json`, in a top-level `_catalog` block, and the
build reads it from there:

```json
{
  "_catalog": {
    "name": "Display name (optional override of _sources[].name)",
    "summary": "One to two plain-English sentences describing the dataset.",
    "search_terms": ["Respiratory", "flu", "rsv"],
    "bucket": [],
    "files": {
      "data.csv.gz": "Vaccination uptake by vaccine and age",
      "data_urban.csv.gz": "Vaccination uptake by vaccine and urban/rural residence"
    }
  }
}
```

Everything else in the index entry (`github_folder`, `data_url`,
`data_dictionary`, `latest_date`, and each file's `dataset_link`) is computed
from the repo contents on every build, so there is nothing to write for those.

Defaults when a field is **absent** from `_catalog`:

- `name` → `_sources[].name` from `measure_info.json`.
- `summary` → a mechanical extractive stub (first sentence of the `_sources`
  descriptions). The build prints a `WARNING: <dataset> has no hand-written
  summary in measure_info.json _catalog` line when it uses this — treat that as
  a stop signal, not an acceptable result.
- `search_terms` and `bucket` → both default to the human-readable labels of the
  bundles that consume the source (`bundle_chronic_diseases` → `"Chronic
  diseases"`). They are two independent fields: deleting one from `_catalog`
  re-derives only that one; an explicit `[]` is respected as an intentional
  "none" and sticks.
- a file missing from `_catalog.files` → a blurb derived from the file name
  (`"Stratified by <tokens>."` / `"Overall; no stratification beyond time and
  geography."`). Both are placeholders to be replaced, never shipped.

## Procedure

### 1. Find what needs text

From the repo root, list source folders (everything in `data/` that is not
`bundle_*` and has both a `measure_info.json` and at least one
`standard/*.csv.gz`). For each, open `measure_info.json` and check its
`_catalog`:

- Needs a **summary** if `_catalog` is missing, `summary` is missing/empty, or
  the text reads like the raw first sentence of the source description rather
  than a purpose-written blurb.
- Needs **stratification** text if `_catalog.files` has no key for some
  `standard/*.csv.gz`, or its value is a placeholder.

Cross-check against `docs/data_sources_index.json` (the generated output) to see
what the site currently shows — but write the fix into `measure_info.json`.

Default when a new dataset is added: only fill what's missing/placeholder; leave
good existing text alone. If the user asks to refresh a dataset, rewrite it.

### 2. Write `summary` (1–2 easy-to-read sentences)

Read the same file's `_sources` `description` field(s) — consider **all**
entries under `_sources`, since multi-source datasets (e.g. `nchs_mortality`)
cover several distinct things. Write **one to two sentences**, **~25 words**
(the existing catalog runs 12–70), suitable for a data catalog: no preamble, no
quotes, no markdown.

Write at roughly a **15-year-old reading level** — short sentences, everyday
words, one idea per sentence. Spell out or briefly explain jargon and acronyms
the first time they appear (e.g. "ED (emergency department) visits" rather than
bare "ED visits"). Cut methodology detail that doesn't help a general reader
decide whether the dataset is useful: model names, survey-instrument citations,
ICD code lists, catchment sizes, lag times. That detail belongs in
`_sources.description`, not the catalog summary.

The house pattern is *`<source short name> <verb> <what it measures>`*:

- `"The NSSP records the percentage of emergency department patient visits for RSV, flu, and COVID-19."`
- `"NARMS tracks antimicrobial resistance in bacteria infecting people and animals in the food supply chain."`
- `"Annual average county unemployment rate from the BLS's LAUS program, covering the labor force ages 16 and older."`

### 3. Write `search_terms` and `bucket`

**`search_terms`** feeds the website search box. **Typically 3–4 terms**
(range 1–9), ordered *topic label(s) first, then specific keywords*:

1. **Topic labels**, capitalized — start from the labels of the bundles that
   consume this source, then reuse the established vocabulary rather than
   inventing a synonym: `Antimicrobial resistance`, `Cancer screening`,
   `Childhood immunizations`, `Chronic diseases`, `County access`, `Enteric
   diseases`, `Injury and overdose`, `Maternal health`, `Measles`,
   `Preventative services`, `Respiratory`, `Rural health`, `Vector borne`,
   `Youth wellbeing`.
2. **Specific keywords**, lowercase — how a visitor would actually type it:
   disease and pathogen names, colloquial synonyms, and the domain words behind
   the measures (`flu`, `rsv`, `overdose`, `gun`, `firearm`, `hep b`,
   `diarrhea`, `unemployment`, `heat`, `food access`, `maternal mortality`).

Examples: `["Respiratory", "flu", "rsv", "Covid"]`,
`["Childhood immunizations", "Measles"]`,
`["Rural health", "unemployment", "labor force", "economic determinants"]`.

**`bucket`** is the site-navigation grouping, edited independently of
`search_terms` even though the two derive from the same default. **Every dataset
currently has `[]`** — the grouping has not been assigned yet, so write
`"bucket": []` unless the user names one.

### 4. Write `_catalog.files` blurbs for every file

For **each** `standard/*.csv.gz` of the dataset (single- and multi-file alike),
add a key — the **bare file name**, no path and no URL — mapping to a short
blurb saying what that file holds and how it is stratified **beyond time and
geography**. The stratifiers are exactly the non-`time`/non-`geography`/non-value
columns (`age`, `sex`, `race_ethnicity`, `serotype`, `vaccine`, `virus`,
`grade`, …); the measure columns say what is measured. Read a file's header with
`vroom::vroom(path, n_max = 0)`.

Keep each to a **noun phrase of ~5 words** (the existing catalog runs 2–16, no
trailing period) — name the stratifying dimensions plainly and skip background
that belongs in the dataset `summary`.

- **Multi-file datasets:** say what distinguishes each file from its siblings.
- **Single-file datasets:** still name the stratifiers. Many single-file sources
  *do* have them (e.g. `nccr` by cancer type/age/sex/race; `nssp` by virus) —
  don't settle for a placeholder just because there is one file. Only when a
  file genuinely has no dimension beyond time and geography, name its measure
  instead (`measles_cdc/data.csv.gz` → `"Weekly measles case counts"`).

Examples:

```json
"files": {
  "data.csv.gz":            "Vaccination uptake by vaccine and age",
  "data_insurance.csv.gz":  "Vaccination uptake by vaccine and insurance coverage type",
  "data_urban.csv.gz":      "Vaccination uptake by vaccine and urban/rural residence"
}
```

More: `"County level unemployment rates"`, `"School-level vaccination and
exemption rates"`, `"Antimicrobial resistance in people by antimicrobial
agent"`, `"Rt data by state"`.

### 5. Regenerate the index

From the repo **root** (not `docs/`):

```powershell
Rscript scripts/build_docs.R
```

This rewrites `docs/index.html`, `resources/data_manifest.json`, and
`docs/data_sources_index.json` from the `_catalog` blocks. No API key is
required.

### 6. Verify

Open `docs/data_sources_index.json` and confirm each affected entry shows the
`summary`, `search_terms`, and `bucket` you wrote, and a `files` array whose
entries each have a `dataset_link` pointing at a real `standard/*.csv.gz` and a
meaningful `dataset_stratification`. Check the build log for any
`WARNING: ... no hand-written summary` line. Report which datasets were added or
refreshed.

## Notes

- The GitHub raw base is `https://raw.githubusercontent.com/PopHIVE/Ingest/main`;
  links look like `.../main/data/<dataset>/standard/<file>.csv.gz`.
- Commit the edited `data/<dataset>/measure_info.json` together with the
  regenerated `docs/` and `resources/data_manifest.json`.
- Keep `_catalog` as the last top-level key of `measure_info.json`, after
  `_sources`, so variable definitions stay at the top of the file.
