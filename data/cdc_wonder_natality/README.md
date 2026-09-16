# cdc_wonder_natality

CDC WONDER **Natality, 2016-2024 expanded (Single Race)**, database D149 —
annual live-birth counts, national, state, and county level, by the mother's
age, the mother's race and Hispanic origin, and the infant's sex.

Beware the role names: `age` is the **mother's** age, `sex` is the **infant's**
sex, and `race_ethnicity` is the **mother's**. Breakdowns are queried one
dimension at a time and are not cross-tabulated — a row stratified by age has
`sex` and `race_ethnicity` set to `"Overall"`, and vice versa.

The export carries counts only. Birth Rate and Fertility Rate are separate
checkboxes on the WONDER form that this pull does not tick, so there is no
denominator or rate in `standard/`.

## Where the raw data comes from

CDC WONDER has no API for sub-national data (its official API is national-only
by policy), so `raw/natality_expanded_2016_2024/` is produced by the Selenium
scraper in the sibling repo
[`PopHIVE/wonder-scraper-drowning`](https://github.com/PopHIVE/wonder-scraper-drowning),
which drives the live query form — one query per year × geography level ×
demographic breakdown, each download validated against the export's
`Query Parameters:` footer before it is kept. From a checkout of that repo:

```bash
# one-time
python3 -m venv scraper/.venv
scraper/.venv/bin/python -m pip install selenium webdriver-manager \
    "git+https://github.com/govex/cdc-wonder-scraper.git"

# national + state, straight into this source's raw/ tree
caffeinate -i -s scraper/.venv/bin/python scraper/natality_scraper.py \
    --output-dir ../Ingest/data/cdc_wonder_natality/raw/natality_expanded_2016_2024

# county, added separately -- see the caveat below before running this
caffeinate -i -s scraper/.venv/bin/python scraper/natality_scraper.py \
    --levels national state county \
    --output-dir ../Ingest/data/cdc_wonder_natality/raw/natality_expanded_2016_2024
```

Already-downloaded files are skipped, so a re-run only fills gaps; add
`--check --verify` to list expected-vs-present files and validate every
present file's footer without downloading anything.

**County level** is a 51-states × 9-years × 4-queries loop (1,836 queries) with
a 20–40s delay per request plus a fresh browser session each time, so budget
**2–3 days**, not hours. `ingest.R` writes `standard/data_county.csv.gz`
automatically once those files exist, and needs no change. Run it under
`caffeinate -i -s` with the lid open — the scraper's own README notes that a
mid-run sleep turns seconds-long steps into 10–40 minute stalls.

### County coverage is partial — read this before using `data_county.csv.gz`

CDC publishes county natality **only for counties of 100,000+ people**. Within
each state, every smaller county is pooled into one row labelled
`"Unidentified Counties, XX"` under the pseudo-FIPS code `SS999` (state FIPS +
`999`). Montana 2023, for example, names just Missoula and Yellowstone; the
other 54 counties — 8,219 of the state's 10,078 births, 82% — collapse into
`30999`.

Those rows are **kept, not dropped**: discarding them would silently lose most
births in rural states and leave the county table far short of its state total.
But `SS999` is not a real FIPS code, so **exclude it when joining to county
geography or drawing a county map**. `ingest.R` reports how many such codes
there are and what share of county births they hold on every run.

This is a CDC publication rule, not a scraper limitation — the state and
national tables are unaffected and remain complete.

## Eight more breakdowns: `data_state_detail.csv.gz`

Beyond mother's age/sex/race, D149 exposes birth weight, gestational age,
delivery method, plurality, prenatal care trimester, tobacco use, mother's
education, and marital status — each queried on its own, **national + state
only** (no county pull for these). They land in a separate file with generic
`dimension`/`category` columns rather than eight more mostly-`"Overall"`
columns on `data_state.csv.gz`: with 11 independent one-at-a-time breakdowns,
a wide table would be dominated by filler.

| `dimension` value | WONDER variable used |
|---|---|
| `birth_weight` | Infant Birth Weight 12 (the 12-group resolution; finer 14-group and 100g-increment versions exist but aren't pulled) |
| `gestational_age` | OE Gestational Age Recode 10 (the Obstetric Estimate method — NCHS's preferred one since ~2014, not the older LMP-based method) |
| `delivery_method` | Delivery Method (plain vaginal/cesarean/unknown; finer "Final Route and Delivery Method" / "Delivery Method Expanded" variables exist but aren't pulled) |
| `plurality` | Plurality (single/twin/triplet+) |
| `prenatal_care_trimester` | Trimester Prenatal Care Began |
| `tobacco_use` | Tobacco Use (yes/no/unknown; the separate cigarette-*count* variables aren't pulled) |
| `mothers_education` | Mother's Education |
| `marital_status` | Marital Status |

Two label quirks worth knowing before using this file:

- **`"No prenatal care"`** (under `prenatal_care_trimester`) is a real,
  substantive answer — it is kept as its own category, not collapsed into
  `"Unknown"`.
- **`"Excluded"`** (under `mothers_education` only) means the responding
  state's birth-certificate revision doesn't collect that item at all — a
  jurisdiction-level gap, not one record's missingness. It's still collapsed
  into `"Unknown"` here for consistency with every other placeholder, but the
  reason is different in kind from a person whose education just wasn't
  recorded.

None of these eight were pulled before 2026-09-15; add `--levels ... county`
to a query for one of them the same way as the others if county ever becomes
worth the 2–3 day cost, but nothing here does that automatically.

## Output

`standard/data_state.csv.gz` (and `standard/data_county.csv.gz`, once the
county pull has been run) — `geography` (FIPS code: `"00"` national, 2-digit
state, 5-digit county incl. `SS999` pooled counties), `time` (`YYYY-12-31`),
`age`, `sex`, `race_ethnicity`, `natality_births`, `natality_suppressed_flag`.

`standard/data_state_detail.csv.gz` — the eight breakdowns above: `geography`
(`"00"` national or 2-digit state; no county), `time`, `dimension`, `category`,
`natality_births`, `natality_suppressed_flag`. No `"Overall"` row here — sum a
dimension's categories yourself if you need one, keeping suppressed rows in
mind.

- National rows are CDC WONDER's own national query, never a sum of the
  states: suppressed state cells each hide 1–9 births, so a state sum runs low.
- A state's (or county's) `"Overall"` row in `data_state.csv.gz` is summed from
  its infant-sex rows, and is NA if either sex was suppressed.
- Cells representing 1–9 births are suppressed by CDC; `natality_births` is NA
  with `natality_suppressed_flag = 1` and is **not** imputed.
- WONDER's placeholder category rows (`"Unknown or Not Stated"`,
  `"Not Available"`, `"Not Reported"`, `"Not Stated"`, and — `data_state_detail`
  only — `"Excluded"`) are collapsed into a single `"Unknown"` level rather
  than dropped — on the Hispanic-origin breakdown that category carries real
  volume (about 1% of births nationally).

Consumed by `bundle_maternal_health` (→ `maternal_natality.parquet`, and the
`births` measure in `maternal_state.parquet`).

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
