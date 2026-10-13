# epic_mental_health

Epic Cosmos mental health emergency department burden: monthly ED length-of-stay statistics
(median and quartiles), diagnosis case mix, and ED visit counts, by mental health diagnosis
group, state of residence and age band, with an all-cause ED baseline.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Updating

All raw data is a set of password-protected SlicerDicer exports in `raw/staging/`, all pulled
2026-10-07 with data through 2026-08. Re-export a session over the full date range and replace
its file; `ingest.R` reprocesses only when a file hash changes. Each file's layout is read from
its own header. All sessions use Data model = ED Encounters, population base = All ED
Encounters, filtered to **Country of Residence** = United States of America. The ingest
rejects a Country of Care export (it counts non-US residents treated in the US, ~1.3% more
visits nationally, and disagrees with the residence sessions in small state cells) and a Sneak
Peek sample export (~1% of real volume).

- **Flat sessions** (12 files, one per diagnosis and measure set: `behavioral qs.xlsx`,
  `mood q1 q3.xlsx`, `psychosis all.xlsx`, the `ED Encounters_Summary_*` files, ...): rows are
  Start Date / End Date / ED Diagnoses / State of Residence / Age at Time of Visit (5-year
  bands), one column per measure (median, Q1, Q3, Percentage of Sliced Population). Rows are
  laid out in blocks (national all-cause, national by diagnosis, state by diagnosis, state by
  diagnosis by age) with no merged cells. Overlapping files (e.g. `psychosis all` and
  `psychosis med pct`) must agree cell for cell or the ingest stops.
- **Visit-count crosstab** (`ED Encounters_Crosstab_2026-10-07T12_25_04.xlsx`, session
  2864470): Number of ED Encounters, rows = State of Residence (plus a `Total` state for the
  nation) x Age at Time of Visit (five bands and a per-state all-ages `Total` row), columns =
  Year over Month. The source of every `epic_ed_n_visits` value and the denominators of the
  case mix. The state is written on its first row only and is carried down.

The older state x year x month crosstab with Evaluated Sex columns (August 2026) is no longer
read; the ingest stops with a clear error if one is placed in `raw/staging/`.

Requires:

- `EPIC_XLSX_PASSWORD` in `.Renviron` (see `usethis::edit_r_environ()`) — exports are
  password protected.
- `msoffcrypto-tool` on the Python used by R (`python -m pip install msoffcrypto-tool`).

## Output

`standard/data.csv.gz`, indexed by `geography` x `time` (month end, `YYYY-MM-DD`) x `age` —
17,472 rows, 81 columns, 2022-01 through 2026-08. `age` takes `0-4`, `5-9`, `10-14`, `15-19`,
`20-24`, and `Overall` (all ages). The exports stop at age 25, so the bands do not sum to
`Overall`. Age bands exist at the state level for every measure, and nationally for
`epic_ed_n_visits` only. Geographies are the 50 states, DC, and national (`"00"`).

| Column family | Statistic | Where populated |
|---------------|-----------|-----------------|
| `epic_ed_los_median_*` | Median ED length of stay, minutes. **Recommended default** | All geographies, ages |
| `epic_ed_los_q1_*`, `epic_ed_los_q3_*` | 25th / 75th percentile, minutes | As median |
| `epic_ed_pct_visits_*` | Percent of ED encounters with the diagnosis (case mix) | National and state, all ages; state age bands |
| `epic_ed_pct_age_*` | Percent of the state's diagnosis encounters in the age band | State, age-band rows only |
| `epic_ed_n_visits` | Number of ED visits, all causes | National and state, every age row |

Diagnosis suffixes: `suicidal_behavior`, `mood`, `behavioral`, `substance_use`, `psychosis`,
`eating_disorders`, `other`, `all_cause`. `all_cause` (every ED encounter, including
non-mental-health) is published for median/Q1/Q3 nationally for all ages only, and is the
baseline to normalize against. `epic_ed_pct_visits_*` and `epic_ed_pct_age_*` omit it.
Every column has a `_suppressed_flag`; see Notes.

## Notes

- **State `epic_ed_pct_visits_*` is derived**, because the source's "Percentage of Sliced
  Population" means a different thing in each block: national diagnosis row = diagnosis share
  of all ED encounters (`nat`, read directly for the national row); state row = the state's
  share of the national diagnosis volume (`r`); age-band row = the band's share of the state's
  diagnosis encounters (`pa`). With `N` an encounter count, using `epic_ed_n_visits`:

  ```
  N(state, dx)       = r * nat * N(nation, all)
  case mix, all ages = N(state, dx) / N(state, all)
  case mix, age band = pa * N(state, dx) / N(state, age band, all)
  ```

  Checked against the August derivation (state x sex crosstab, now retired): the median state
  ratio is 0.99-1.00 for six of seven diagnoses, with p10-p90 about 0.94-1.07, and the seven
  buckets sum to a median 6.6% of state ED encounters (6.1% before). Eating disorders run ~10%
  below the old values because the old export pinned the national share at 0.01 whereas the
  new ones resolve 0.008-0.010. Source percentages carry about three significant digits, so
  rare buckets (eating disorders at ~0.01% nationally) carry large relative rounding error. The
  buckets are not mutually exclusive, so they need not sum to a total.

- **`*_all_cause` is all-cause, not a mental health total.** It is the total row of the ED
  Diagnoses grouper, which includes the non-mental-health diagnoses that are not displayed as
  their own columns. Use it as the baseline when comparing geographies or months.

- **`epic_ed_n_visits`**: on `Overall` rows it is all-ages ED encounters for the state or the
  nation (the nation matches the sum of states to within 0.1%); on age-band rows it is that
  band's count, about 57% of the total in sum. Counts of 10 or fewer are imputed as 5 with the
  flag set to 1 (the value is kept).

- **Suppression and flags.** Epic suppresses cells built on 10 or fewer encounters, and omits
  those rows from the exports entirely, so an absent state-month-age row can be suppression or
  simply not returned. Length-of-stay and percentage statistics are not counts and cannot be
  imputed: the value is left `NA`. The flag is `1` for a missing value inside the months a
  column's exports cover, `0` where published, and `NA` where the column is not reported for
  that row at all (not a stratum it describes: for example `epic_ed_pct_age_*` on `Overall`
  rows). For derived `epic_ed_pct_visits_*` the flag is 1 when an input was suppressed.

- **Coverage gaps.** Suicidal behavior starts 2022-07. Some states are absent from a given
  diagnosis's export throughout (for example no DC, South Dakota or Wisconsin for behavioral),
  and about 31 states have eating-disorder data. Puerto Rico, other territories, Canadian
  provinces, Mexican states and `None of the above` (unknown state) are dropped:
  `resources/all_fips.csv.gz` has no name for them.

- **Quartile ordering holds**: `Q1 <= median <= Q3` in every one of the 51,608 published
  triples. Length of stay runs to ED **departure**, so it includes boarding time for patients
  awaiting an inpatient or psychiatric bed; that is the main reason mental health stays run
  several times the all-cause median, and it makes these measures boarding indicators as much
  as throughput ones.

- **Removed in the October rebuild** (all came only from the retired August crosstab): average,
  standard deviation, minimum and maximum length of stay, and the Male/Female/Ambiguous strata
  (and the `sex` column). Medians in the two pulls differed (about a minute nationally, up to
  ~450 minutes in small state cells) because the source refreshes between pulls, so values here
  will not match the earlier file. Retrieve the old version from git history (commit 6d7494e)
  if needed.

- **`ingest.R` validates rather than cleans.** It hard-fails on structural problems — duplicate
  index rows, a value and its flag disagreeing, overlapping exports that disagree, unrecognized
  diagnosis/age/measure labels, percentages outside 0-100, age bands summing past 100, `Q1 <=
  median <= Q3` failing in more than 1% of cells, state age-band counts exceeding the state
  total, a median mental health share outside 1-25%, or the national visit count straying from
  the sum of the states.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
