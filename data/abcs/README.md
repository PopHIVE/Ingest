# abcs

CDC **Active Bacterial Core surveillance (ABCs)**. CDC publishes one dataset per
pathogen; this source ingests three of them through a single `ingest.R`:

| Pathogen | CDC dataset | Outputs |
|---|---|---|
| *Streptococcus pneumoniae* (pneumococcus) | [`qvzb-qs6p`](https://data.cdc.gov/resource/qvzb-qs6p/) | `data.csv.gz`, `uad.csv.gz` |
| Group A Streptococcus (*S. pyogenes*) | [`9y49-tura`](https://data.cdc.gov/d/9y49-tura) | `strep_*`, `gas_*` |
| Group B Streptococcus (*S. agalactiae*) | [`95m5-agj4`](https://data.cdc.gov/d/95m5-agj4) | `strep_*`, `gbs_*` |

Each dataset's download state is tracked separately in `process.json`
(`raw_state`, `raw_state_gas`, `raw_state_gbs`), so a change to one does not
force the others to reprocess.

**ABCs is population-based but covers a catchment of selected US counties —
roughly 35 million people — not the whole country.** Rates are observed within
that catchment; counts labelled national are CDC's population-weighted
extrapolations. All strep output is national only (`geography = "00"`), with no
state or county breakdown.

## Standard output

| File | Grain | Contents |
|---|---|---|
| `data.csv.gz` | year × geography × age × serotype | pneumococcal IPD counts, percent, rate |
| `uad.csv.gz` | 2020, 4-state area × serotype | pneumococcal urinary antigen detection |
| `strep_rates.csv.gz` | year × pathogen × age × sex × race × onset | case and death rates per 100,000 (two case-rate measures — see below) |
| `strep_counts.csv.gz` | year × pathogen × age × onset | estimated cases, deaths, survivals |
| `strep_resistance.csv.gz` | year × pathogen × age × onset × antibiotic | percent non-susceptible, plus isolates tested |
| `gas_syndromes.csv.gz` | year × syndrome | Group A syndrome rates per 100,000 |
| `gas_emm.csv.gz` | year × emm type | Group A emm type percents and isolate counts |
| `gbs_syndromes.csv.gz` | year × age × onset × syndrome | Group B syndrome percents |
| `gbs_serotypes.csv.gz` | year × age × onset × serotype | Group B capsular serotype percents |
| `gbs_alph.csv.gz` | year × age × onset × ALPH type | Group B ALPH gene percents, 2015 on |

Group A and Group B share a raw layout, so rates, counts and resistance are
**merged into single files keyed by a `pathogen` column** with
organism-agnostic measure names (`abcs_rate_cases`, not `abcs_gas_rate_cases`).
Syndromes and typing are **not** merged, because they are not the same
measures: Group A syndromes are a rate per 100,000 while Group B's are a percent
of cases, and emm types and capsular serotypes are different concepts.

Antibiotics, syndromes, emm types, serotypes and ALPH genes are **dimensions,
not measures**, so each gets a column of its own and the measures stay wide —
the same shape `serotype` has in `data.csv.gz`, which carries 88 serotypes in
one column rather than 88 columns. Encoding them in column names instead had
made `gas_emm.csv.gz` 48 columns wide.

## Missing values

**A blank is a cell CDC did not publish for that row, so a 0 is always a measured
zero.** Nothing is imputed or filled in. In `gbs_serotypes` for 2000 late-onset,
serotypes II, IV and VI read 0 because CDC reported none, while the two
VI-grouping rows are blank because CDC did not break them out that year.

Common reasons a cell is blank: an antibiotic off that pathogen's susceptibility
panel (tetracycline and linezolid are Group A only), an emm type CDC did not
itemise that year or a Group B serotype it regrouped, a rate not broken out for
that stratification (death rates are published only overall and by age), or a
denominator CDC omits for most of the resistance series.

Earlier versions zero-filled these cells and paired every measure with an
`abcs_not_reported_flag_<measure>` column. Both are gone: the flags were exactly
`is.na(measure)`, and 57 of 81 never fired.

## Notes and gotchas

- **The 2026 CDC restructure (Group A) changed meaning, not just shape.** `emm
  types` was one topic whose `viewby` held the type; now each type is its own
  topic, `viewby` holds that type's isolate count, and `value` is a
  **proportion** despite units reading "Percent". And syndromes moved from a
  percent of cases (1997+) to a **rate per 100,000** (2001+), hence the
  `rate_syndrome_*` naming. Re-running the pre-2026 parsing produced an empty
  emm file.
- **Every "Percent" value is a proportion (0–1)** in the current release and is
  scaled by 100 here. A guard stops the ingest if any exceeds 1.5, so a flip back
  to true percents fails loudly rather than inflating everything 100×.
- **Non-ASCII labels.** Age arrives as `≥65 years old` and the Group B adult
  group as both `Adults, ≥65 years old` and `Adults, ≥ 65 years old`. Under the C
  locale `dcf_process` runs in, a non-ASCII literal cannot be translated for
  comparison, so matching one directly silently drops every affected row. All
  labels are stripped to ASCII and whitespace-squeezed first, with a `stop()` if
  no 65+ rows survive.
- **`age` bands are source-determined and inconsistent between files** —
  pneumococcal uses `<5 years` / `5-49 years` / `50+ years`; strep rates use
  `<1` / `1` / `2-4` / `5-17` / `18-34` / `35-49` / `50-64` / `65+`; strep
  resistance and typing use `<1` / `18-64` / `65+`. ABCs reports no 2-17 band for
  Group B, and its `18-64` / `65+` split does not line up with the `18-49` /
  `50-64` bands CLAUDE.md documents. `Total` is the all-ages aggregate throughout
  and overlaps the bands.
- **One case-rate measure, two denominators — see `rate_denominator`.** CDC
  labels every rate `"Per 100,000 population"` while using two different bases,
  so `strep_rates.csv.gz` carries a `rate_denominator` column:
  `"Stratum population"` means per 100,000 of the group the row describes, and
  `"Population"` means per 100,000 of the whole population regardless of the
  row's stratum (how CDC reports the infant onset rates). For 1997 Group B the
  `<1` band reads **115.7** (≈ 3,900 infant cases / 3.9M births × 100,000) while
  early-onset + late-onset read 0.70 + 0.40 = **1.10** (≈ 3,900 / 272M ×
  100,000). They differ in all 28 years, mean absolute difference 66.3, so **rows
  with different denominators must never be summed or plotted on one axis** —
  filter on the column first.
- **`onset` is not additive.** CDC's Group B labels conflate age with infant
  onset timing (`Infants, early-onset disease`), which the ingest decomposes into
  `age` + `onset`. Onset rows are a *subset* of infants, and the `Total` rows
  span all ages — 1997 reads 16,600 all-ages cases against 2,600 + 1,300 infant
  cases, so summing across `onset` double-counts.
- **One derived row.** `strep_counts.csv.gz` carries an `age = "<1"` /
  `onset = "Total"` case count computed as early + late (1997: 3,900), because CDC
  publishes the parts but no combined infant figure. Summing is valid for counts,
  which carry no denominator — which is exactly why no equivalent total is
  derived for the rates. Everything else in these files is as published.
- **`Total` is the aggregate level** for `age`, `sex`, `race_ethnicity` and
  `onset`, matching the label used across the rest of the database. Note this
  differs from the `Overall` that CLAUDE.md documents.
- **Rates and counts are written to separate files.** Counts exist for only a few
  of the rate files' index combinations; folding them together left columns
  ~85–93% NA, sparse enough that `vroom`'s type guessing infers `logical` and
  silently blanks out every real value on read.
- **Group B serotype rows for 1997–1998** are placeholder zeros with a zero
  isolate count; treat that series as starting in 1999.
- Pneumococcal denominator data comes from the ABCs surveillance matrix,
  extracted to csv: [abcs-surveillance-matrix.pdf](https://www.cdc.gov/abcs/downloads/abcs-surveillance-matrix.pdf).
  The all-sites summary is recalculated from the 8 sites that report
  consistently from 1998 on, and so differs from CDC's own All-site figure.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

```R
dcf::dcf_check("abcs")
dcf::dcf_process("abcs")
```
