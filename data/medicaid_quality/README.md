# medicaid_quality

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

You can us the `dcf` package to check the project:

```R
dcf_check_source("medicaid_quality", "..")
```

And process it:

```R
dcf_process("medicaid_quality", "..")
```

## Output files

| File | One row per | Population |
|------|-------------|------------|
| `standard/data_child_core_set.csv.gz` | state, year, measure | Child Core Set (`_ch`) measures; the row closest to the Child target population (below) |
| `standard/data_adult_core_set.csv.gz` | state, year, measure | Adult Core Set (`_ad`) measures; the row closest to the Adult target population (below) |

Both files are long, with columns `geography`, `time`, `domain`, `measure`,
`measure_type`, `value` (the state rate), `pct_25`, `pct_75` and
`population_match`. Each `measure` (e.g. `medicaid_bcs_ad`) is documented by the
`measure_info.json` entry of the same name with `_rate` appended
(`medicaid_bcs_ad_rate`). A row exists only where the state reported a value.
In FFY 2014 CMS used some domain labels it later dropped (e.g. Behavioral
Health, later Behavioral Health Care); every year of a measure carries its most
recent CMS domain label.

`measure_type` says whether higher or lower rates are better. It comes from
CMS's `measure_type` field (reported from FFY 2019), applied to all years of
the measure; for measures that ended before FFY 2019 it is inferred from the
order of CMS's bottom and top quartiles (FFY 2016-2018), which matches CMS's
field wherever both exist. It is blank for LDL-AD and TDENT-CH (reported only
in FFY 2014-2015).

## Population hierarchy

States report each measure for a population described by a free-text CMS
string (e.g. `Medicaid & CHIP`, `Medicaid (Title XIX), Title XXI-funded
Medicaid Expansion CHIP, Individuals Dually Eligible for Medicare and
Medicaid`). Following advice from a Medicaid expert, each Core Set has a
**target population** and the row that matches it most closely is used
(`add_population_match()` in `ingest.R`):

- **Child Core Set (`-CH`)**: all Medicaid **and** CHIP children (Title XIX +
  Title XXI Medicaid-expansion CHIP + separate CHIP; `Medicaid & CHIP` before
  2024).
- **Adult Core Set (`-AD`)**: **non-dual** Medicaid (Title XIX).

Dual eligibles are avoided because Medicare is their primary payer, so much of
their care is missing from Medicaid data. When a state reports only a narrower
population (e.g. Medicaid without CHIP for a Child measure), the row is used
and flagged rather than dropped: the state may run CHIP as a Medicaid expansion
or have no separate CHIP, in which case the row is the full target. A Child
measure reported only for CHIP is used as a last resort and flagged
`chip_only`. Adult rows with no Medicaid population (separate CHIP, duals-only,
`Other`-only) and Child `Other`-only rows are never used in the Core Set files.

Order of preference, after a non-suppressed value over a suppressed one:

| Core Set | Preference (best first) |
|----------|-------------------------|
| Child | Medicaid + all CHIP → + `Other` → missing one CHIP component (e.g. Title XIX + M-CHIP) → Medicaid only → CHIP only |
| Adult | Title XIX only → + CHIP / M-CHIP / `Other` (fewest add-ons first) → Medicaid including duals |

The `population_match` column records how each row's population relates to
the target (documented in `measure_info.json` under `population_match`):

| Value | Meaning |
|-------|---------|
| `target` | matches the target population |
| `broader` | target plus small non-dual additions (`Other`; CHIP for Adult measures) |
| `narrower` | Child measures: only part of Medicaid + CHIP was reported |
| `chip_only` | Child measures: only CHIP enrollees were reported (no Medicaid). Least comparable with `target`: CHIP covers higher-income children whose rates can differ systematically. |
| `broader_incl_duals` | Adult measures: the state reported only a population that includes dual eligibles. These rows are kept, but Medicare pays first for duals, so much of their care is missing from Medicaid data and the rate may understate care received. |
