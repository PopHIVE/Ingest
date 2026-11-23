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
`measure_type`, `value` (the state rate), `pct_25`, `pct_75`,
`population_match`, `methodology`, `methodology_group`, `methodology_change` and
`data_quality_note` (see [Data-quality columns](#data-quality-columns)). Each `measure` (e.g. `medicaid_bcs_ad`) is documented by the
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

## Data-quality columns

Two kinds of flag mark values that may not be comparable with other years or
states. Neither changes `value`.

**Method changes (`methodology`, `methodology_group`, `methodology_change`).**
`methodology` is CMS's data-collection method for the row. `methodology_group`
groups it by whether medical records were reviewed:

| `methodology_group` | CMS methodology |
|---------------------|-----------------|
| `with_chart_review` | Hybrid (claims plus medical record review), alone or combined with other methods |
| `without_chart_review` | Administrative, ECDS or Electronic Health Records, without Hybrid |
| `no_group` | surveys (CAHPS, NCI), Other, not reported |

`methodology_change` is `1` when the group changed since the last year the
state reported the measure (not necessarily the previous FFY), `0` when it did
not, and blank for a state's first reported year and for `no_group` rows.
Across states, Hybrid -> Administrative lowers rates by a median 4.5 points and
the reverse raises them by 6.6 (unchanged methods: +0.2). Example: Kentucky
CIS-CH and IMA-CH moved from Hybrid (FFY 2022) to Administrative (FFY 2024) and
CIS-CH fell from 64 to 19. Administrative and ECDS share a group because
switches between them show no systematic effect.

**Documented problems (`data_quality_note`).** Set in `ingest.R`
(`quality_notes`), keyed on state FIPS, FFY and CMS measure abbreviation:

| Code | Cases | Source |
|------|-------|--------|
| `state_data_undercount` | West Virginia W30-CH, WCV-CH, FFY 2023-2024 | Hand-curated: rates fell to about a third when calculated in the state warehouse and recovered in FFY 2025; the state reported a warehouse logic error; plan rates in WV's EQR report confirm |
| `specification_deviation` | Wyoming AMR-CH, FFY 2022 | Hand-curated: state reported omitting two denominator steps |
| `ffs_excluded` | AZ, FL, LA, MA, MO, NE, NH, NJ, NM, NY, PA, RI, WA, WV (FFY 2024-2025) | Automatic: a CMS comment sentence that mentions both an exemption and FFS / fee-for-service (e.g. "one-year exemption from reporting this measure for the FFS population", "CMS-approved exemption for the fee-for-service population"). The ingest log lists the matches and any other exemption wordings it did not flag |

Not flagged row by row, but worth knowing: Alaska notes possible
underreporting from timely-filing lag on many measures (FFY 2021-2025), and
Virginia warns that FFY 2024 trends should be read with caution due to
methodological changes.
