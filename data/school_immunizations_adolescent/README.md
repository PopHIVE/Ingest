# School immunization assessments, adolescents

County and state Tdap, MenACWY, and HPV coverage and exemption rates for the
grade at which states require adolescent vaccines, taken from the per-state
standard files in [PopHIVE/school_immunizations](https://github.com/PopHIVE/school_immunizations).

`ingest.R` downloads `data/<ST>/standard/data.csv.gz` for each state in
`SPEC`, keeps the adolescent-grade county and state rows, converts the
proportions to percent, and maps each state's columns onto the common
`school_adol_pct_*` set. A state whose upstream file loses a column gets a
warning, not a failure.

| State | Grade | Years | Measures |
|-------|-------|-------|----------|
| AK | Adolescent (registry, ages 13-17) | 2023-24 to 2024-25 | tdap, hpv, menacwy, complete; state and Anchorage only |
| CT | 7th | 2012-13 to 2025-26 | tdap, menacwy, complete, medical/religious exemptions; planning regions from 2024-25 |
| IN | 6th and 7th | 2023-24 to 2025-26 | tdap, menacwy |
| LA | 6th | 2021-22 to 2024-25 | tdap, menacwy, complete, any exemption |
| MA | 7th | 2013-14 to 2025-26 | tdap, menacwy, medical/religious/any exemptions |
| ND | 7th | 2018-19 to 2024-25 | tdap, menacwy, medical/religious/personal exemptions |
| TX | 7th | 2013-14 to 2025-26 | tdap, menacwy, conscientious exemptions; small cells suppressed |
| WA | 6th to 2019-20, 7th from 2020-21 | 2016-17 to 2025-26 | complete, all exemption types |

`time` is the school-year start (`YYYY-09-01`). Rates are comparable within a
state over time, not across states: series definitions, the grade assessed,
and denominators differ.

Not included, for a later pass: Colorado (K-12 combined only), Pennsylvania
(7th grade file has no Tdap or MenACWY), states with adolescent exemption
rates but no coverage (AL, KY, MO, NJ, RI, UT, VA, WV), and states that
publish adolescent rows by school only (ME, MI), which need an
enrollment-weighted county roll-up.

```r
dcf::dcf_process("school_immunizations_adolescent")
```
