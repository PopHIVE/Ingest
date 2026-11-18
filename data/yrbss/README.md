# yrbss

CDC Youth Risk Behavior Surveillance System, pulled from the YRBS Explorer API
(https://yrbs-explorer.services.cdc.gov/). National and state estimates for
high school students, 2005 onward, every two years.

Questions covered: physical activity and sexual behaviors, plus selected items
from injury and violence, tobacco, alcohol and other drugs, diet, and other
health topics. The full list is `measure_dict` in `ingest.R`.

Measures are the percent of students with the risk behavior, except
`pct_close_at_school`. CDC reports some questions the other way round (e.g.
got 8 or more hours of sleep); those are converted to 100 minus the value.

Alabama, Alaska, California, Colorado, Florida, Georgia, Idaho, Iowa, Kansas,
Nebraska, Pennsylvania, Tennessee, Texas, and Wyoming dropped out of the
Explorer after CDC's 2025 release and came back in November 2026 with fewer
questions and years. For these 14 states, any question and year the Explorer
no longer serves is filled from the last pull before that release
(`raw/yrbss_chartdata_pre2025.csv.gz`). Other states are not filled.

CDC renumbered its question codes in the 2025 release. The ingest stops if the
codes or wording change again; review the change, then update
`raw/yrbss_catalog_reference.csv`.

Three wide files, one per stratification (sex, race/ethnicity, grade mapped to
modal age). Strata are marginal, not crossed. Each measure has value, 95% CI
bounds, and `_suppressed` / `_not_asked` flags. Suppressed and not-asked
estimates are blank (NA), not 0 (they were 0 before November 2026).

Not included: sexual identity, transgender status, and sex-of-contacts strata
(dropped at download).

Rebuild from the repo root:

```r
dcf::dcf_process("yrbss")
```
