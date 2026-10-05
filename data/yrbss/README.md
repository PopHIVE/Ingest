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
Nebraska, Pennsylvania, Tennessee, Texas, and Wyoming are not in CDC's 2025
release. Their estimates come from `raw/yrbss_chartdata_pre2025.csv.gz` and
end in 2023 or earlier.

CDC renumbered its question codes in the 2025 release. The ingest stops if the
codes or wording change again; review the change, then update
`raw/yrbss_catalog_reference.csv`.

Three wide files, one per stratification (sex, race/ethnicity, grade mapped to
modal age). Strata are marginal, not crossed. Each measure has value, 95% CI
bounds, and `_suppressed` / `_not_asked` flags.

Not included: sexual identity, transgender status, and sex-of-contacts strata
(dropped at download).

Rebuild from the repo root:

```r
dcf::dcf_process("yrbss")
```
