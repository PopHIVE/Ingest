# yrbss

CDC Youth Risk Behavior Surveillance System, pulled from the YRBS Explorer API
(https://yrbs-explorer.services.cdc.gov/). National and state estimates for
high school students, 2005 onward, every two years.

Questions covered: all of Physical Activity (C06) and Sexual Behaviors (C04),
plus selected items from injury and violence, tobacco, alcohol and other
drugs, diet, and other health topics. The full list is `measure_dict` in
`ingest.R`.

Three wide files, one per stratification (sex, race/ethnicity, grade mapped to
modal age). Strata are marginal, not crossed. Each measure has value, 95% CI
bounds, and `_suppressed` / `_not_asked` flags.

Not included: sexual identity, transgender status, and sex-of-contacts strata
(dropped at download).

Rebuild from the repo root:

```r
dcf::dcf_process("yrbss")
```
