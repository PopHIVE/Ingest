# delphi_nrevss

CDC National Respiratory and Enteric Virus Surveillance System (NREVSS) laboratory
testing data, read from the CMU Delphi Epidata v5 API (`nrevss` source).

| File | Geography | Contents |
|---|---|---|
| `standard/data_nation.csv.gz` | `00` | Weekly tests, positive tests and percent positive for seven viruses, from 2019-07 |
| `standard/data_region.csv.gz` | `hhs_1` ... `hhs_10` | Same signals as the national file, by HHS region |
| `standard/data_state.csv.gz` | state FIPS | CDC moving averages for RSV and COVID-19 only, from 2024-02 |

This source replaced the `NREVSS` source (CDC dataset 3cxc-4k8q) in November 2026.
`bundle_respiratory/dist/rsv_positive_tests.parquet` is now built from the RSV
columns of `data_region.csv.gz`; the bundle computes the centered 5-week average,
the population-weighted national value, `scaled_cases` and the epidemiological
year/week that the old source either published or computed.

Weekly positive counts of 1-4 are suppressed by CDC and are missing here (percent
positive is still reported in those weeks). The old CDC HHS file was not
suppressed. The bundle counts these weeks as 2.5 when it computes the 5-week
average, and sets `suppressed_flag = 1` on every value that relies on one.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
