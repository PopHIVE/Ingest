# bundle_flu

This is a Data Collection Framework data bundle project, initialized with `dcf::dcf_add_bundle`.

You can use the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_flu", "..")
```

Influenza surveillance and vaccination from 19 sources, with **flu variables only**: the RSV and
COVID columns of shared files (NSSP, Epic, RESP-NET, wastewater, NHSN, CFA Rt, NIS, IIS, ...) are
left out. Surveillance and vaccination are separate groups of files because they have different
shapes (weekly series by source, versus coverage by season and stratum).

Bundles name the time column **`date`**; the standard files call it `time`. `geography` is a name
(state, county, region) and `geography_fips` its identifier, with `"00"` for the national total.

## Output files

| Parquet | Sources | Rows | Grain |
|---|---|---|---|
| `flu_overall_trends.parquet` | `epic_resp_infections`, `nssp`, `respnet`, `wastewater`, `delphi_nhsn`, `delphi_hospital_claims`, `delphi_ili_fluview`, `kinsa_ili` | 66,986 | state + national, weekly, one row per `source` |
| `flu_trends_by_age.parquet` | `epic_resp_infections`, `respnet`, `nhsn_hospital_capacity` | 164,275 | state + national, weekly, by age group |
| `flu_ed_visits_by_county.parquet` | `nssp` | 652,491 | county, weekly |
| `flu_hospital_capacity.parquet` | `nhsn_hospital_capacity` | 267,040 | state, national and HHS region, weekly, long by `measure` |
| `flu_rt_and_mortality.parquet` | `cdc_cfa_rt`, `nchs_mortality`, `nnds` | 119,217 | state + national, long by `measure` |
| `flu_vax_coverage.parquet` | `fluvaxview`, `nis_flu_rsv`, `iis_vax`, `medicare_vax`, `vsd_pregnancy_vax` | 321,384 | state + national, by season, age and stratum |
| `flu_vax_substate.parquet` | `fluvaxview`, `nis_flu_rsv`, `iis_vax` | 73,393 | county, HHS region and named sub-state areas |
| `flu_vax_doses.parquet` | `iqvia_vax_administered`, `flu_doses_distributed` | 10,656 | national, weekly |

### Trend files

`flu_overall_trends` and `flu_trends_by_age` follow the `bundle_respiratory` trend layout: `value`
in the source's own units, `value_smooth` (3-week trailing mean), and `value_scale` /
`value_smooth_scale` (min-max to 0-100 **within one series**: one geography and source, plus age
in the by-age file). Scaling is what makes sources with different units comparable on one axis;
`value` is not comparable across `source`.

- **History starts 2020-01-04** (`TREND_START` in `build.R`), unlike `bundle_respiratory`, which
  keeps only the last two years. The scaled values are therefore relative to the full period.
  Series with fewer than 52 observations are dropped (per geography and source).
- **Kinsa is daily** in its source and is averaged to the Saturday ending each week; the week in
  progress (fewer than four days) is dropped. It is national only.
- `suppressed_flag` is 1 only for Epic Cosmos small cells that were imputed.
- **Delphi Hospital Claims contributes no rows.** `delphi_hospital_flu_smooth` is blank for every
  geography and date in the current `delphi_hospital_claims` standard file, so the series is
  filtered out. It is wired in and will appear if the source recovers.
- Age labels are each source's own and **do not line up** (Epic and RESP-NET `"<1 Years"`,
  `"1-4 Years"`; NHSN `"0-4"`, `"5-17"`). RESP-NET is `"Total"` only. NHSN `"Unknown"` is dropped.
- CDC NHSN appears in both files but in different units: `delphi_nhsn_flu` (overall) is a weekly
  admissions **count** and `nhsn_adm_rate_flu` (by age) is admissions **per 100,000**. The
  absolute admissions behind both are also in `flu_hospital_capacity`.

### `flu_hospital_capacity` and `flu_rt_and_mortality`

Both are long: filter on `measure` first, since `value` mixes units.

- `flu_hospital_capacity` carries the 15 flu columns of NHSN (patients, ICU patients, admissions
  all-ages / adult / pediatric, admission rates, percent of beds, and percent of hospitals
  reporting). `measure` is the NHSN column name. It keeps territories, and HHS regions
  (`geography_fips` `hhs_1` to `hhs_10`, not a FIPS code), with `geography_level` saying which.
- `flu_rt_and_mortality` has CDC CFA Rt (with interval and probability of growth, from
  2026-04), NCHS quarterly death rate, and NNDSS counts. **The NCHS series is "influenza and
  pneumonia" combined**, not influenza alone. Haemophilus influenzae in `nnds` is a bacterium and
  is not included.
- **NNDSS is year-to-date.** Pediatric flu deaths and novel influenza A are published as a running
  total that resets each MMWR year, so each is emitted as `_cumulative` (as published) and
  `_weekly` (differenced); the two are not additive. NNDSS occasionally revises a count down, which
  makes a few weekly values negative (4 of 56,316). They are **kept as reported**, as in
  `bundle_respiratory` and `bundle_measles`, so cut plot y axes at 0.

### Vaccination

`flu_vax_coverage` stacks every coverage source in one schema. A dimension a row is not stratified
on carries `"Total"` (`stratum_type = "Overall"`), so nothing needs NA handling.

| column | meaning |
|---|---|
| `source` | `CDC FluVaxView`, `CDC NIS-Flu`, `IIS`, `CMS Medicare`, `CDC Vaccine Safety Datalink` |
| `season` | `"2024-25"`; blank only for FluVaxView counties (calendar years) |
| `age`, `population` | age group in the source's own labels; `population` is `"Pregnant"` for VSD |
| `stratum_type`, `stratum` | `Race and Ethnicity`, `Vaccination setting`, or an NIS demographic |
| `measure` | `coverage` for all; NIS also gives `intent_definitely`, `intent_probably`, `intent_no` |
| `value`, `value_lcl`, `value_ucl` | percent, with the source's confidence limits where it publishes them |
| `denominator`, `n_vaccinated` | sample size / population behind the estimate; `n_vaccinated` is IIS only |

- **Suppressed cells are blank, not imputed**: `suppressed_flag = 1` and `value` is NA. Sources
  with no suppression (IIS, Medicare, VSD) are 0. Some cells are blank without a flag because the
  source did not publish them (a few FluVaxView cells, and many NIS intent cells).
- **Labels differ across sources**: age (`"18-49 Years"` vs `"18-49 years"`), race
  (`"Black, Non-Hispanic"` vs VSD's `"Black, NH"`). Filter on `source` first.
- FluVaxView coverage is published **monthly through each season**, so one season has several
  `date`s; the May row is the end-of-season estimate. IIS is monthly, NIS and Medicare weekly.
- `age = "Total"` on FluVaxView race and VSD rows means the source publishes no age split there,
  not necessarily all ages.
- IIS `partial_state_flag = 1` marks the New York and Pennsylvania state rows, which exclude
  New York City and Philadelphia (those report separately, and are in `flu_vax_substate`).
- Medicare's 75+ rows are RSV only, so flu is 65+. Its 186 fully blank source rows are dropped.

`flu_vax_substate` has the same value columns plus `geography_level` and `state_fips`.
`geography_fips` is a county FIPS for counties; for `HHS region`, `Substate`, `Local`, `City` and
`Freely associated state` it is the source's own id (`hhs_1`, `il_city_of_chicago`), which is not
a FIPS code. NIS `Substate` and `Local` areas overlap (Bexar County is also inside Texas-Rest of
State), so do not sum across `geography_level`.

`flu_vax_doses` is national. `doses_administered` (IQVIA, single doses, by `setting` and `age`)
and `doses_distributed_cumulative_millions` (CDC, millions) have different units. IQVIA
`setting = "Combined"` is pharmacy plus physician office, not every setting.
