# bundle_census

This is a Data Collection Framework data bundle project, initialized with `dcf::dcf_add_bundle`.

A **source-complete** tall-format mirror of the census source: every measure the
Census ingest produces, in one uniform shape.

Unlike every other bundle here, this one is organised by **source**, not by
topic. Its purpose is format, not exclusive access: `census/standard/` already
publishes the same data, but as six wide CSVs split by geography level and by
Census program. This bundle collapses that into two parquet files with a single
`(geography, time, measure, value, source)` schema.

## Output Files

- `dist/census_county.parquet` — 5-digit county FIPS. 90 measures, 6 programs.
- `dist/census_state.parquet` — 2-digit state FIPS plus national `"00"`.
  87 measures (the three `census_ur_*` urban/rural measures are county-only).

## No allow-list, by design

`build.R` takes **every** non-index column of every census standard file, and
resolves each measure's `source` from its name prefix. `bundle_county_access`
curates an explicit list of census measures; if this bundle did the same, the
two would silently drift apart whenever the Census ingest gained a variable.
Being definitionally complete means it cannot. A measure whose prefix is not in
`SOURCE_BY_PREFIX` fails the build loudly rather than getting `source = NA`.

## Measures are deliberately duplicated into topic bundles

The same measures also appear in the topic bundles, alongside comparable
measures from other sources:

| Bundle | Census measures | Sits alongside |
|---|---|---|
| `bundle_county_access` | 45 — ACS social determinants, SAHIE, SAIPE, urban/rural, OQM | CHR access, AHRF providers, HUD housing, BLS unemployment, USDA food access |
| `bundle_maternal_health` | 1 — `acs_BTH`, as `birth_rate` | CHR teen births, infant/child mortality, CMS prenatal care |

**The topic bundles are canonical for analysis.** Their measures are curated and
directly comparable across sources — `acs_UMP` next to `bls_pct_unemployment`,
`acs_UNS` next to `chr_uninsured`, `acs_SNP` next to
`usda_pct_limited_access_low_income`. This bundle is a convenience mirror of a
single source.

**Do not union this bundle with the topic bundles** — you will double-count.
Note also that `acs_BTH` appears here under its source name and in
`bundle_maternal_health` under the id `birth_rate`.

This duplication follows existing practice: `bundle_antimicrobial_resistance`
and `bundle_enteric_diseases` ship byte-identical `resistance_by_agent.parquet`
and `resistance_by_pattern.parquet`.

## Sources

All of `census/standard/`. ZCTA output is not included — it is no longer
produced by the census ingest.

`data_pep`, `data_saipe`, `data_sahie` and `data_oqm` each carry national, state
and county rows in a single file, so the build splits them by FIPS length.

You can us the `dcf` package to rebuild the bundle:

```R
dcf::dcf_process("bundle_census", "..")
```
