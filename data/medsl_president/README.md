# MEDSL county presidential returns

Votes for president by county and party, 2000-2024, from the MIT Election Data
and Science Lab (<https://doi.org/10.7910/DVN/VOQCHQ>).

`standard/data_county.csv.gz` has total, Democratic and Republican votes, the
two parties' percent of all votes, and the Republican percent of the two-party
vote, one row per county and election. `standard/data_state.csv.gz` has the
same columns for the states, DC and the nation (`00`). `time` is the end of the
election year.

Alaska reports by election district, so it is in the state file only. Votes
that are not assigned to a county (statewide write-ins, overseas ballots) count
toward the state and national totals only. Kansas City, MO is added to Jackson
County, Bedford city, VA to Bedford County, and Shannon County, SD is carried
as Oglala Lakota County.

The ingest checks the dataset's version on Harvard Dataverse and downloads the
file again when a new one is published. If Dataverse cannot be reached it uses
the copy in `raw/`.

```r
dcf::dcf_process("medsl_president")
```
