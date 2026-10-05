# USAFacts voter registration

County voter registration by party affiliation, compiled by USAFacts from
state election offices for the 27 states and DC that record party affiliation
and publish it by county
(<https://usafacts.org/articles/more-voters-are-registering-outside-the-two-party-system/>).

`standard/data_county.csv.gz` has the number of registered voters and the
percent registered Democratic, Republican, and other or unaffiliated, one row
per county and year (2016-2026). `standard/data_state.csv.gz` has the same
columns by state, built from the county rows. There is no national row.

`time` is the end of the year. USAFacts takes the report closest to November
of each year, and the 2026 point is generally from early 2026. Kentucky starts
in 2017, Idaho and Rhode Island in 2018; California, Connecticut and
Massachusetts end in 2025; some states skip years. Connecticut uses its eight
former counties.

```r
dcf::dcf_process("usafacts_voter_registration")
```
