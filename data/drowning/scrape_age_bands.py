"""
CDC WONDER age-band drowning scraper
======================================
Pulls drowning deaths and population for ages 0-24 as ONE WONDER cell per
geography and year, by FILTERING WONDER's Ten-Year Age Group variable (D158.V5:
<1, 1-4, 5-14, 15-24, ...) to those groups and grouping by geography only.
WONDER computes the total itself, so the cell is suppressed only if all of 0-24
has 1-9 deaths, not if any one age group does. County uses Five-Year groups
(<1 ... 20-24) for the same band, because CDC drops county population for
Ten-Year ones. A second query per geography and year has no age filter
(raw_category "Overall"): all drowning deaths, not stratified by age.

Same database and cause as the other drowning data: Underlying Cause of Death
2018-2024, Single Race, ICD-10 W65-W74. Output (one parquet per level, columns as
in raw/ucd_icd10_expanded_2018_2024.parquet, breakdown = "ageband"; raw_category
is "0-24" or "Overall"):

    raw/ageband_<level>_ucd_icd10_expanded_2018_2024.parquet

Run from this folder (setup as in scrape_national.py):
    .venv/Scripts/python scrape_age_bands.py --levels national state
    .venv/Scripts/python scrape_age_bands.py --levels county --output-dir D:/keep
Already-downloaded CSVs in --output-dir are skipped, so a re-run fills gaps.
County is one all-states query per band x year (14 queries, not one per state).
"""

import argparse
import csv
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

import scrape_national as sn

HERE = Path(__file__).resolve().parent

# band -> WONDER Ten-Year Age Group option values of D158.V5
BAND_GROUPS = {
    "0-24": ["1", "1-4", "5-14", "15-24"],
    "Overall": [],  # no age filter: all drowning deaths, not stratified by age
}
BANDS = list(BAND_GROUPS)

# County: CDC omits Population and Crude Rate when the Ten-Year variable is used
# at county level, but not for Five-Year groups. The same bands are expressed in
# Five-Year groups (D158.V51) there so county rates exist.
BAND_GROUPS_COUNTY = {
    "0-24": ["1", "1-4", "5-9", "10-14", "15-19", "20-24"],
    "Overall": [],
}
FOOTER_AGE_KEY = {"D158.V5": "Ten-Year Age Groups:", "D158.V51": "Five-Year Age Groups:"}


def age_spec(level: str):
    """(variable code, {band: option values}) used to filter `level`'s queries."""
    if level == "county":
        return "D158.V51", BAND_GROUPS_COUNTY
    return "D158.V5", BAND_GROUPS

# puller query-name prefix -> group-by labels
GROUPBY = {"national": ["Year"], "state": ["State"], "county": ["County"]}


def qname(level: str, band: str) -> str:
    return f"{level}_band_{band}"


def band_of(name: str) -> str:
    return name.rsplit("_band_", 1)[1]


def out_file(level: str) -> Path:
    return HERE / "raw" / f"ageband_{level}_ucd_icd10_expanded_2018_2024.parquet"


def patch_puller(cdc_wonder_puller):
    """Make every *_band<k> query also set the age filter after the cause filter."""
    from selenium.webdriver.common.by import By

    orig = cdc_wonder_puller.make_setup_func

    def set_age_filter(driver, var, values):
        radio = driver.find_element(By.ID, f"RO_age{var}")
        driver.execute_script("arguments[0].click();", radio)
        time.sleep(0.5)
        driver.execute_script(
            """
            var sel = document.getElementById(arguments[0]), want = arguments[1];
            for (var i = 0; i < sel.options.length; i++)
                sel.options[i].selected = want.indexOf(sel.options[i].value) >= 0;
            sel.dispatchEvent(new Event('change', {bubbles: true}));
            """,
            f"S{var}", values,
        )
        time.sleep(0.5)
        got = driver.execute_script(
            "var s=document.getElementById(arguments[0]);"
            "return Array.from(s.options).filter(o=>o.selected).map(o=>o.value);",
            f"S{var}",
        )
        return sorted(got) == sorted(values) and radio.is_selected()

    def patched(cause_selector=None):
        base = orig(cause_selector)

        def setup(driver, dataset_config, query_name, year):
            if not base(driver, dataset_config, query_name, year):
                return False
            var, groups = age_spec(query_name.split("_", 1)[0])
            values = groups[band_of(query_name)]
            if values and not set_age_filter(driver, var, values):
                cdc_wonder_puller.log.error(f"    AGE FILTER NOT APPLIED for {query_name}; refusing")
                return False
            return True

        return setup

    cdc_wonder_puller.make_setup_func = patched

    # County: one all-states query per band x year instead of one per state
    # (a county x geography-only table is ~3,100 rows, far under WONDER's
    # ~75,000-row cap). state=None makes the runner leave the state filter alone.
    def expected_files(output_dir, queries, years, states=cdc_wonder_puller.US_STATES):
        planned = []
        for year in years:
            for q in queries:
                dest = Path(output_dir) / str(year) / f"{q}.csv"
                planned.append(cdc_wonder_puller.PlannedQuery(dest, q, year, None))
        return planned

    cdc_wonder_puller.expected_files = expected_files


def run_puller(scraper_dir, out_dir, levels, years, headless, bands):
    sys.path.insert(0, str(scraper_dir))
    import cdc_wonder_puller

    patch_puller(cdc_wonder_puller)
    names = {qname(lv, b): GROUPBY[lv] for lv in levels for b in bands}
    result = cdc_wonder_puller.pull_cdc_wonder(
        sn.DATASET_KEY, codes=sn.CODES, tree_path=list(sn.TREE_PATH),
        year_range=years, geography_levels=levels, age_grouping="ten_year",
        stratify_sex=False, stratify_race=False, extra_queries=names,
        output_dir=out_dir, only=list(names), headless=headless,
    )
    if result["failed"]:
        sys.exit("wonder-scraper reported failures; not writing the ageband files")


def footer_ages(path: Path, var: str) -> str:
    """The age filter line WONDER echoes in the export footer ("" if no age filter)."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.startswith('"' + FOOTER_AGE_KEY[var]):
                return line.strip().strip('"').split(": ", 1)[1]
    return ""


def expected_ages(band: str, level: str) -> str:
    label = {"1": "< 1 year"}
    return "; ".join(label.get(v, f"{v} years") for v in age_spec(level)[1][band])


def read_export(path: Path, year: int, level: str, band: str) -> list:
    var = age_spec(level)[0]
    if footer_ages(path, var) != expected_ages(band, level):
        sys.exit(f"{path}: age filter is {footer_ages(path, var)!r}, "
                 f"expected {expected_ages(band, level)!r}")
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for r in reader:
            if not r or r[0].startswith("---"):
                break
            if r[0].strip():  # "Total" summary row
                continue
            cells = [c.strip() for c in r[1:]]
            if level == "national":
                # grouped by Year: Year, Year Code, Deaths, Population, rate x5
                geo_name, geography, cells = "United States", "00", cells[2:]
                cells = [None, None] + cells
            else:
                geo_name, geography = cells[0], cells[1]
                cells = [None, None] + cells[2:]
            rows.append({
                "year": year, "level": level, "breakdown": "ageband",
                "file_state": None, "geo_name": geo_name, "geography": geography,
                "raw_category": band, "raw_category_code": band,
                "deaths": cells[2], "population": cells[3], "crude_rate": cells[4],
                "crude_rate_lcl": cells[5], "crude_rate_ucl": cells[6],
                "crude_rate_se": cells[7],
            })
    return rows


def consolidate(csv_dir: Path, level: str, years) -> pd.DataFrame:
    rows = []
    for year in range(years[0], years[1] + 1):
        for band in BANDS:
            q = qname(level, band)
            path = csv_dir / str(year) / f"{q}.csv"
            if not path.exists():
                sys.exit(f"missing export: {path}")
            got = read_export(path, year, level, band)
            if not got:
                sys.exit(f"no data rows in {path}")
            rows += got
    return pd.DataFrame(rows, columns=sn.COLUMNS)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--levels", nargs="+", default=["national", "state"],
                   choices=list(GROUPBY))
    p.add_argument("--years", nargs=2, type=int, default=list(sn.YEARS), metavar=("START", "END"))
    p.add_argument("--bands", nargs="+", default=BANDS, choices=BANDS,
                   help="subset of bands, for a smoke test (skips writing parquets)")
    p.add_argument("--scraper-dir", type=Path, help="checkout of PopHIVE/wonder-scraper")
    p.add_argument("--output-dir", type=Path,
                   help="keep the per-query CSVs here (default: a temp dir, deleted afterwards)")
    p.add_argument("--no-headless", action="store_true", help="show the browser")
    args = p.parse_args(argv)
    years = tuple(args.years)

    scraper_dir = sn.find_scraper(args.scraper_dir)
    tmp = None if args.output_dir else tempfile.TemporaryDirectory(prefix="wonder_ageband_")
    csv_dir = args.output_dir or Path(tmp.name)
    try:
        run_puller(scraper_dir, csv_dir, args.levels, years, not args.no_headless, args.bands)
        if args.bands != BANDS:
            print("subset of bands: CSVs kept in", csv_dir)
            return 0
        base = csv_dir / sn.DATASET_KEY if (csv_dir / sn.DATASET_KEY).is_dir() else csv_dir
        for level in args.levels:
            df = consolidate(base, level, years)
            out_file(level).parent.mkdir(exist_ok=True)
            df.to_parquet(out_file(level), compression="zstd", index=False)
            print(f"wrote {len(df)} rows -> {out_file(level)}")
    finally:
        if tmp:
            tmp.cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
