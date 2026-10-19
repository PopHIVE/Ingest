"""
CDC WONDER national drowning scraper
====================================
Pulls the national-level drowning rows that ingest.R cannot get from the
manually downloaded state/county exports, and saves them as

    raw/national_ucd_icd10_expanded_2018_2024.parquet

(same columns as raw/ucd_icd10_expanded_2018_2024.parquet, with level =
"national", geography = "00"). ingest.R reads that file and appends the rows to
standard/data_state.csv.gz; this script does no standardizing.

Why a separate national pull: CDC WONDER suppresses every state cell with 1-9
deaths, so summing the state rows undercounts (2024: 4,087 from the state cells
vs 4,201 reported nationally). The national total has to come from WONDER's own
national query.

Scraping is done by https://github.com/PopHIVE/wonder-scraper (Selenium driving
the live query form), national queries only: one per year x breakdown
(total, ten-year age, sex, race, Hispanic origin) = 35 queries, a few minutes.
Same database and cause as the other drowning data: Underlying Cause of Death
2018-2024, Single Race, ICD-10 W65-W74 (W71/W72 were never assigned).

One-time setup (needs Chrome; run from this folder):
    cd data/drowning          # from the repo root
    python -m venv .venv
    .venv/Scripts/python -m pip install selenium webdriver-manager pandas pyarrow \\
        "git+https://github.com/govex/cdc-wonder-scraper.git"

Run:
    .venv/Scripts/python scrape_national.py
    .venv/Scripts/python scrape_national.py --scraper-dir C:/path/to/wonder-scraper
    .venv/Scripts/python scrape_national.py --no-headless      # watch the browser

The wonder-scraper checkout is taken from --scraper-dir, else $WONDER_SCRAPER_DIR,
else cloned into .wonder-scraper/ next to this script (git-ignored).
"""

import argparse
import csv
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/PopHIVE/wonder-scraper.git"
OUT_FILE = HERE / "raw" / "national_ucd_icd10_expanded_2018_2024.parquet"

DATASET_KEY = "ucd_icd10_expanded_2018_2024"
CODES = "W65-W74"          # exact label in WONDER's ICD Browse tree
TREE_PATH = ["V01-Y89"]    # injury chapter that has to be expanded to reveal it
YEARS = (2018, 2024)
AGE_GROUPING = "ten_year"  # D158.V5, matching the state/county pulls

# puller query name -> `breakdown` value in the raw parquet. "total" is new:
# the other raw slices have no plain national total.
BREAKDOWNS = {
    "national_total": "total",
    "national_age": "age",
    "national_gender": "gender",
    "national_race": "race",
    "national_ethnicity": "ethnicity",
}

COLUMNS = [
    "year", "level", "breakdown", "file_state", "geo_name", "geography",
    "raw_category", "raw_category_code", "deaths", "population", "crude_rate",
    "crude_rate_lcl", "crude_rate_ucl", "crude_rate_se",
]


def find_scraper(scraper_dir=None) -> Path:
    path = scraper_dir or os.environ.get("WONDER_SCRAPER_DIR")
    path = Path(path) if path else HERE / ".wonder-scraper"
    if not (path / "cdc_wonder_puller.py").exists():
        if scraper_dir or os.environ.get("WONDER_SCRAPER_DIR"):
            sys.exit(f"no cdc_wonder_puller.py in {path}")
        print(f"cloning {REPO_URL} -> {path}")
        subprocess.run(["git", "clone", "--depth", "1", REPO_URL, str(path)], check=True)
    return path


def run_puller(scraper_dir: Path, out_dir: Path, headless: bool) -> None:
    sys.path.insert(0, str(scraper_dir))
    import cdc_wonder_puller

    argv = [
        "--dataset", DATASET_KEY,
        "--codes", CODES,
        "--tree-path", *TREE_PATH,
        "--levels", "national",
        "--age", AGE_GROUPING,
        "--years", str(YEARS[0]), str(YEARS[1]),
        "--output-dir", str(out_dir),
    ]
    if not headless:
        argv.append("--no-headless")
    if cdc_wonder_puller.main(argv) != 0:
        sys.exit("wonder-scraper reported failures; not writing the national file")


def read_export(path: Path, year: int, breakdown: str) -> list:
    """Rows of one WONDER export: header, data rows, then a '---' footer."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for r in reader:
            if not r or r[0].startswith("---"):
                break
            if r[0].strip():  # "Total" summary row; the 'Notes' cell is blank on data rows
                continue
            cells = [c.strip() for c in r[1:]]
            rows.append({
                "year": year, "level": "national", "breakdown": breakdown,
                "file_state": None, "geo_name": "United States", "geography": "00",
                "raw_category": cells[0], "raw_category_code": cells[1],
                "deaths": cells[2], "population": cells[3], "crude_rate": cells[4],
                "crude_rate_lcl": cells[5], "crude_rate_ucl": cells[6],
                "crude_rate_se": cells[7],
            })
    return rows


def consolidate(csv_dir: Path) -> pd.DataFrame:
    rows = []
    for year in range(YEARS[0], YEARS[1] + 1):
        for qname, breakdown in BREAKDOWNS.items():
            path = csv_dir / str(year) / f"{qname}.csv"
            if not path.exists():
                sys.exit(f"missing export: {path}")
            got = read_export(path, year, breakdown)
            if not got:
                sys.exit(f"no data rows in {path}")
            rows += got
    return pd.DataFrame(rows, columns=COLUMNS)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--scraper-dir", type=Path, help="checkout of PopHIVE/wonder-scraper")
    p.add_argument("--output-dir", type=Path,
                   help="keep the per-query CSVs here (default: a temp dir, deleted afterwards)")
    p.add_argument("--no-headless", action="store_true", help="show the browser")
    args = p.parse_args(argv)

    scraper_dir = find_scraper(args.scraper_dir)
    tmp = None if args.output_dir else tempfile.TemporaryDirectory(prefix="wonder_national_")
    csv_dir = args.output_dir or Path(tmp.name)
    try:
        run_puller(scraper_dir, csv_dir, headless=not args.no_headless)
        df = consolidate(csv_dir / DATASET_KEY if (csv_dir / DATASET_KEY).is_dir() else csv_dir)
    finally:
        if tmp:
            tmp.cleanup()

    OUT_FILE.parent.mkdir(exist_ok=True)
    df.to_parquet(OUT_FILE, compression="zstd", index=False)
    total = df[df.breakdown == "total"].set_index("year")["deaths"]
    print(f"wrote {len(df)} rows -> {OUT_FILE}")
    print("national total deaths by year:", total.to_dict())
    return 0


if __name__ == "__main__":
    sys.exit(main())
