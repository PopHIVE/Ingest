#!/usr/bin/env python3
"""
Validate that the county overdose data fix resolves the ambiguity issue.
Run after build.R completes to verify FIPS and age are properly included.
"""
import pandas as pd

oc = pd.read_parquet("dist/overdose_by_geography_and_source_county.parquet")

# Check file structure
print("=== COUNTY FILE STRUCTURE ===")
print(f"Shape: {oc.shape}")
print(f"Columns: {list(oc.columns)}")
print(f"\nData types:\n{oc.dtypes}")

# Check FIPS inclusion
print("\n=== FIPS CODE VALIDATION ===")
if 'fips' in oc.columns:
    print(f"✓ FIPS column present")
    print(f"  Unique FIPS codes: {oc.fips.nunique()}")
    print(f"  Null FIPS values: {oc.fips.isna().sum()}")
else:
    print(f"✗ FIPS column MISSING")

# Check age stratum preservation
print("\n=== AGE STRATUM VALIDATION ===")
if 'age' in oc.columns:
    print(f"✓ Age column present")
    print(f"  Unique age groups: {oc.age.nunique()}")
    print(f"  Age groups: {oc.age.unique()}")
else:
    print(f"✗ Age column MISSING")

# Original validation metrics
print("\n=== DUPLICATE KEY ANALYSIS ===")
keys = oc.groupby(['fips', 'date', 'source']).size() if 'fips' in oc.columns else oc.groupby(['geography', 'date', 'source']).size()
nun = oc.groupby(['fips', 'date', 'source'] if 'fips' in oc.columns else ['geography', 'date', 'source'])['value'].nunique()

total_rows = len(oc)
unique_geographies = oc['geography'].nunique() if 'geography' in oc.columns else oc['fips'].nunique()
fully_duplicated = oc.duplicated().sum()
key_duplicates = (keys > 1).sum()
conflicting_duplicates = ((keys > 1) & (nun > 1)).sum()

print(f"Total rows:                {total_rows}")
print(f"Unique geographies (FIPS): {unique_geographies}")
print(f"Fully duplicated rows:     {fully_duplicated}")
print(f"Duplicate (fips, date, source) keys: {key_duplicates}")
print(f"Conflicting values in duplicates:    {conflicting_duplicates}")

# Expected targets (from issue description)
print("\n=== EXPECTED IMPROVEMENTS ===")
print(f"Expected unique FIPS:      ~3,100 (was 1,954 unique names)")
print(f"Expected duplicate keys:   0 (was 7,932)")
print(f"Expected conflicting values: 0 (was 7,487)")

# Sample Washington County to verify fix
print("\n=== WASHINGTON COUNTY TEST CASE ===")
if 'geography' in oc.columns:
    wash = oc[oc['geography'] == 'Washington County']
    if len(wash) > 0:
        print(f"Washington County rows: {len(wash)}")
        print(f"Unique values for 2021-07-01 CDC/NCHS:")
        test_case = wash[(wash['date'] == '2021-07-01') & (wash['source'] == 'CDC/NCHS')]
        if len(test_case) > 0:
            print(f"  {len(test_case)} rows with {test_case['value'].nunique()} distinct values")
            print(f"  FIPS codes: {test_case['fips'].unique()}")
        else:
            print("  (Test case not found)")
    else:
        print("Washington County not found in data")
