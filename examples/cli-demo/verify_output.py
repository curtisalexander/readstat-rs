# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow", "polars"]
# ///
"""Verify that the output files produced by the CLI demo are valid.

Checks CSV, NDJSON, Parquet, and Feather files for correct row/column counts,
schema consistency, and data integrity.
"""

import sys
from pathlib import Path

import polars as pl
import pyarrow.feather as feather
import pyarrow.parquet as pq

here = Path(__file__).resolve().parent

EXPECTED_ROWS = 1081
EXPECTED_COLUMNS = 13
EXPECTED_COLUMN_NAMES = [
    "Brand", "Model", "Minivan", "Wagon", "Pickup", "Automatic",
    "EngineSize", "Cylinders", "CityMPG", "HwyMPG", "SUV", "AWD", "Hybrid",
]

errors: list[str] = []


def check(condition: bool, msg: str) -> None:
    if not condition:
        errors.append(msg)
        print(f"  FAIL: {msg}")
    else:
        print(f"  OK:   {msg}")


# --- CSV ---
print("=== CSV ===")
csv_path = here / "cars.csv"
df_csv = pl.read_csv(csv_path)
print(f"File:    {csv_path.name}")
print(f"Rows:    {df_csv.height}")
print(f"Columns: {df_csv.width}")
check(df_csv.height == EXPECTED_ROWS, f"row count == {EXPECTED_ROWS}")
check(df_csv.width == EXPECTED_COLUMNS, f"column count == {EXPECTED_COLUMNS}")
check(df_csv.columns == EXPECTED_COLUMN_NAMES, "column names match")
print(f"\nFirst 5 rows:\n{df_csv.head(5)}")
print()

# --- NDJSON ---
print("=== NDJSON ===")
ndjson_path = here / "cars.ndjson"
df_ndjson = pl.read_ndjson(ndjson_path)
print(f"File:    {ndjson_path.name}")
print(f"Rows:    {df_ndjson.height}")
print(f"Columns: {df_ndjson.width}")
check(df_ndjson.height == EXPECTED_ROWS, f"row count == {EXPECTED_ROWS}")
check(df_ndjson.width == EXPECTED_COLUMNS, f"column count == {EXPECTED_COLUMNS}")
check(df_ndjson.columns == EXPECTED_COLUMN_NAMES, "column names match")
print(f"\nFirst 5 rows:\n{df_ndjson.head(5)}")
print()

# --- Parquet ---
print("=== Parquet ===")
parquet_path = here / "cars.parquet"
table = pq.read_table(parquet_path)
print(f"File:    {parquet_path.name}")
print(f"Rows:    {table.num_rows}")
print(f"Columns: {table.num_columns}")
print(f"Schema:\n{table.schema}")
check(table.num_rows == EXPECTED_ROWS, f"row count == {EXPECTED_ROWS}")
check(table.num_columns == EXPECTED_COLUMNS, f"column count == {EXPECTED_COLUMNS}")
check(table.column_names == EXPECTED_COLUMN_NAMES, "column names match")
print(f"\nFirst 5 rows:\n{pl.from_arrow(table.slice(0, 5))}")
print()

# --- Feather ---
print("=== Feather ===")
feather_path = here / "cars.feather"
table = feather.read_table(feather_path)
print(f"File:    {feather_path.name}")
print(f"Rows:    {table.num_rows}")
print(f"Columns: {table.num_columns}")
print(f"Schema:\n{table.schema}")
check(table.num_rows == EXPECTED_ROWS, f"row count == {EXPECTED_ROWS}")
check(table.num_columns == EXPECTED_COLUMNS, f"column count == {EXPECTED_COLUMNS}")
check(table.column_names == EXPECTED_COLUMN_NAMES, "column names match")
print(f"\nFirst 5 rows:\n{pl.from_arrow(table.slice(0, 5))}")
print()

# --- Cross-format consistency ---
print("=== Cross-format consistency ===")
df_parquet = pl.from_arrow(pq.read_table(parquet_path))
df_feather = pl.from_arrow(feather.read_table(feather_path))
check(df_csv.equals(df_parquet), "CSV == Parquet")
check(df_csv.equals(df_feather), "CSV == Feather")
check(df_csv.equals(df_ndjson), "CSV == NDJSON")
print()

# --- Summary ---
if errors:
    print(f"FAILED: {len(errors)} check(s) failed")
    for e in errors:
        print(f"  - {e}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED")
