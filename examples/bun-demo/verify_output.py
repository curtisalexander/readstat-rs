# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
"""Verify that the Parquet and Feather files produced by bun-demo are valid."""

from pathlib import Path

import pyarrow.feather as feather
import pyarrow.parquet as pq

here = Path(__file__).resolve().parent

print("=== Parquet ===")
parquet_path = here / "cars.parquet"
table = pq.read_table(parquet_path)
print(f"File:    {parquet_path.name}")
print(f"Rows:    {table.num_rows}")
print(f"Columns: {table.num_columns}")
print(f"Schema:\n{table.schema}")
print()

print("=== Feather ===")
feather_path = here / "cars.feather"
table = feather.read_table(feather_path)
print(f"File:    {feather_path.name}")
print(f"Rows:    {table.num_rows}")
print(f"Columns: {table.num_columns}")
print(f"Schema:\n{table.schema}")
