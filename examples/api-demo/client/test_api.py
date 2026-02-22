# /// script
# requires-python = ">=3.9"
# dependencies = ["httpx"]
# ///
"""
Test the readstat API server endpoints.

Usage: uv run test_api.py [base_url] [sas_file]
Example: uv run test_api.py http://localhost:3000 ../test-data/cars.sas7bdat
"""

import sys

import httpx


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:3000"
    sas_file = sys.argv[2] if len(sys.argv) > 2 else "../test-data/cars.sas7bdat"

    print(f"=== Testing {base_url} with {sas_file} ===\n")

    with open(sas_file, "rb") as f:
        file_bytes = f.read()

    client = httpx.Client(base_url=base_url, timeout=30)

    # Health check
    print("--- GET /health ---")
    r = client.get("/health")
    r.raise_for_status()
    print(r.json())
    print()

    # Metadata
    print("--- POST /metadata ---")
    r = client.post("/metadata", files={"file": ("cars.sas7bdat", file_bytes)})
    r.raise_for_status()
    md = r.json()
    print(f"  row_count: {md['row_count']}")
    print(f"  var_count: {md['var_count']}")
    print(f"  table_name: {md.get('table_name', '')}")
    print(f"  encoding: {md.get('file_encoding', '')}")
    print(f"  variables: {len(md.get('vars', {}))}")
    print()

    # Preview
    print("--- POST /preview (5 rows) ---")
    r = client.post("/preview?rows=5", files={"file": ("cars.sas7bdat", file_bytes)})
    r.raise_for_status()
    lines = r.text.strip().split("\n")
    for line in lines:
        print(f"  {line}")
    print()

    # Data formats
    for fmt in ("csv", "ndjson", "parquet", "feather"):
        print(f"--- POST /data?format={fmt} ---")
        r = client.post(
            f"/data?format={fmt}", files={"file": ("cars.sas7bdat", file_bytes)}
        )
        r.raise_for_status()
        if fmt in ("csv", "ndjson"):
            preview_lines = r.text.strip().split("\n")[:3]
            for line in preview_lines:
                print(f"  {line[:120]}")
        else:
            print(f"  {len(r.content)} bytes")
        print()

    print("=== All tests passed ===")


if __name__ == "__main__":
    main()
