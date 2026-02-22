# API Server Demo

Two identical API servers demonstrating how to integrate `readstat` into backend applications:

- **Rust server** (Axum) — direct library integration
- **Python server** (FastAPI) — cross-language integration via PyO3/maturin bindings

Both servers expose the same endpoints and return identical results for the same input.

## Prerequisites

**Rust server:**
- Rust toolchain
- `libclang` (for readstat-sys bindgen)
- Git submodules initialized: `git submodule update --init --recursive`

**Python server:**
- Everything above, plus:
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Python 3.9+

## Quick Start

### Rust Server (port 3000)

```bash
cd examples/api-demo/rust-server
cargo run
```

You should see:

```
Rust API server listening on http://localhost:3000
```

### Python Server (port 3001)

```bash
cd examples/api-demo/python-server

# Build the PyO3 bindings into the project venv
uv sync
uv run maturin develop -m readstat_py/Cargo.toml

# Start the server
uv run uvicorn server:app --port 3001
```

You should see:

```
INFO:     Started server process [...]
INFO:     Uvicorn running on http://127.0.0.1:3001 (Press CTRL+C to quit)
```

## Walking Through the Endpoints

The examples below use port 3000 (Rust server). Replace with 3001 for the Python server — the responses are identical.

Set a convenience variable for the test file:

```bash
FILE=test-data/cars.sas7bdat
```

### 1. Health Check

```bash
curl http://localhost:3000/health
```

Expected output:

```json
{"status":"ok"}
```

### 2. File Metadata

Upload a SAS file and get back its metadata as JSON:

```bash
curl -F "file=@$FILE" http://localhost:3000/metadata
```

Expected output (formatted):

```json
{
  "row_count": 1081,
  "var_count": 13,
  "table_name": "CARS",
  "file_label": "Written by SAS",
  "file_encoding": "WINDOWS-1252",
  "version": 9,
  "is64bit": 0,
  "creation_time": "2008-09-30 12:55:01",
  "modified_time": "2008-09-30 12:55:01",
  "compression": "None",
  "endianness": "Little",
  "vars": {
    "0": {
      "var_name": "Brand",
      "var_type": "String",
      "var_type_class": "String",
      "var_label": "",
      "var_format": "",
      "var_format_class": null,
      "storage_width": 13,
      "display_width": 0
    },
    "1": {
      "var_name": "Model",
      "var_type": "String",
      "var_type_class": "String",
      ...
    },
    ...
  }
}
```

The `vars` map is keyed by column index and includes type info, labels, and SAS format metadata for all 13 variables.

### 3. Preview Rows

Get the first N rows as CSV (default 10, here we ask for 5):

```bash
curl -F "file=@$FILE" "http://localhost:3000/preview?rows=5"
```

Expected output:

```csv
Brand,Model,Minivan,Wagon,Pickup,Automatic,EngineSize,Cylinders,CityMPG,HwyMPG,SUV,AWD,Hybrid
TOYOTA,Prius,0.0,0.0,0.0,1.0,1.5,4.0,60.0,51.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,1.0,1.3,4.0,48.0,47.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,1.0,1.3,4.0,47.0,48.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,0.0,1.3,4.0,46.0,51.0,0.0,0.0,1.0
HONDA,Civic Hybrid,0.0,0.0,0.0,0.0,1.3,4.0,45.0,51.0,0.0,0.0,1.0
```

### 4. Convert to CSV

Export the full dataset (all 1,081 rows) as CSV:

```bash
curl -F "file=@$FILE" "http://localhost:3000/data?format=csv" -o output.csv
```

The response has `Content-Type: text/csv` and `Content-Disposition: attachment; filename="data.csv"`.

### 5. Convert to NDJSON

Export as newline-delimited JSON (one JSON object per row):

```bash
curl -F "file=@$FILE" "http://localhost:3000/data?format=ndjson"
```

Expected output (first few lines):

```json
{"Brand":"TOYOTA","Model":"Prius","Minivan":0.0,"Wagon":0.0,"Pickup":0.0,"Automatic":1.0,"EngineSize":1.5,"Cylinders":4.0,"CityMPG":60.0,"HwyMPG":51.0,"SUV":0.0,"AWD":0.0,"Hybrid":1.0}
{"Brand":"HONDA","Model":"Civic Hybrid","Minivan":0.0,"Wagon":0.0,"Pickup":0.0,"Automatic":1.0,"EngineSize":1.3,"Cylinders":4.0,"CityMPG":48.0,"HwyMPG":47.0,"SUV":0.0,"AWD":0.0,"Hybrid":1.0}
{"Brand":"HONDA","Model":"Civic Hybrid","Minivan":0.0,"Wagon":0.0,"Pickup":0.0,"Automatic":1.0,"EngineSize":1.3,"Cylinders":4.0,"CityMPG":47.0,"HwyMPG":48.0,"SUV":0.0,"AWD":0.0,"Hybrid":1.0}
...
```

The response has `Content-Type: application/x-ndjson`.

### 6. Convert to Parquet

Export as Apache Parquet (binary, Snappy-compressed):

```bash
curl -F "file=@$FILE" "http://localhost:3000/data?format=parquet" -o output.parquet
```

This produces a ~15 KB Parquet file. You can inspect it with tools like `parquet-tools`, DuckDB, or pandas:

```python
import pandas as pd
print(pd.read_parquet("output.parquet").head())
```

### 7. Convert to Feather

Export as Arrow IPC (Feather v2) format:

```bash
curl -F "file=@$FILE" "http://localhost:3000/data?format=feather" -o output.feather
```

This produces a ~130 KB Feather file. Read it back with any Arrow-compatible tool:

```python
import pandas as pd
print(pd.read_feather("output.feather").head())
```

## Automated Test Scripts

Both scripts work against either server — just change the URL.

### Shell script (curl)

```bash
cd examples/api-demo
bash client/test_api.sh http://localhost:3000 test-data/cars.sas7bdat
bash client/test_api.sh http://localhost:3001 test-data/cars.sas7bdat
```

### Python script (httpx)

Uses [PEP 723](https://peps.python.org/pep-0723/) inline script metadata, so `uv run` handles dependencies automatically — no virtual environment setup needed:

```bash
cd examples/api-demo/client
uv run test_api.py http://localhost:3000 ../test-data/cars.sas7bdat
uv run test_api.py http://localhost:3001 ../test-data/cars.sas7bdat
```

Expected output:

```
=== Testing http://localhost:3000 with ../test-data/cars.sas7bdat ===

--- GET /health ---
{'status': 'ok'}

--- POST /metadata ---
  row_count: 1081
  var_count: 13
  table_name: CARS
  encoding: WINDOWS-1252
  variables: 13

--- POST /preview (5 rows) ---
  Brand,Model,Minivan,Wagon,Pickup,Automatic,EngineSize,Cylinders,CityMPG,HwyMPG,SUV,AWD,Hybrid
  TOYOTA,Prius,0.0,0.0,0.0,1.0,1.5,4.0,60.0,51.0,0.0,0.0,1.0
  ...

--- POST /data?format=csv ---
  Brand,Model,Minivan,Wagon,Pickup,Automatic,EngineSize,Cylinders,CityMPG,HwyMPG,SUV,AWD,Hybrid
  TOYOTA,Prius,0.0,0.0,0.0,1.0,1.5,4.0,60.0,51.0,0.0,0.0,1.0
  HONDA,Civic Hybrid,0.0,0.0,0.0,1.0,1.3,4.0,48.0,47.0,0.0,0.0,1.0

--- POST /data?format=ndjson ---
  {"Brand":"TOYOTA","Model":"Prius","Minivan":0.0,...}
  ...

--- POST /data?format=parquet ---
  15403 bytes

--- POST /data?format=feather ---
  129650 bytes

=== All tests passed ===
```

## API Reference

| Method | Path | Request | Response | Content-Type |
|--------|------|---------|----------|--------------|
| `GET` | `/health` | — | `{"status": "ok"}` | `application/json` |
| `POST` | `/metadata` | multipart `file` | JSON metadata | `application/json` |
| `POST` | `/preview?rows=N` | multipart `file` | CSV text (first N rows, default 10) | `text/csv` |
| `POST` | `/data?format=csv` | multipart `file` | Full dataset as CSV | `text/csv` |
| `POST` | `/data?format=ndjson` | multipart `file` | Full dataset as NDJSON | `application/x-ndjson` |
| `POST` | `/data?format=parquet` | multipart `file` | Full dataset as Parquet | `application/octet-stream` |
| `POST` | `/data?format=feather` | multipart `file` | Full dataset as Feather | `application/octet-stream` |

The multipart field name must be `file`. Binary formats include a `Content-Disposition` header with a suggested filename.

## How It Works

### Rust Server

```
HTTP upload → Axum multipart extraction → Vec<u8>
  → spawn_blocking {
      ReadStatMetadata::read_metadata_from_bytes()
      ReadStatData::read_data_from_bytes() → Arrow RecordBatch
      write_batch_to_{csv,ndjson,parquet,feather}_bytes()
    }
  → HTTP response
```

All ReadStat C library FFI calls run inside `spawn_blocking` to avoid blocking the tokio async runtime.

### Python Server

```
HTTP upload → FastAPI UploadFile → bytes
  → readstat_py.read_to_{csv,ndjson,parquet,feather}(bytes)
    → [PyO3 boundary]
      → ReadStatMetadata::read_metadata_from_bytes()
      → ReadStatData::read_data_from_bytes() → Arrow RecordBatch
      → write_batch_to_*_bytes()
    → [back to Python]
  → HTTP response
```

The PyO3 binding layer is intentionally thin — 5 functions that take `&[u8]` and return `Vec<u8>` (or `String` for metadata). No complex types cross the FFI boundary.
