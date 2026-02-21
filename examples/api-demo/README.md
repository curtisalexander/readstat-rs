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

### Python Server (port 3001)

```bash
cd examples/api-demo/python-server

# Build the PyO3 bindings into the project venv
uv sync
uv run maturin develop -m readstat_py/Cargo.toml

# Start the server
uv run uvicorn server:app --port 3001
```

### Run Tests

```bash
# Shell script (against either server)
cd examples/api-demo
bash client/test_api.sh http://localhost:3000 test-data/cars.sas7bdat
bash client/test_api.sh http://localhost:3001 test-data/cars.sas7bdat

# Python script (uses uv for dependency management, no setup needed)
cd examples/api-demo/client
uv run test_api.py http://localhost:3000 ../test-data/cars.sas7bdat
```

## API Reference

| Method | Path | Request | Response |
|--------|------|---------|----------|
| `GET` | `/health` | — | `{"status": "ok"}` |
| `POST` | `/metadata` | multipart file upload | JSON metadata |
| `POST` | `/preview?rows=N` | multipart file upload | CSV text (first N rows, default 10) |
| `POST` | `/data?format=fmt` | multipart file upload | Data in requested format |

**Supported formats:** `csv`, `ndjson`, `parquet`, `feather`

The file field name in the multipart upload must be `file`.

## Example curl Commands

```bash
FILE=test-data/cars.sas7bdat

# Health check
curl http://localhost:3000/health

# Get metadata
curl -F "file=@$FILE" http://localhost:3000/metadata

# Preview first 5 rows as CSV
curl -F "file=@$FILE" "http://localhost:3000/preview?rows=5"

# Convert to Parquet
curl -F "file=@$FILE" "http://localhost:3000/data?format=parquet" -o output.parquet

# Convert to NDJSON
curl -F "file=@$FILE" "http://localhost:3000/data?format=ndjson"
```

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
