# SAS Explorer

A static browser frontend for inspecting, exporting, and running bounded local
SQL queries over `.sas7bdat` files. SAS bytes are read and parsed only inside a
dedicated module Web Worker; they never leave the browser. Complete datasets or
selected variables and bounded row ranges can be downloaded as CSV, NDJSON,
Parquet, or Feather. Experimental read-only SQL uses a separately loaded
DuckDB-Wasm worker.

## Run locally

Build the self-hosted DuckDB assets, place a compatible `readstat_wasm.wasm`
beside these files, then serve the directory over HTTP (ES modules and workers
do not work reliably from `file://`):

```sh
npm ci
npm run build:sas-explorer-vendor
cd examples/sas-explorer
python3 -m http.server 8000
```

Open the printed server address in a modern browser. The WASM binary is intentionally not included here. The build supplied to this example must export `read_metadata`, `read_preview`, the full and `_reduced` variants of `read_data`, `read_data_ndjson`, `read_data_parquet`, and `read_data_feather`, plus `malloc`, `free`, `free_string`, `free_binary`, `readstat_last_error`, memory, and optionally `_initialize`. Serve `.wasm` as `application/wasm`; serving compressed assets should preserve `Content-Length` if determinate engine-download progress is desired.

The size, preview, export, and SQL policies are configured once in `worker.js`.
The recommended file size is 250 MiB, the hard maximum is 500 MiB, previews are
always bounded, and export is limited to source files no larger than 100 MiB.
The SQL experiment also accepts source files up to 100 MiB and loads at most
100,000 explicitly selected rows. Query display is capped at 500 rows.

Reduced export and SQL input limit parsed rows and columns, but each resulting
Parquet buffer is still materialized in memory. DuckDB is lazy-loaded only when
SQL is requested, query execution stays in DuckDB's worker, and result batches
are consumed incrementally. A chunked Arrow IPC parser interface remains the
next phase if these measured constraints justify continuing.
