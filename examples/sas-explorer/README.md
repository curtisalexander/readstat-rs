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

Open the printed server address in a modern browser. The WASM binary is intentionally not included here. The build supplied to this example must export `read_metadata`, `read_preview`, the full and `_reduced` variants of `read_data`, `read_data_ndjson`, `read_data_parquet`, and `read_data_feather`, plus `create_arrow_stream_session`, `read_arrow_stream_session_batch`, `free_arrow_stream_session`, `malloc`, `free`, `free_string`, `free_binary`, `readstat_last_error`, memory, and optionally `_initialize`. Serve `.wasm` as `application/wasm`; serving compressed assets should preserve `Content-Length` if determinate engine-download progress is desired.

The size, preview, export, and SQL policies are configured once in `worker.js`.
The recommended file size is 250 MiB, the hard maximum is 500 MiB, previews are
always bounded, and export is limited to source files no larger than 100 MiB.
The SQL experiment also accepts source files up to 100 MiB and loads at most
100,000 explicitly selected rows. Query display is capped at 500 rows.

Reduced exports still materialize their complete output in memory. SQL input is
different: Explorer creates one stateful WASM session that retains a source copy
and resolved metadata, then pulls up to 10,000 selected rows at a time as a
complete Arrow IPC stream. It awaits that batch's DuckDB insertion before
requesting the next batch. This provides backpressure, avoids a complete
intermediate Parquet buffer, and avoids copying the source into WASM for every
batch. DuckDB is lazy-loaded only when SQL is requested, query execution stays
in DuckDB's worker, and result batches are consumed incrementally. Each input
batch remains a separate bounded parse over the retained bytes. While data is
loading, the SQL panel shows the active read/insert phase, completed rows and
batches, elapsed time, and a determinate row progress bar.
