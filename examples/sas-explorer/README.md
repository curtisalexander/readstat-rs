# SAS Explorer

A static, dependency-free browser frontend for inspecting and exporting local `.sas7bdat` files. SAS bytes are read and parsed only inside a dedicated module Web Worker; they never leave the browser. Complete datasets can be downloaded as CSV, NDJSON, Parquet, or Feather.

## Run locally

Place a compatible `readstat_wasm.wasm` beside these files, then serve the directory over HTTP (ES modules and workers do not work reliably from `file://`):

```sh
cd examples/sas-explorer
python3 -m http.server 8000
```

Open the printed server address in a modern browser. The WASM binary is intentionally not included here. The build supplied to this example must export `read_metadata`, `read_preview`, `read_data`, `read_data_ndjson`, `read_data_parquet`, and `read_data_feather`, plus `malloc`, `free`, `free_string`, `free_binary`, `readstat_last_error`, memory, and optionally `_initialize`. Serve `.wasm` as `application/wasm`; serving compressed assets should preserve `Content-Length` if determinate engine-download progress is desired.

The size, preview, and export policies are configured once in `worker.js`. The recommended file size is 250 MiB, the hard maximum is 500 MiB, previews are always bounded, and complete export is limited to source files no larger than 100 MiB. The lower export limit reflects the current full-materialization architecture; it is not a claim that every 100 MiB source will fit on every device.

Selected-column and bounded row-range exports are planned next. SQL exploration remains a later optional feature.
