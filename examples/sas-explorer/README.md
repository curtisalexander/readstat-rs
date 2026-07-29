# SAS Explorer

A static, dependency-free browser frontend for inspecting local `.sas7bdat` files. SAS bytes are read and parsed only inside a dedicated module Web Worker; they never leave the browser.

## Run locally

Place a compatible `readstat_wasm.wasm` beside these files, then serve the directory over HTTP (ES modules and workers do not work reliably from `file://`):

```sh
cd examples/sas-explorer
python3 -m http.server 8000
```

Open the printed server address in a modern browser. The WASM binary is intentionally not included here. The build supplied to this example must export `read_metadata(ptr, len)` and `read_preview(ptr, len, row_limit)`, plus `malloc`, `free`, `free_string`, `readstat_last_error`, memory, and optionally `_initialize`. Serve `.wasm` as `application/wasm`; serving compressed assets should preserve `Content-Length` if determinate engine-download progress is desired.

The size and preview policies are configured once in `worker.js`. The recommended file size is 250 MiB, the hard maximum is 500 MiB, and previews are always bounded.

Exports and SQL exploration are planned; this version deliberately contains no export or SQL controls.
