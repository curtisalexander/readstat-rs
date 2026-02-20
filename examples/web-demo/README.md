# Web Demo: SAS7BDAT Viewer & Converter

Browser-based demo that reads SAS `.sas7bdat` files entirely client-side using WebAssembly. Upload a file to view metadata, preview data in a sortable table, and export to CSV, NDJSON, Parquet, or Feather.

No build tools, no `npm install`, no framework — just static files served over HTTP.

## Quick start

1. **Copy the WASM binary** into this directory (if not already present):

   ```bash
   cp crates/readstat-wasm/pkg/readstat_wasm.wasm examples/web-demo/
   ```

   If you need to rebuild it first, see the [bun-demo README](../bun-demo/README.md) for build instructions.

2. **Serve the directory** with any static HTTP server. You must point the server at the **directory**, not at `index.html` directly:

   ```bash
   # From the repo root:
   python -m http.server 8000 -d examples/web-demo
   npx serve examples/web-demo
   bunx serve examples/web-demo

   # Or from the web-demo directory:
   cd examples/web-demo
   python -m http.server 8000
   npx serve
   bunx serve
   ```

   > **Note:** Do not pass `index.html` as the argument (e.g., `bunx serve index.html`). That tells `serve` to look for a directory named `index.html`, which will cause the WASM and JS files to 404.

3. **Open** `http://localhost:3000` (for `serve`) or `http://localhost:8000` (for Python) in your browser.

4. **Upload** a `.sas7bdat` file (e.g., `crates/readstat-tests/tests/data/cars.sas7bdat`).

## Features

- **Metadata panel** — table name, encoding, row/variable count, compression, timestamps
- **Variable table** — name, type, label, and format for each column
- **Data preview** — first 100 rows in a sortable table (uses [Tabulator](https://tabulator.info/) from CDN, with plain HTML table fallback)
- **Export** — download as CSV, NDJSON, Parquet, or Feather

## WASM binary

The `readstat_wasm.wasm` file is built from the `readstat-wasm` crate (`crates/readstat-wasm/`). It compiles the ReadStat C library and the Rust `readstat` parsing library to WebAssembly via the `wasm32-unknown-emscripten` target. The binary is ~2.7 MB.

A pre-built copy is checked in at `crates/readstat-wasm/pkg/readstat_wasm.wasm`.

## Browser compatibility

- Requires a modern browser with WebAssembly support (Chrome 57+, Firefox 52+, Safari 11+, Edge 16+)
- Must be served over HTTP(S) — `file://` URLs will not work due to WASM `fetch()` requirements
- Tabulator.js is loaded from CDN; if offline, the data preview falls back to a plain HTML table

## File structure

```
examples/web-demo/
├── index.html          # App (HTML + inline CSS + inline JS)
├── readstat_wasm.js    # Browser-compatible WASM wrapper
├── readstat_wasm.wasm  # WASM binary (copied from pkg/)
└── README.md           # This file
```
