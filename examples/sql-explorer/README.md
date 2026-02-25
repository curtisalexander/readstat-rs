# SAS7BDAT SQL Explorer

An interactive browser-based tool for uploading `.sas7bdat` files and querying them with SQL — entirely client-side using WebAssembly.

## How It Works

1. Upload a `.sas7bdat` file (drag-and-drop or file picker)
2. The file is parsed in-browser via the `readstat-wasm` WebAssembly module
3. Data is loaded into [AlaSQL](https://github.com/AlaSQL/alasql), a client-side SQL engine
4. Write SQL queries in a syntax-highlighted editor (powered by CodeMirror 6)
5. View results in an interactive, sortable table (powered by Tabulator)
6. Export query results as CSV

No data leaves your browser — all processing happens locally.

## Quick Start

Serve the directory with any static HTTP server. The entire directory must be served (not just `index.html`) so the browser can load the `.js` and `.wasm` files alongside it.

From the repository root:

```bash
# Python
python -m http.server 8000 -d examples/sql-explorer

# Bun
bunx serve examples/sql-explorer
```

Or `cd` into the directory and serve from there:

```bash
cd examples/sql-explorer

# Python
python -m http.server 8000

# Bun
bunx serve .
```

Then open `http://localhost:8000` in your browser.

> **Note:** The page must be served over HTTP(S) — opening `index.html` directly as a `file://` URL won't work because browsers block WASM loading from the local filesystem.

## WASM Files

The `readstat_wasm.js` and `readstat_wasm.wasm` files are copies from `examples/web-demo/`. If you rebuild the WASM module, copy the updated files here as well.

To rebuild from source (requires Emscripten):

```bash
cd crates/readstat-wasm
./build.sh
cp pkg/readstat_wasm.js pkg/readstat_wasm.wasm ../../examples/sql-explorer/
```

## CDN Dependencies

All loaded automatically from CDNs — no `npm install` required:

| Library | Version | CDN | Purpose |
|---------|---------|-----|---------|
| [AlaSQL](https://github.com/AlaSQL/alasql) | 4.x | jsdelivr | Client-side SQL engine |
| [CodeMirror 6](https://codemirror.net/) | 6.x | esm.sh | SQL editor with syntax highlighting |
| [Tabulator](https://tabulator.info/) | 6.x | unpkg | Interactive sortable/filterable result tables |

## Example Queries

Once a file is loaded, the data is available as a table named `data`. Some queries to try:

```sql
-- Preview all rows
SELECT * FROM data LIMIT 100

-- Count rows
SELECT COUNT(*) AS total_rows FROM data

-- Filter rows
SELECT * FROM data WHERE column_name = 'value'

-- Aggregate
SELECT column_name, COUNT(*) AS n FROM data GROUP BY column_name ORDER BY n DESC

-- Select specific columns
SELECT col1, col2, col3 FROM data LIMIT 50
```

Column names with spaces or special characters should be wrapped in square brackets: `[Column Name]`.

For the full list of supported SQL syntax, see the [AlaSQL SQL Reference](https://github.com/AlaSQL/alasql/wiki/SQL%20statements).
