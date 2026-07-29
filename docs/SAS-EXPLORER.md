# SAS Explorer design

## Status and product decisions

Implementation is in progress in [`examples/sas-explorer`](../examples/sas-explorer/).
The first public version is a modestly branded, desktop-oriented static app that:

- serves at `/explorer/` beside the existing mdBook GitHub Pages site;
- reads and parses `.sas7bdat` files only in a dedicated Web Worker;
- states clearly that the selected file never leaves the browser;
- shows file metadata, a searchable variable list, and a bounded row preview;
- reports WASM download, local file read, metadata, preview parse, and preview
  encoding state without blocking the UI;
- defaults to 100 preview rows, with choices from 25 through 1,000;
- recommends files no larger than 250 MiB and enforces a configurable 500 MiB
  hard maximum until browser measurements justify a different policy.

The implementation intentionally uses plain JavaScript and CSS with no runtime
third-party dependencies. Export is the next phase, followed by optional light
SQL for exploration and reduced-result export.

## Existing assets and lessons

The repository already contains most of the technical experiments an explorer
would otherwise need:

- [`examples/web-demo`](../examples/web-demo/) proves browser-local metadata,
  preview, sorting, and CSV/NDJSON/Parquet/Feather export.
- [`examples/sql-explorer`](../examples/sql-explorer/) proves browser-local SQL
  through AlaSQL, CodeMirror, and Tabulator.
- [`examples/bun-demo`](../examples/bun-demo/) exercises the canonical Node/Bun
  package wrapper.
- The release workflow produces a versioned WASM bundle containing the Node
  wrapper, binary, and package manifest. Browser code must keep using a browser
  wrapper: the package wrapper imports Node filesystem modules.
- `read_preview(bytes, row_limit)` applies the row bound while parsing and
  returns NDJSON; it does not serialize the whole dataset and truncate in the UI.
- Browser WASM hosts provide `env.readstat_progress(stage, current, total)`.
  Stages cover metadata, preview parsing/encoding, and export parsing/encoding.
- Native WASM failures are available through `readstat_last_error`, so a proof
  and later app can show actionable parse errors rather than a generic null
  result.

The existing demos intentionally materialize complete outputs in browser memory.
That is acceptable for a compatibility proof and small-file MVP, but it is not a
large-file architecture.

## Milestone 0: worker-first GitHub Pages explorer

### User experience

One static application with a local file picker and drag/drop, visible operation
progress, a dataset summary, searchable variable metadata, and a sortable
bounded preview. There is no SQL engine, framework, bundler, service worker,
analytics, export UI, or persistent storage in this milestone.

### Deployment shape

Add the app at `/explorer/` in the existing mdBook Pages artifact. The current
Pages workflow uses GitHub's official `upload-pages-artifact` and `deploy-pages`
actions. Its build job:

1. Installs Emscripten and source-builds `readstat-wasm`.
2. Builds the book.
3. Copies the explorer HTML/JavaScript and newly built canonical WASM module
   into `target/book/explorer/`.
4. Asserts the required app, worker, and WASM files exist before artifact upload.

This avoids deploying a stale duplicate binary and ensures an explorer ABI
change and its caller are published atomically from the same commit.

Use relative URLs (`./readstat_wasm.wasm`) so project Pages under
`/<repository>/` and a future custom domain both work.

### Technical constraints to verify

- The browser wrapper streams the fetch response to report byte progress, then
  calls `WebAssembly.instantiate`. If `Content-Length` is unavailable, the UI
  uses indeterminate download progress.
- The current module is single-threaded and does not use `SharedArrayBuffer`, so
  cross-origin isolation is not required. WebAssembly threads would require
  COOP/COEP headers; GitHub Pages does not provide general custom response-header
  control. Do not add a service-worker workaround to the proof.
- The main thread sends the browser `File` handle to a dedicated worker. The
  worker uses `FileReader` progress events, retains the bytes, and performs every
  WASM call. No file bytes leave the page. Test this in browser developer tools:
  after the initial static assets, selecting and parsing a file must cause no
  network request.
- Errors from malformed input must include the native parser message.
- A project-subpath deployment must not 404 its `.wasm` asset.

References:

- [GitHub Pages custom workflow documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
- [MDN `WebAssembly.instantiateStreaming`](https://developer.mozilla.org/en-US/docs/WebAssembly/Reference/JavaScript_interface/instantiateStreaming_static)
- [MDN shared-memory security requirements](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer#security_requirements)

### Acceptance criteria

- Deploys from the normal Pages workflow with no manual asset copy.
- Loads from the repository's project Pages subpath.
- Parses `cars.sas7bdat`, reports nonzero row/variable counts, and returns no
  more than the requested preview rows.
- Rejects a three-byte invalid file with a useful native error.
- Confirmed on current desktop Chrome, Firefox, Safari, and Edge.
- Browser network logs show no upload after local file selection.
- CI source-builds the WASM module and smoke-tests bounded preview and native
  progress through the package wrapper. A deployed browser smoke test remains
  desirable after the first Pages deployment.

## Export phase

The next phase adds output selection while remaining worker-first:

1. CSV, NDJSON, Parquet, and Feather output choices.
2. Selected-column and row-range export before SQL is introduced.
3. Worker-side generation with parse and encoding progress.
4. Export-specific size guidance because complete serialized output currently
   remains in memory before browser download.

All four full-data WASM exports already exist. Large output needs separate limits
and later a chunked/streaming design; accepting a file for metadata and bounded
preview does not imply that a full CSV export is safe at the same source size.

## Architecture evolution options

### Option A: Evolve `web-demo` with plain JavaScript

- **Advantages:** smallest dependency and deployment surface; existing code
  already proves the flow.
- **Costs:** the single HTML file will become difficult to test and maintain as
  state, table virtualization, and worker messaging grow.
- **Current choice:** use this while the product remains a focused
  metadata/preview/export utility.

### Option B: Small TypeScript application with a lightweight UI framework

- **Advantages:** typed metadata contracts, component boundaries, testable state,
  and easier worker integration.
- **Costs:** introduces a package manager, bundler, dependency updates, and base
  path configuration.
- **Use when:** validation supports sustained product work whose state and UI
  complexity exceed the plain application.

### Option C: Evolve `sql-explorer`

- **Advantages:** SQL editor and result table already exist.
- **Costs:** AlaSQL, CodeMirror, and Tabulator are substantial dependencies;
  complete CSV materialization duplicates data and makes SQL the architecture
  rather than an optional feature.
- **Use when:** user research says SQL is the primary workflow. It should not be
  the default starting point.

## Large-file direction

The current WASM ABI accepts a complete byte buffer and returns complete output,
so browser peak memory can include the source file, WASM copy, Arrow data, and
serialized result simultaneously. Before advertising large-file support:

1. Measure practical limits by browser and device class.
2. Move parsing off the UI thread with a dedicated Web Worker. A worker does not
   require shared memory; transfer the source `ArrayBuffer` rather than copy it.
3. Design a chunk/visitor WASM ABI that returns bounded batches or preview rows.
4. Keep SQL optional. Evaluate engines only after a bounded data interface
   exists; do not solve SQL by silently materializing several full copies.

These are separate library/API projects and are not prerequisites for the Pages
proof or metadata-first MVP.

## Explicit non-goals

- Server uploads or server-side parsing.
- Automatic numeric type narrowing.
- A service worker for CPU work. Parsing belongs in the dedicated Web Worker.
- WebAssembly threads or COOP/COEP workarounds unless measurements show that a
  single worker is insufficient.
- Matching desktop-scale file limits in the first release.
- Replacing the CLI, Rust API, or current examples.
- Choosing a framework before the compatibility proof is complete.
