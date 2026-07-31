# SAS Explorer plan

## Status and product decisions

Milestones 0 and 1 are implemented in
[`examples/sas-explorer`](../examples/sas-explorer/) and deployed at
[curtisalexander.github.io/readstat-rs/explorer/](https://curtisalexander.github.io/readstat-rs/explorer/).
The modestly branded, desktop-oriented static app:

- serves at `/explorer/` beside the existing mdBook GitHub Pages site;
- reads and parses `.sas7bdat` files only in a dedicated Web Worker;
- states clearly that the selected file never leaves the browser;
- shows file metadata, a searchable variable list, and a bounded row preview;
- exports complete datasets as CSV, NDJSON, Parquet, or Feather inside the
  existing worker and downloads them with useful filenames and media types;
- reports WASM download, local file read, metadata, preview parse, and preview
  encoding state plus export parse and encoding state without blocking the UI;
- defaults to 100 preview rows, with choices from 25 through 1,000;
- recommends files no larger than 250 MiB and enforces a configurable 500 MiB
  hard maximum until browser measurements justify a different policy; and
- separately limits full export to 100 MiB source files because the current ABI
  materializes the parsed dataset and complete serialized output in memory.

The implementation intentionally uses plain JavaScript and CSS with no runtime
third-party dependencies. The production Pages deployment has been verified to
serve both the app and its WASM module successfully, including the
`application/wasm` content type. Pushes to `main` build and deploy the explorer
with the documentation; no release tag or separate repository is required.

**Next milestone:** add selected-column and bounded row-range export through a
new reduced-export WASM API. Optional lightweight SQL follows that.

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

## Milestone 0: worker-first GitHub Pages explorer (deployed)

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

### Technical constraints

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

### Acceptance status

- [x] Deploys from the normal Pages workflow with no manual asset copy.
- [x] Loads from the repository's project Pages subpath.
- [x] Serves the generated WASM with the correct MIME type.
- [x] Parses `cars.sas7bdat`, reports nonzero row/variable counts, and returns
  no more than the requested preview rows in the WASM smoke test.
- [x] Rejects an invalid preview row limit with a useful native error.
- [x] Reports bounded-preview and native progress through the package wrapper.
- [ ] Add an automated smoke test against the deployed Pages site.
- [x] Confirm current desktop Chromium behavior with the release WASM build.
- [ ] Confirm current desktop Firefox, Safari, and Edge behavior.
- [x] Confirm in Chromium network logs that selecting, parsing, and exporting a
  local file causes no network request.
- [x] Confirm malformed file input displays the native parser message in a
  release-build browser smoke test.

The unchecked browser-validation items are release-hardening work, not blockers
for starting export development.

## Milestone 1: selectable export (implemented)

Keep the current dependency-free, worker-first architecture. The first export
increment should use the existing full-data WASM exports rather than introducing
SQL or a new framework:

1. Add a format selector for CSV, NDJSON, Parquet, and Feather.
2. Send the export request to the existing dedicated worker; SAS bytes and all
   WASM calls remain worker-only.
3. Map the selected format to `read_data`, `read_data_ndjson`,
   `read_data_parquet`, or `read_data_feather` and return the generated output
   to the main thread for browser download with the correct extension and MIME
   type.
4. Show export parse and encoding progress using the existing native progress
   stages. Keep the interface responsive while output is generated and prevent
   overlapping preview/export operations.
5. Add clear export-specific memory guidance and a configurable export limit.
   Complete serialized output currently remains in memory, so the preview file
   limit must not automatically become the full-export limit.
6. Handle stale operations, allocation failures, parser errors, and download URL
   cleanup without retaining an old output or duplicate source bytes.

### Milestone 1 acceptance criteria

- [x] Each format downloads with a useful filename, extension, and MIME type.
- [x] CSV and NDJSON output is complete; downloaded Parquet and Feather output
  reads back as 1,081 rows by 13 columns in PyArrow for the browser smoke corpus.
- [x] Export parsing and encoding progress is visible, and the UI remains
  responsive while the worker performs the export.
- [x] Selecting/exporting a file causes no network request after static assets
  load in the Chromium smoke test.
- [x] A failed or disallowed export leaves the loaded dataset available for
  preview.
- [x] Export limits and their rationale are visible to the user and configurable
  in the worker's single policy object. The initial full-export source limit is
  100 MiB.

Generated-WASM CI exercises all four export functions, output signatures, row
completeness, and native progress stages. It also runs SAS Explorer in headless
Chromium, uploads the browser smoke corpus, downloads all four formats, reads
them independently with PyArrow, and compares every decoded value. Testing
against the deployed Pages URL and manual Firefox/Safari/Edge coverage remain
follow-up work rather than Milestone 1 blockers.

### Milestone 1b: reduced export (next)

After full-data format selection works, add selected-column and bounded row-range
export without routing the data through the UI thread. This likely requires a
new WASM API because the current four export functions serialize the complete
dataset. Define and test that API before adding reduction controls. SQL remains
out of scope for this increment.

All four full-data WASM exports already exist. Large output ultimately needs a
chunked/streaming design; accepting a file for metadata and bounded preview does
not imply that a full CSV export is safe at the same source size.

## Milestone 2: lightweight SQL and result export

SQL is secondary to preview and direct export. Its intended use is light local
exploration and reducing a dataset to selected rows or columns for re-export,
not building a full analytics environment. Choose the SQL engine only after the
bounded/reduced data interface exists, and keep it inside a worker so query work
does not block the UI.

## Later: SAS header visualization

Add a compact annotated byte map modeled on the exploratory diagrams in
`readstat-rs2/docs/phase1-header.html`. Distinguish the variable-size file-header
zone from the 336–344-byte parsed prefix, then divide that prefix into the fixed
164-byte start record, alignment-sensitive numeric middle, and fixed 120-byte
end record. Highlight the decisive bytes: `a2` at `0x20` for 32-/64-bit pointer
width, `a1` at `0x23` for the optional four-byte alignment pad, endian at `0x25`,
and encoding at `0x46`. Show how those values determine page-count width,
page-header size, subheader-pointer size, and the start of page 0 at
`header_size`. A side-by-side 32-/64-bit example and an annotated hex view of a
real corpus header would make the interpretation concrete. This is an
educational, later-stage feature and is not part of the export milestones.

## Architecture evolution options

### Option A: Continue SAS Explorer with plain JavaScript

- **Advantages:** smallest dependency and deployment surface; existing code
  and demos already prove the flow.
- **Costs:** the application will become difficult to test and maintain if
  state, table virtualization, and worker messaging grow substantially.
- **Current choice:** continue this while the product remains a focused
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
2. Keep all parsing and generation in the dedicated Web Worker. The current
   implementation sends the `File` handle to the worker, which reads and retains
   the bytes without creating a main-thread source buffer.
3. Extend the existing bounded preview API with a chunk/visitor WASM ABI for
   reduced and streaming exports; do not return complete large outputs at once.
4. Keep SQL optional. Evaluate engines only after a bounded data interface
   exists; do not solve SQL by silently materializing several full copies.

The worker and bounded preview portions are complete. Browser limit measurement
and streaming/reduced export remain separate library/API projects; they are not
prerequisites for the initial selectable full-data export UI.

## Explicit non-goals

- Server uploads or server-side parsing.
- Automatic numeric type narrowing.
- A service worker for CPU work. Parsing belongs in the dedicated Web Worker.
- WebAssembly threads or COOP/COEP workarounds unless measurements show that a
  single worker is insufficient.
- Mobile layout or matching native desktop-scale file limits in the first
  releases.
- Replacing the CLI, Rust API, or current examples.
- Choosing a framework while the focused plain-JavaScript application remains
  maintainable.
