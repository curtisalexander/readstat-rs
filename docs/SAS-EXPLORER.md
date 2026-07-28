# SAS Explorer design

## Status and first decision

This is a deferred product track. Do not start with a full explorer. The first
milestone is a deliberately minimal GitHub Pages proof that answers one question:

> Can the canonical single-threaded `readstat-wasm` bundle load on the project's
> existing GitHub Pages site, parse a local `.sas7bdat` file, and display one
> metadata value without uploading the file?

Only after that proof works in current Chrome, Firefox, Safari, and Edge should
the project choose an application architecture or add visualization and SQL.

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
- Native WASM failures are available through `readstat_last_error`, so a proof
  and later app can show actionable parse errors rather than a generic null
  result.

The existing demos intentionally materialize complete outputs in browser memory.
That is acceptable for a compatibility proof and small-file MVP, but it is not a
large-file architecture.

## Milestone 0: GitHub Pages compatibility proof

### User experience

One static page with:

1. A file input restricted to `.sas7bdat`.
2. A disabled status line that becomes `WASM ready` after initialization.
3. A `Read metadata` button.
4. Plain text output for file name, byte size, row count, variable count, and
   encoding.
5. A visible error region populated from the native WASM error.

No table library, SQL engine, framework, bundler, service worker, analytics,
export, drag-and-drop, or persistent storage belongs in this proof.

### Deployment shape

Add the proof under a stable subpath of the existing mdBook Pages artifact, for
example `/sas-explorer-proof/`. The current Pages workflow already uses GitHub's
official `upload-pages-artifact` and `deploy-pages` actions. Its build job should:

1. Build the book.
2. Copy the proof's `index.html`, browser wrapper, and canonical
   `readstat_wasm.wasm` into `target/book/sas-explorer-proof/`.
3. Assert those three files exist before artifact upload.

For the first proof, use the checked-in WASM binary so Pages validates hosting,
paths, MIME behavior, and browser execution independently from an Emscripten
build. Once proven, switch the Pages build to the same source build or verified
release artifact used by releases; do not maintain an untraceable fourth binary.

Use relative URLs (`./readstat_wasm.wasm`) so project Pages under
`/<repository>/` and a future custom domain both work.

### Technical constraints to verify

- The browser wrapper should fetch bytes and call `WebAssembly.instantiate`.
  `instantiateStreaming` may be added only with a fallback if a host serves an
  unexpected MIME type.
- The current module is single-threaded and does not use `SharedArrayBuffer`, so
  cross-origin isolation is not required. WebAssembly threads would require
  COOP/COEP headers; GitHub Pages does not provide general custom response-header
  control. Do not add a service-worker workaround to the proof.
- No file bytes leave the page. Test this in browser developer tools: after the
  initial static assets, selecting and parsing a file must cause no network
  request.
- Errors from malformed input must include the native parser message.
- A project-subpath deployment must not 404 its `.wasm` asset.

References:

- [GitHub Pages custom workflow documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
- [MDN `WebAssembly.instantiateStreaming`](https://developer.mozilla.org/en-US/docs/WebAssembly/Reference/JavaScript_interface/instantiateStreaming_static)
- [MDN shared-memory security requirements](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer#security_requirements)

### Acceptance criteria

- Deploys from the normal Pages workflow with no manual asset copy.
- Loads from the repository's project Pages subpath.
- Parses `cars.sas7bdat` and reports the expected nonzero row/variable counts.
- Rejects a three-byte invalid file with a useful native error.
- Confirmed on current desktop Chrome, Firefox, Safari, and Edge.
- Browser network logs show no upload after local file selection.
- A small automated browser smoke test can load the page and initialize WASM;
  file-picker automation is optional for this first proof.

## MVP after the proof

The smallest useful explorer should remain metadata-first:

1. Local file picker and privacy statement.
2. File-level metadata summary.
3. Searchable variable list showing name, logical/Arrow type, storage width,
   label, SAS format, and display width.
4. First 100 rows in a plain or virtualized table.
5. CSV download of the preview or selected data.
6. Clear file-size guidance and actionable parse/memory errors.

Parquet/Feather export can follow because the WASM exports already exist. SQL,
charts, summaries, multi-file support, and saved sessions are post-MVP.

## Architecture options after MVP

### Option A: Evolve `web-demo` with plain JavaScript

- **Advantages:** smallest dependency and deployment surface; existing code
  already proves the flow.
- **Costs:** the single HTML file will become difficult to test and maintain as
  state, table virtualization, and worker messaging grow.
- **Use when:** the product remains a focused metadata/preview utility.

### Option B: Small TypeScript application with a lightweight UI framework

- **Advantages:** typed metadata contracts, component boundaries, testable state,
  and easier worker integration.
- **Costs:** introduces a package manager, bundler, dependency updates, and base
  path configuration.
- **Use when:** MVP validation supports sustained product work. This is the
  likely long-term choice, but selecting a framework before the Pages proof is
  premature.

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
- WebAssembly threads or COOP/COEP workarounds.
- Matching desktop-scale file limits in the first release.
- Replacing the CLI, Rust API, or current examples.
- Choosing a framework before the compatibility proof is complete.
