import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const output = resolve(root, "examples/sas-explorer/vendor");
const duckdb = resolve(root, "node_modules/@duckdb/duckdb-wasm/dist");
const arrow = resolve(root, "node_modules/apache-arrow");
const duckdbPackage = JSON.parse(
  await readFile(resolve(duckdb, "../package.json"), "utf8"),
);
const arrowPackage = JSON.parse(
  await readFile(resolve(arrow, "package.json"), "utf8"),
);

await rm(output, { force: true, recursive: true });
await mkdir(output, { recursive: true });

await build({
  bundle: true,
  entryPoints: [resolve(duckdb, "duckdb-browser.mjs")],
  format: "esm",
  minify: true,
  outfile: resolve(output, "duckdb-browser.mjs"),
  platform: "browser",
  sourcemap: false,
  target: ["es2022"],
});

for (const name of [
  "duckdb-browser-eh.worker.js",
  "duckdb-browser-mvp.worker.js",
  "duckdb-eh.wasm",
  "duckdb-mvp.wasm",
]) {
  await cp(resolve(duckdb, name), resolve(output, name));
}

const duckdbLicense = `DuckDB-Wasm ${duckdbPackage.version}
https://github.com/duckdb/duckdb-wasm

Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
`;
const arrowLicense = await readFile(resolve(arrow, "LICENSE.txt"), "utf8");
const arrowNotice = await readFile(resolve(arrow, "NOTICE.txt"), "utf8");
await writeFile(
  resolve(output, "THIRD_PARTY_NOTICES.txt"),
  `${duckdbLicense}\n\nApache Arrow JavaScript ${arrowPackage.version}\n${arrowLicense}\n${arrowNotice}`,
);

console.log(`Built self-hosted DuckDB-Wasm assets in ${output}`);
