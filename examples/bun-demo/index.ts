import { readFileSync } from "fs";
import init, { read_metadata } from "readstat-wasm";

// Initialize the WASM module
await init();

// Read a .sas7bdat file from disk
const bytes = readFileSync(
  new URL(
    "../../crates/readstat-tests/tests/data/cars.sas7bdat",
    import.meta.url,
  ),
);

// Parse metadata via WASM
const json = read_metadata(new Uint8Array(bytes));
const metadata = JSON.parse(json);

// Display results
console.log("=== SAS7BDAT Metadata ===");
console.log(`Table name:    ${metadata.table_name}`);
console.log(`File encoding: ${metadata.file_encoding}`);
console.log(`Row count:     ${metadata.row_count}`);
console.log(`Variable count:${metadata.var_count}`);
console.log(`Compression:   ${metadata.compression}`);
console.log(`Endianness:    ${metadata.endianness}`);
console.log(`Created:       ${metadata.creation_time}`);
console.log(`Modified:      ${metadata.modified_time}`);
console.log();
console.log("=== Variables ===");
for (const [index, v] of Object.entries(metadata.vars) as [
  string,
  Record<string, unknown>,
][]) {
  console.log(`  [${index}] ${v.var_name} (${v.var_type}, ${v.var_format})`);
}
