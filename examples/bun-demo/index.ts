import { readFileSync, writeFileSync } from "fs";
import { init, read_metadata, read_data } from "readstat-wasm";

// Initialize the WASM module
await init();

// Read a .sas7bdat file from disk
const bytes = readFileSync(
  new URL(
    "../../crates/readstat-tests/tests/data/cars.sas7bdat",
    import.meta.url,
  ),
);

const input = new Uint8Array(bytes);

// Parse metadata via WASM
const json = read_metadata(input);
const metadata = JSON.parse(json);

// Display metadata
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

// Read data as CSV
console.log();
console.log("=== CSV Data (preview) ===");
const csv = read_data(input);
const lines = csv.split("\n");
// Print header + first 5 data rows
const preview = lines.slice(0, 6);
for (const line of preview) {
  console.log(line);
}
if (lines.length > 7) {
  console.log(`... (${lines.length - 2} total data rows)`);
}

// Write full CSV to file
const outPath = new URL("cars.csv", import.meta.url);
writeFileSync(outPath, csv);
console.log();
console.log(`Wrote ${metadata.row_count} rows to cars.csv`);
