import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let instance;
let memory;

/** Provide the minimal WASI + Emscripten import stubs the module needs. */
function getImports() {
  return {
    wasi_snapshot_preview1: {
      environ_sizes_get(countPtr, sizePtr) {
        const view = new DataView(memory.buffer);
        view.setInt32(countPtr, 0, true);
        view.setInt32(sizePtr, 0, true);
        return 0;
      },
      environ_get() {
        return 0;
      },
      fd_close() {
        return 0;
      },
      fd_seek() {
        return 0;
      },
      fd_read() {
        return 0;
      },
      fd_write(fd, iovPtr, iovLen, nwrittenPtr) {
        // Minimal stderr/stdout support — discard output but report success.
        const view = new DataView(memory.buffer);
        let totalWritten = 0;
        for (let i = 0; i < iovLen; i++) {
          const len = view.getUint32(iovPtr + i * 8 + 4, true);
          totalWritten += len;
        }
        view.setUint32(nwrittenPtr, totalWritten, true);
        return 0;
      },
      random_get(bufPtr, bufLen) {
        const buf = new Uint8Array(memory.buffer, bufPtr, bufLen);
        crypto.getRandomValues(buf);
        return 0;
      },
    },
    env: {
      emscripten_notify_memory_growth() {},
      __syscall_getcwd(buf, size) {
        const cwd = "/\0";
        const bytes = new TextEncoder().encode(cwd);
        new Uint8Array(memory.buffer).set(bytes, buf);
        return buf;
      },
    },
  };
}

/** Read a null-terminated C string from wasm memory. */
function readCString(ptr) {
  const mem = new Uint8Array(memory.buffer);
  let end = ptr;
  while (mem[end] !== 0) end++;
  return new TextDecoder().decode(mem.slice(ptr, end));
}

/**
 * Call a WASM function that accepts (ptr, len) and returns a C string pointer.
 * Handles memory allocation, copying input bytes, and freeing the result.
 */
function _callWasmStringFn(wasmFn, bytes) {
  if (!instance) {
    throw new Error("WASM module not initialised — call init() first");
  }

  const { malloc, free_string } = instance.exports;

  // Allocate wasm memory and copy the input bytes.
  const inputPtr = malloc(bytes.length);
  if (inputPtr === 0) throw new Error("malloc failed");

  new Uint8Array(memory.buffer).set(bytes, inputPtr);

  // Call the wasm function.
  const resultPtr = wasmFn(inputPtr, bytes.length);

  // Free the input buffer (reuse malloc/free from emscripten).
  instance.exports.free(inputPtr);

  if (resultPtr === 0) {
    throw new Error("WASM function returned null — parsing failed");
  }

  // Read the result string and free it.
  const result = readCString(resultPtr);
  free_string(resultPtr);
  return result;
}

/**
 * Initialise the WASM module. Must be called (and awaited) before
 * calling any other exported functions.
 */
export async function init() {
  if (instance) return;

  const wasmPath = join(__dirname, "readstat_wasm.wasm");
  const wasmBytes = readFileSync(wasmPath);
  const imports = getImports();

  const result = await WebAssembly.instantiate(wasmBytes, imports);
  instance = result.instance;
  memory = instance.exports.memory;

  // Emscripten modules expose an _initialize function for ctors.
  if (typeof instance.exports._initialize === "function") {
    instance.exports._initialize();
  }
}

/**
 * Read metadata from a `.sas7bdat` file provided as a `Uint8Array`.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @returns {string} A JSON string containing file-level and variable-level metadata.
 */
export function read_metadata(bytes) {
  return _callWasmStringFn(instance.exports.read_metadata, bytes);
}

/**
 * Read metadata, skipping the full row count for speed.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @returns {string} A JSON string containing metadata (row_count may be inaccurate).
 */
export function read_metadata_fast(bytes) {
  return _callWasmStringFn(instance.exports.read_metadata_fast, bytes);
}

/**
 * Read data from a `.sas7bdat` file and return it as a CSV string.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @returns {string} CSV data with header row.
 */
export function read_data(bytes) {
  return _callWasmStringFn(instance.exports.read_data, bytes);
}

/**
 * Read data from a `.sas7bdat` file and return it as an NDJSON string.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @returns {string} Newline-delimited JSON data.
 */
export function read_data_ndjson(bytes) {
  return _callWasmStringFn(instance.exports.read_data_ndjson, bytes);
}

export default init;
