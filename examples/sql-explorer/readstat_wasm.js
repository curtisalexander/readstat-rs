/**
 * Browser-compatible WASM wrapper for readstat-wasm.
 *
 * Adapted from crates/readstat-wasm/pkg/readstat_wasm.js — replaces Node.js
 * APIs (fs, path, url) with browser-native fetch() + import.meta.url.
 *
 * Public API is identical: init(), read_metadata(), read_metadata_fast(),
 * read_data(), read_data_ndjson(), read_data_parquet(), read_data_feather().
 */

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
 */
function _callWasmStringFn(wasmFn, bytes) {
  if (!instance) {
    throw new Error("WASM module not initialised — call init() first");
  }

  const { malloc, free_string } = instance.exports;

  const inputPtr = malloc(bytes.length);
  if (inputPtr === 0) throw new Error("malloc failed");

  new Uint8Array(memory.buffer).set(bytes, inputPtr);

  const resultPtr = wasmFn(inputPtr, bytes.length);

  instance.exports.free(inputPtr);

  if (resultPtr === 0) {
    throw new Error("WASM function returned null — parsing failed");
  }

  const result = readCString(resultPtr);
  free_string(resultPtr);
  return result;
}

/**
 * Call a WASM function that accepts (ptr, len, out_len_ptr) and returns a
 * binary byte buffer.
 */
function _callWasmBinaryFn(wasmFn, bytes) {
  if (!instance) {
    throw new Error("WASM module not initialised — call init() first");
  }

  const { malloc, free_binary } = instance.exports;

  const inputPtr = malloc(bytes.length);
  if (inputPtr === 0) throw new Error("malloc failed");

  new Uint8Array(memory.buffer).set(bytes, inputPtr);

  const outLenPtr = malloc(4);
  if (outLenPtr === 0) {
    instance.exports.free(inputPtr);
    throw new Error("malloc failed for out_len");
  }

  const resultPtr = wasmFn(inputPtr, bytes.length, outLenPtr);

  instance.exports.free(inputPtr);

  if (resultPtr === 0) {
    instance.exports.free(outLenPtr);
    throw new Error("WASM function returned null — parsing failed");
  }

  const view = new DataView(memory.buffer);
  const resultLen = view.getUint32(outLenPtr, true);
  instance.exports.free(outLenPtr);

  const result = new Uint8Array(memory.buffer, resultPtr, resultLen).slice();

  free_binary(resultPtr, resultLen);

  return result;
}

/**
 * Initialise the WASM module. Must be called (and awaited) before
 * calling any other exported functions.
 */
export async function init() {
  if (instance) return;

  const wasmUrl = new URL("readstat_wasm.wasm", import.meta.url);
  const response = await fetch(wasmUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch WASM module: ${response.status} ${response.statusText}`);
  }

  const wasmBytes = await response.arrayBuffer();
  const imports = getImports();

  const result = await WebAssembly.instantiate(wasmBytes, imports);
  instance = result.instance;
  memory = instance.exports.memory;

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

/**
 * Read data from a `.sas7bdat` file and return it as Parquet bytes.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @returns {Uint8Array} Parquet file bytes (Snappy-compressed).
 */
export function read_data_parquet(bytes) {
  return _callWasmBinaryFn(instance.exports.read_data_parquet, bytes);
}

/**
 * Read data from a `.sas7bdat` file and return it as Feather (Arrow IPC) bytes.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @returns {Uint8Array} Feather file bytes.
 */
export function read_data_feather(bytes) {
  return _callWasmBinaryFn(instance.exports.read_data_feather, bytes);
}

export default init;
