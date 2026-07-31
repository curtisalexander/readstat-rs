import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let instance;
let memory;
let progressCallback = () => {};

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
      readstat_progress(stage, current, total) {
        progressCallback({ stage, current, total });
      },
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
  while (end < mem.length && mem[end] !== 0) end++;
  return new TextDecoder().decode(mem.slice(ptr, end));
}

/** Read the borrowed native last-error string before another export replaces it. */
function getLastError() {
  const lastError = instance.exports.readstat_last_error;
  if (typeof lastError !== "function") {
    return "WASM function returned null — parsing failed";
  }
  const ptr = lastError();
  return ptr === 0 ? "WASM function returned null — parsing failed" : readCString(ptr);
}

/**
 * Call a WASM function that accepts (ptr, len) and returns a C string pointer.
 * Handles memory allocation, copying input bytes, and freeing the result.
 */
function _callWasmStringFn(wasmFn, bytes, ...args) {
  if (!instance) {
    throw new Error("WASM module not initialised — call init() first");
  }

  const { malloc, free_string } = instance.exports;

  // Allocate wasm memory and copy the input bytes.
  const inputPtr = malloc(bytes.length);
  if (inputPtr === 0) throw new Error("malloc failed");

  new Uint8Array(memory.buffer).set(bytes, inputPtr);

  let resultPtr;
  try {
    resultPtr = wasmFn(inputPtr, bytes.length, ...args);
  } finally {
    // Free the input buffer (reuse malloc/free from emscripten).
    instance.exports.free(inputPtr);
  }

  if (resultPtr === 0) {
    throw new Error(getLastError());
  }

  try {
    return readCString(resultPtr);
  } finally {
    free_string(resultPtr);
  }
}

/**
 * Call a WASM function that accepts (ptr, len, out_len_ptr) and returns a
 * binary byte buffer. Handles memory allocation, copying input bytes, reading
 * the (ptr, len) result pair, and freeing via free_binary.
 */
function _callWasmBinaryFn(wasmFn, bytes, ...args) {
  if (!instance) {
    throw new Error("WASM module not initialised — call init() first");
  }

  const { malloc, free_binary } = instance.exports;

  // Allocate wasm memory and copy the input bytes.
  const inputPtr = malloc(bytes.length);
  if (inputPtr === 0) throw new Error("malloc failed");

  new Uint8Array(memory.buffer).set(bytes, inputPtr);

  // Allocate space for the out_len parameter (4 bytes for a u32-sized usize on wasm32).
  const outLenPtr = malloc(4);
  if (outLenPtr === 0) {
    instance.exports.free(inputPtr);
    throw new Error("malloc failed for out_len");
  }

  let resultPtr;
  try {
    resultPtr = wasmFn(inputPtr, bytes.length, ...args, outLenPtr);
  } catch (error) {
    instance.exports.free(outLenPtr);
    throw error;
  } finally {
    instance.exports.free(inputPtr);
  }

  if (resultPtr === 0) {
    instance.exports.free(outLenPtr);
    throw new Error(getLastError());
  }

  let resultLen;
  try {
    resultLen = new DataView(memory.buffer).getUint32(outLenPtr, true);
  } finally {
    instance.exports.free(outLenPtr);
  }

  try {
    // Copy the result bytes so they remain owned after freeing wasm memory.
    return new Uint8Array(memory.buffer, resultPtr, resultLen).slice();
  } finally {
    free_binary(resultPtr, resultLen);
  }
}

function _validateSelection(selection) {
  const columns = selection?.columns;
  const rowOffset = selection?.rowOffset;
  const rowLimit = selection?.rowLimit;
  if (!Array.isArray(columns) || columns.length === 0 || columns.some((name) => typeof name !== "string" || name.length === 0)) {
    throw new TypeError("columns must be a non-empty array of column names");
  }
  if (!Number.isInteger(rowOffset) || rowOffset < 0 || rowOffset > 0xffff_ffff) {
    throw new RangeError("rowOffset must be an integer between 0 and 4294967295");
  }
  if (!Number.isInteger(rowLimit) || rowLimit < 1 || rowLimit > 0xffff_ffff) {
    throw new RangeError("rowLimit must be an integer between 1 and 4294967295");
  }
  return { columns: new TextEncoder().encode(JSON.stringify(columns)), rowOffset, rowLimit };
}

function _callWasmReducedFn(wasmFn, bytes, selection, binary) {
  if (!instance) {
    throw new Error("WASM module not initialised — call init() first");
  }
  const reduced = _validateSelection(selection);
  const columnsPtr = instance.exports.malloc(reduced.columns.length);
  if (columnsPtr === 0) throw new Error("malloc failed for selected columns");
  try {
    new Uint8Array(memory.buffer).set(reduced.columns, columnsPtr);
    const args = [columnsPtr, reduced.columns.length, reduced.rowOffset, reduced.rowLimit];
    return binary
      ? _callWasmBinaryFn(wasmFn, bytes, ...args)
      : _callWasmStringFn(wasmFn, bytes, ...args);
  } finally {
    instance.exports.free(columnsPtr);
  }
}

/**
 * Initialise the WASM module. Must be called (and awaited) before
 * calling any other exported functions.
 */
export async function init(options = {}) {
  if (instance) return;
  progressCallback = options.onProgress || progressCallback;

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

/** Read selected columns and a bounded row range as CSV. */
export function read_data_reduced(bytes, selection) {
  return _callWasmReducedFn(instance.exports.read_data_reduced, bytes, selection, false);
}

/** Read selected columns and a bounded row range as NDJSON. */
export function read_data_ndjson_reduced(bytes, selection) {
  return _callWasmReducedFn(instance.exports.read_data_ndjson_reduced, bytes, selection, false);
}

/**
 * Read a bounded row preview as newline-delimited JSON.
 *
 * @param {Uint8Array} bytes - The raw file contents.
 * @param {number} rowLimit - Maximum number of rows to return.
 * @returns {string} Newline-delimited JSON preview data.
 */
export function read_preview(bytes, rowLimit) {
  if (!Number.isInteger(rowLimit) || rowLimit < 1 || rowLimit > 0xffff_ffff) {
    throw new RangeError("rowLimit must be an integer between 1 and 4294967295");
  }
  return _callWasmStringFn(instance.exports.read_preview, bytes, rowLimit);
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

/** Read selected columns and a bounded row range as Parquet bytes. */
export function read_data_parquet_reduced(bytes, selection) {
  return _callWasmReducedFn(instance.exports.read_data_parquet_reduced, bytes, selection, true);
}

/** Read selected columns and a bounded row range as Feather bytes. */
export function read_data_feather_reduced(bytes, selection) {
  return _callWasmReducedFn(instance.exports.read_data_feather_reduced, bytes, selection, true);
}

export default init;
