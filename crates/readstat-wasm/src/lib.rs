use readstat::{ReadStatData, ReadStatMetadata, write_batch_to_csv_bytes, write_batch_to_ndjson_bytes};
use readstat::{write_batch_to_parquet_bytes, write_batch_to_feather_bytes};
use std::ffi::CString;
use std::os::raw::c_char;
use std::slice;

/// Read metadata from a `.sas7bdat` file provided as a byte buffer.
///
/// # Safety
///
/// `ptr` must point to a valid byte buffer of at least `len` bytes.
/// Returns a pointer to a null-terminated JSON string allocated on the heap.
/// The caller must free it by passing the pointer to [`free_string`].
/// Returns null on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_metadata(ptr: *const u8, len: usize) -> *mut c_char {
    unsafe { read_metadata_inner(ptr, len, false) }
}

/// Read metadata, skipping the full row count for speed.
///
/// # Safety
///
/// Same contract as [`read_metadata`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_metadata_fast(ptr: *const u8, len: usize) -> *mut c_char {
    unsafe { read_metadata_inner(ptr, len, true) }
}

/// Read data from a `.sas7bdat` file and return it as CSV.
///
/// # Safety
///
/// `ptr` must point to a valid byte buffer of at least `len` bytes.
/// Returns a pointer to a null-terminated CSV string allocated on the heap.
/// The caller must free it by passing the pointer to [`free_string`].
/// Returns null on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data(ptr: *const u8, len: usize) -> *mut c_char {
    unsafe { read_data_inner(ptr, len, OutputFormat::Csv) }
}

/// Read data from a `.sas7bdat` file and return it as NDJSON.
///
/// # Safety
///
/// Same contract as [`read_data`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_ndjson(ptr: *const u8, len: usize) -> *mut c_char {
    unsafe { read_data_inner(ptr, len, OutputFormat::Ndjson) }
}

/// Read data from a `.sas7bdat` file and return it as Parquet bytes.
///
/// # Safety
///
/// `ptr` must point to a valid byte buffer of at least `len` bytes.
/// `out_len` must point to a writable `usize` where the output length will be stored.
/// Returns a pointer to a byte buffer allocated on the heap.
/// The caller must free it by passing the pointer and length to [`free_binary`].
/// Returns null on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_parquet(
    ptr: *const u8,
    len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    unsafe { read_data_binary_inner(ptr, len, BinaryOutputFormat::Parquet, out_len) }
}

/// Read data from a `.sas7bdat` file and return it as Feather (Arrow IPC) bytes.
///
/// # Safety
///
/// Same contract as [`read_data_parquet`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_feather(
    ptr: *const u8,
    len: usize,
    out_len: *mut usize,
) -> *mut u8 {
    unsafe { read_data_binary_inner(ptr, len, BinaryOutputFormat::Feather, out_len) }
}

/// Free a string previously returned by any of the `read_*` string functions.
///
/// # Safety
///
/// `ptr` must be a pointer returned by one of the exported functions,
/// or null (which is a no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// Free a binary buffer previously returned by [`read_data_parquet`] or [`read_data_feather`].
///
/// # Safety
///
/// `ptr` must be a pointer returned by one of the binary export functions with the
/// corresponding `len`, or null (which is a no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_binary(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
    }
}

unsafe fn read_metadata_inner(ptr: *const u8, len: usize, skip_row_count: bool) -> *mut c_char {
    if ptr.is_null() || len == 0 {
        return std::ptr::null_mut();
    }

    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(bytes, skip_row_count).is_err() {
        return std::ptr::null_mut();
    }

    match serde_json::to_string(&md) {
        Ok(json) => match CString::new(json) {
            Ok(c) => c.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

enum OutputFormat {
    Csv,
    Ndjson,
}

enum BinaryOutputFormat {
    Parquet,
    Feather,
}

unsafe fn read_data_inner(ptr: *const u8, len: usize, format: OutputFormat) -> *mut c_char {
    if ptr.is_null() || len == 0 {
        return std::ptr::null_mut();
    }

    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    // First pass: read metadata
    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(bytes, false).is_err() {
        return std::ptr::null_mut();
    }

    let row_count = md.row_count as u32;

    // Second pass: read data
    let mut d = ReadStatData::new().init(md, 0, row_count);
    if d.read_data_from_bytes(bytes).is_err() {
        return std::ptr::null_mut();
    }

    let batch = match &d.batch {
        Some(b) => b,
        None => return std::ptr::null_mut(),
    };

    let output_bytes = match format {
        OutputFormat::Csv => write_batch_to_csv_bytes(batch),
        OutputFormat::Ndjson => write_batch_to_ndjson_bytes(batch),
    };

    match output_bytes {
        Ok(bytes) => match CString::new(bytes) {
            Ok(c) => c.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

unsafe fn read_data_binary_inner(
    ptr: *const u8,
    len: usize,
    format: BinaryOutputFormat,
    out_len: *mut usize,
) -> *mut u8 {
    if ptr.is_null() || len == 0 || out_len.is_null() {
        return std::ptr::null_mut();
    }

    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    // First pass: read metadata
    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(bytes, false).is_err() {
        return std::ptr::null_mut();
    }

    let row_count = md.row_count as u32;

    // Second pass: read data
    let mut d = ReadStatData::new().init(md, 0, row_count);
    if d.read_data_from_bytes(bytes).is_err() {
        return std::ptr::null_mut();
    }

    let batch = match &d.batch {
        Some(b) => b,
        None => return std::ptr::null_mut(),
    };

    let output_bytes = match format {
        BinaryOutputFormat::Parquet => write_batch_to_parquet_bytes(batch),
        BinaryOutputFormat::Feather => write_batch_to_feather_bytes(batch),
    };

    match output_bytes {
        Ok(mut vec) => {
            let data_ptr = vec.as_mut_ptr();
            let data_len = vec.len();
            std::mem::forget(vec);
            unsafe { *out_len = data_len };
            data_ptr
        }
        Err(_) => std::ptr::null_mut(),
    }
}
