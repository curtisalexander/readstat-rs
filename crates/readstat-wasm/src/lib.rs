use readstat::{ReadStatData, ReadStatMetadata, write_batch_to_csv_bytes, write_batch_to_ndjson_bytes};
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

/// Free a string previously returned by any of the `read_*` functions.
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
