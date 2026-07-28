use readstat::{
    ReadStatMetadata, ReadStatReader, write_batch_to_csv_bytes, write_batch_to_ndjson_bytes,
};
use readstat::{write_batch_to_feather_bytes, write_batch_to_parquet_bytes};
use std::ffi::CString;
use std::os::raw::c_char;
use std::slice;
use std::{cell::RefCell, panic::UnwindSafe};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(message: impl ToString) {
    let message = message.to_string().replace('\0', "\\0");
    LAST_ERROR.with(|error| *error.borrow_mut() = CString::new(message).ok());
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned())
}

fn catch_export<T: Copy>(failure: T, f: impl FnOnce() -> Result<T, String> + UnwindSafe) -> T {
    LAST_ERROR.with(|error| *error.borrow_mut() = None);
    match std::panic::catch_unwind(f) {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            set_last_error(error);
            failure
        }
        Err(payload) => {
            set_last_error(format!("WASM export panicked: {}", panic_message(payload)));
            failure
        }
    }
}

/// Return the most recent error for this thread, or null if the last call succeeded.
///
/// The returned pointer is borrowed and remains valid until the next exported read call
/// on the same thread. The caller must not free it.
#[unsafe(no_mangle)]
pub extern "C" fn readstat_last_error() -> *const c_char {
    LAST_ERROR.with(|error| {
        error
            .borrow()
            .as_ref()
            .map_or(std::ptr::null(), |message| message.as_ptr())
    })
}

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
    catch_export(std::ptr::null_mut(), || unsafe {
        read_metadata_inner(ptr, len, false)
    })
}

/// Read metadata, skipping the full row count for speed.
///
/// # Safety
///
/// Same contract as [`read_metadata`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_metadata_fast(ptr: *const u8, len: usize) -> *mut c_char {
    catch_export(std::ptr::null_mut(), || unsafe {
        read_metadata_inner(ptr, len, true)
    })
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
    catch_export(std::ptr::null_mut(), || unsafe {
        read_data_inner(ptr, len, &OutputFormat::Csv)
    })
}

/// Read data from a `.sas7bdat` file and return it as NDJSON.
///
/// # Safety
///
/// Same contract as [`read_data`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_ndjson(ptr: *const u8, len: usize) -> *mut c_char {
    catch_export(std::ptr::null_mut(), || unsafe {
        read_data_inner(ptr, len, &OutputFormat::Ndjson)
    })
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
    catch_export(std::ptr::null_mut(), || unsafe {
        if out_len.is_null() {
            return Err("out_len must not be null".to_owned());
        }
        *out_len = 0;
        read_data_binary_inner(ptr, len, &BinaryOutputFormat::Parquet, out_len)
    })
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
    catch_export(std::ptr::null_mut(), || unsafe {
        if out_len.is_null() {
            return Err("out_len must not be null".to_owned());
        }
        *out_len = 0;
        read_data_binary_inner(ptr, len, &BinaryOutputFormat::Feather, out_len)
    })
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
        // SAFETY: The pointer was produced by `Box::into_raw` on a `Box<[u8]>` of
        // exactly `len` bytes in `read_data_binary_inner`, so reconstructing the
        // same `Box<[u8]>` is valid. `slice_from_raw_parts_mut` builds the raw
        // fat pointer directly, without materializing an intermediate `&mut [u8]`
        // before `Box::from_raw` takes ownership.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) });
    }
}

unsafe fn read_metadata_inner(
    ptr: *const u8,
    len: usize,
    skip_row_count: bool,
) -> Result<*mut c_char, String> {
    if ptr.is_null() || len == 0 {
        return Err("input buffer must not be null or empty".to_owned());
    }

    // SAFETY: The caller guarantees `ptr` is valid for `len` bytes (see public fn docs).
    // We also checked for null/zero above.
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    let md = if skip_row_count {
        // The high-level reader intentionally always computes the exact row count.
        // Keep the low-level metadata call for this fast-path export only.
        let mut md = ReadStatMetadata::new();
        md.read_metadata_from_bytes(bytes, true)
            .map_err(|error| error.to_string())?;
        md
    } else {
        ReadStatReader::from_bytes(bytes)
            .metadata()
            .map_err(|error| error.to_string())?
    };

    let json = serde_json::to_string(&md).map_err(|error| error.to_string())?;
    CString::new(json)
        .map(CString::into_raw)
        .map_err(|error| error.to_string())
}

enum OutputFormat {
    Csv,
    Ndjson,
}

enum BinaryOutputFormat {
    Parquet,
    Feather,
}

unsafe fn read_data_inner(
    ptr: *const u8,
    len: usize,
    format: &OutputFormat,
) -> Result<*mut c_char, String> {
    if ptr.is_null() || len == 0 {
        return Err("input buffer must not be null or empty".to_owned());
    }

    // SAFETY: The caller guarantees `ptr` is valid for `len` bytes (see public fn docs).
    // We also checked for null/zero above.
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    let batch = ReadStatReader::from_bytes(bytes)
        .read()
        .map_err(|error| error.to_string())?;

    let output_bytes = match format {
        OutputFormat::Csv => write_batch_to_csv_bytes(&batch),
        OutputFormat::Ndjson => write_batch_to_ndjson_bytes(&batch),
    };

    let bytes = output_bytes.map_err(|error| error.to_string())?;
    CString::new(bytes)
        .map(CString::into_raw)
        .map_err(|error| error.to_string())
}

unsafe fn read_data_binary_inner(
    ptr: *const u8,
    len: usize,
    format: &BinaryOutputFormat,
    out_len: *mut usize,
) -> Result<*mut u8, String> {
    if ptr.is_null() || len == 0 || out_len.is_null() {
        return Err(
            "input buffer and out_len must not be null; input must not be empty".to_owned(),
        );
    }

    // SAFETY: The caller guarantees `ptr` is valid for `len` bytes (see public fn docs).
    // We also checked for null/zero above.
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    let batch = ReadStatReader::from_bytes(bytes)
        .read()
        .map_err(|error| error.to_string())?;

    let output_bytes = match format {
        BinaryOutputFormat::Parquet => write_batch_to_parquet_bytes(&batch),
        BinaryOutputFormat::Feather => write_batch_to_feather_bytes(&batch),
    };

    match output_bytes.map_err(|error| error.to_string()) {
        Ok(vec) => {
            // Convert to a boxed slice so that the allocation size equals the
            // data length exactly.  `free_binary` reconstructs this `Box<[u8]>`
            // to deallocate with the correct layout.
            let boxed = vec.into_boxed_slice();
            let data_len = boxed.len();
            let data_ptr = Box::into_raw(boxed) as *mut u8;
            // SAFETY: `out_len` was checked non-null at the top of this function.
            unsafe { *out_len = data_len };
            Ok(data_ptr)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_errors_are_actionable_and_success_clears_them() {
        let result = catch_export(0, || Err("bad input".to_owned()));
        assert_eq!(result, 0);
        assert_eq!(
            unsafe { std::ffi::CStr::from_ptr(readstat_last_error()) }
                .to_str()
                .unwrap(),
            "bad input"
        );

        assert_eq!(catch_export(0, || Ok(7)), 7);
        assert!(readstat_last_error().is_null());
    }

    #[test]
    fn export_panics_are_caught() {
        assert_eq!(catch_export(0, || panic!("broken parser")), 0);
        let error = unsafe { std::ffi::CStr::from_ptr(readstat_last_error()) };
        assert!(error.to_string_lossy().contains("broken parser"));
    }
}
