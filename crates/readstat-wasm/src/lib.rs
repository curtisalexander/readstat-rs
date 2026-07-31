use readstat::{
    ProgressCallback, ReadStatMetadata, ReadStatReader, write_batch_to_csv_bytes,
    write_batch_to_ndjson_bytes,
};
use readstat::{write_batch_to_feather_bytes, write_batch_to_parquet_bytes};
use std::ffi::CString;
use std::os::raw::c_char;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cell::RefCell, panic::UnwindSafe};

const STAGE_METADATA: u32 = 1;
const STAGE_PREVIEW_PARSE: u32 = 2;
const STAGE_PREVIEW_ENCODE: u32 = 3;
const STAGE_EXPORT_PARSE: u32 = 4;
const STAGE_EXPORT_ENCODE: u32 = 5;
const PREVIEW_PROGRESS_ROWS: u32 = 25;

#[cfg(target_os = "emscripten")]
#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn readstat_progress(stage: u32, current: u32, total: u32);
}

fn report_progress(stage: u32, current: u64, total: u64) {
    #[cfg(target_os = "emscripten")]
    unsafe {
        readstat_progress(
            stage,
            current.min(u64::from(u32::MAX)) as u32,
            total.min(u64::from(u32::MAX)) as u32,
        );
    }

    #[cfg(not(target_os = "emscripten"))]
    let _ = (stage, current, total);
}

struct WasmProgress {
    stage: u32,
    current: AtomicU64,
    total: u64,
}

impl WasmProgress {
    const fn new(stage: u32, total: u64) -> Self {
        Self {
            stage,
            current: AtomicU64::new(0),
            total,
        }
    }
}

impl ProgressCallback for WasmProgress {
    fn inc(&self, n: u64) {
        let current = self
            .current
            .fetch_add(n, Ordering::Relaxed)
            .saturating_add(n);
        report_progress(self.stage, current.min(self.total), self.total);
    }

    fn parsing_started(&self, _path: &str) {
        report_progress(self.stage, 0, self.total);
    }
}

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
        read_data_inner(ptr, len, &OutputFormat::Csv, None)
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
        read_data_inner(ptr, len, &OutputFormat::Ndjson, None)
    })
}

/// Read selected columns and a bounded row range as CSV.
///
/// # Safety
///
/// `ptr` must point to a valid byte buffer of at least `len` bytes.
/// `columns_ptr` must point to a UTF-8 JSON array of column names containing
/// `columns_len` bytes. `row_limit` must be greater than zero. The returned
/// string follows the ownership contract of [`read_data`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_reduced(
    ptr: *const u8,
    len: usize,
    columns_ptr: *const u8,
    columns_len: usize,
    row_offset: u32,
    row_limit: u32,
) -> *mut c_char {
    catch_export(std::ptr::null_mut(), || unsafe {
        let selection = read_selection(columns_ptr, columns_len, row_offset, row_limit)?;
        read_data_inner(ptr, len, &OutputFormat::Csv, Some(&selection))
    })
}

/// Read selected columns and a bounded row range as NDJSON.
///
/// # Safety
///
/// Same contract as [`read_data_reduced`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_ndjson_reduced(
    ptr: *const u8,
    len: usize,
    columns_ptr: *const u8,
    columns_len: usize,
    row_offset: u32,
    row_limit: u32,
) -> *mut c_char {
    catch_export(std::ptr::null_mut(), || unsafe {
        let selection = read_selection(columns_ptr, columns_len, row_offset, row_limit)?;
        read_data_inner(ptr, len, &OutputFormat::Ndjson, Some(&selection))
    })
}

/// Read at most `row_limit` rows and return them as NDJSON for browser preview.
///
/// # Safety
///
/// `ptr` must point to a valid byte buffer of at least `len` bytes. `row_limit`
/// must be greater than zero. The returned string must be released with
/// [`free_string`]. Returns null on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_preview(ptr: *const u8, len: usize, row_limit: u32) -> *mut c_char {
    catch_export(std::ptr::null_mut(), || unsafe {
        read_preview_inner(ptr, len, row_limit)
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
        read_data_binary_inner(ptr, len, &BinaryOutputFormat::Parquet, out_len, None)
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
        read_data_binary_inner(ptr, len, &BinaryOutputFormat::Feather, out_len, None)
    })
}

/// Read selected columns and a bounded row range as Parquet bytes.
///
/// # Safety
///
/// The input and selection arguments follow [`read_data_reduced`]. `out_len`
/// follows [`read_data_parquet`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_parquet_reduced(
    ptr: *const u8,
    len: usize,
    columns_ptr: *const u8,
    columns_len: usize,
    row_offset: u32,
    row_limit: u32,
    out_len: *mut usize,
) -> *mut u8 {
    catch_export(std::ptr::null_mut(), || unsafe {
        if out_len.is_null() {
            return Err("out_len must not be null".to_owned());
        }
        *out_len = 0;
        let selection = read_selection(columns_ptr, columns_len, row_offset, row_limit)?;
        read_data_binary_inner(
            ptr,
            len,
            &BinaryOutputFormat::Parquet,
            out_len,
            Some(&selection),
        )
    })
}

/// Read selected columns and a bounded row range as Feather bytes.
///
/// # Safety
///
/// Same contract as [`read_data_parquet_reduced`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_data_feather_reduced(
    ptr: *const u8,
    len: usize,
    columns_ptr: *const u8,
    columns_len: usize,
    row_offset: u32,
    row_limit: u32,
    out_len: *mut usize,
) -> *mut u8 {
    catch_export(std::ptr::null_mut(), || unsafe {
        if out_len.is_null() {
            return Err("out_len must not be null".to_owned());
        }
        *out_len = 0;
        let selection = read_selection(columns_ptr, columns_len, row_offset, row_limit)?;
        read_data_binary_inner(
            ptr,
            len,
            &BinaryOutputFormat::Feather,
            out_len,
            Some(&selection),
        )
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

    report_progress(STAGE_METADATA, 0, 0);
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
    report_progress(STAGE_METADATA, 1, 1);

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

struct ExportSelection {
    columns: Vec<String>,
    row_offset: u32,
    row_limit: u32,
}

unsafe fn read_selection(
    columns_ptr: *const u8,
    columns_len: usize,
    row_offset: u32,
    row_limit: u32,
) -> Result<ExportSelection, String> {
    if columns_ptr.is_null() || columns_len == 0 {
        return Err("selected columns must not be null or empty".to_owned());
    }
    if row_limit == 0 {
        return Err("export row limit must be greater than zero".to_owned());
    }

    // SAFETY: The caller guarantees `columns_ptr` is valid for `columns_len` bytes.
    let encoded = unsafe { slice::from_raw_parts(columns_ptr, columns_len) };
    let columns: Vec<String> = serde_json::from_slice(encoded)
        .map_err(|error| format!("invalid selected columns: {error}"))?;
    if columns.is_empty() {
        return Err("select at least one column for export".to_owned());
    }

    Ok(ExportSelection {
        columns,
        row_offset,
        row_limit,
    })
}

fn select_export(
    reader: ReadStatReader,
    selection: Option<&ExportSelection>,
) -> (ReadStatReader, Option<u64>) {
    match selection {
        Some(selection) => (
            reader
                .rows(selection.row_offset, Some(selection.row_limit))
                .columns(selection.columns.iter().cloned()),
            Some(u64::from(selection.row_limit)),
        ),
        None => (reader, None),
    }
}

unsafe fn read_preview_inner(
    ptr: *const u8,
    len: usize,
    row_limit: u32,
) -> Result<*mut c_char, String> {
    if ptr.is_null() || len == 0 {
        return Err("input buffer must not be null or empty".to_owned());
    }
    if row_limit == 0 {
        return Err("preview row limit must be greater than zero".to_owned());
    }

    // SAFETY: The caller guarantees `ptr` is valid for `len` bytes (see public fn docs).
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    let reader = ReadStatReader::from_bytes(bytes);

    report_progress(STAGE_METADATA, 0, 0);
    let metadata = reader.metadata().map_err(|error| error.to_string())?;
    report_progress(STAGE_METADATA, 1, 1);
    let available = metadata.row_count.unwrap_or_default().max(0) as u64;
    let preview_rows = u64::from(row_limit).min(available);
    let reader = reader
        .rows(0, Some(preview_rows as u32))
        .chunk_rows(PREVIEW_PROGRESS_ROWS.min(row_limit).max(1))
        .progress(Arc::new(WasmProgress::new(
            STAGE_PREVIEW_PARSE,
            preview_rows,
        )));

    let batch = reader.read().map_err(|error| error.to_string())?;
    report_progress(STAGE_PREVIEW_PARSE, preview_rows, preview_rows);
    report_progress(STAGE_PREVIEW_ENCODE, 0, 0);
    let output = write_batch_to_ndjson_bytes(&batch).map_err(|error| error.to_string())?;
    report_progress(STAGE_PREVIEW_ENCODE, 1, 1);

    CString::new(output)
        .map(CString::into_raw)
        .map_err(|error| error.to_string())
}

unsafe fn read_data_inner(
    ptr: *const u8,
    len: usize,
    format: &OutputFormat,
    selection: Option<&ExportSelection>,
) -> Result<*mut c_char, String> {
    if ptr.is_null() || len == 0 {
        return Err("input buffer must not be null or empty".to_owned());
    }

    // SAFETY: The caller guarantees `ptr` is valid for `len` bytes (see public fn docs).
    // We also checked for null/zero above.
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    report_progress(STAGE_METADATA, 0, 0);
    let reader = ReadStatReader::from_bytes(bytes);
    let metadata = reader.metadata().map_err(|error| error.to_string())?;
    report_progress(STAGE_METADATA, 1, 1);
    let rows = metadata.row_count.unwrap_or_default().max(0) as u64;
    let (reader, selected_rows) = select_export(reader, selection);
    let rows = selected_rows.unwrap_or(rows);
    let batch = reader
        .progress(Arc::new(WasmProgress::new(STAGE_EXPORT_PARSE, rows)))
        .read()
        .map_err(|error| error.to_string())?;

    report_progress(STAGE_EXPORT_ENCODE, 0, 0);
    let output_bytes = match format {
        OutputFormat::Csv => write_batch_to_csv_bytes(&batch),
        OutputFormat::Ndjson => write_batch_to_ndjson_bytes(&batch),
    };

    let bytes = output_bytes.map_err(|error| error.to_string())?;
    report_progress(STAGE_EXPORT_ENCODE, 1, 1);
    CString::new(bytes)
        .map(CString::into_raw)
        .map_err(|error| error.to_string())
}

unsafe fn read_data_binary_inner(
    ptr: *const u8,
    len: usize,
    format: &BinaryOutputFormat,
    out_len: *mut usize,
    selection: Option<&ExportSelection>,
) -> Result<*mut u8, String> {
    if ptr.is_null() || len == 0 || out_len.is_null() {
        return Err(
            "input buffer and out_len must not be null; input must not be empty".to_owned(),
        );
    }

    // SAFETY: The caller guarantees `ptr` is valid for `len` bytes (see public fn docs).
    // We also checked for null/zero above.
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };

    report_progress(STAGE_METADATA, 0, 0);
    let reader = ReadStatReader::from_bytes(bytes);
    let metadata = reader.metadata().map_err(|error| error.to_string())?;
    report_progress(STAGE_METADATA, 1, 1);
    let rows = metadata.row_count.unwrap_or_default().max(0) as u64;
    let (reader, selected_rows) = select_export(reader, selection);
    let rows = selected_rows.unwrap_or(rows);
    let batch = reader
        .progress(Arc::new(WasmProgress::new(STAGE_EXPORT_PARSE, rows)))
        .read()
        .map_err(|error| error.to_string())?;

    report_progress(STAGE_EXPORT_ENCODE, 0, 0);
    let output_bytes = match format {
        BinaryOutputFormat::Parquet => write_batch_to_parquet_bytes(&batch),
        BinaryOutputFormat::Feather => write_batch_to_feather_bytes(&batch),
    };

    match output_bytes.map_err(|error| error.to_string()) {
        Ok(vec) => {
            report_progress(STAGE_EXPORT_ENCODE, 1, 1);
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

    #[test]
    fn reduced_export_selection_requires_columns_and_rows() {
        let columns = br#"["name","age"]"#;
        let selection = unsafe { read_selection(columns.as_ptr(), columns.len(), 7, 12) }.unwrap();
        assert_eq!(selection.columns, ["name", "age"]);
        assert_eq!(selection.row_offset, 7);
        assert_eq!(selection.row_limit, 12);

        let empty = br#"[]"#;
        assert!(unsafe { read_selection(empty.as_ptr(), empty.len(), 0, 1) }.is_err());
        assert!(unsafe { read_selection(columns.as_ptr(), columns.len(), 0, 0) }.is_err());
    }
}
