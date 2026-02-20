//! Shared utility functions used across the crate.
//!
//! Provides helpers for computing streaming chunk offsets ([`build_offsets`]) and
//! converting C string pointers to owned Rust strings ([`ptr_to_string`]).

use std::ffi::CStr;

use crate::err::ReadStatError;

/// Computes row offset boundaries for streaming chunk-based processing.
///
/// Given a total `row_count` and `stream_rows` (chunk size), returns a sorted
/// vector of offsets for use with [`windows(2)`](slice::windows) to form
/// `[start, end)` pairs.
///
/// # Example
///
/// ```
/// # use readstat::build_offsets;
/// let offsets = build_offsets(25, 10).unwrap();
/// assert_eq!(offsets, vec![0, 10, 20, 25]);
/// // Produces pairs: [0,10), [10,20), [20,25)
/// ```
pub fn build_offsets(
    row_count: u32,
    stream_rows: u32,
) -> Result<Vec<u32>, ReadStatError> {
    let chunks = row_count.div_ceil(stream_rows.max(1));
    let mut offsets = Vec::with_capacity(chunks as usize + 1);

    for c in 0..chunks {
        offsets.push(c * stream_rows);
    }
    offsets.push(row_count);

    Ok(offsets)
}

/// Converts a C string pointer to an owned Rust [`String`].
///
/// Returns an empty string if the pointer is null. Uses lossy UTF-8 conversion
/// to handle non-UTF-8 data gracefully.
pub fn ptr_to_string(x: *const i8) -> String {
    if x.is_null() {
        String::new()
    } else {
        // From Rust documentation - https://doc.rust-lang.org/std/ffi/struct.CStr.html
        let cstr = unsafe { CStr::from_ptr(x) };
        // Get copy-on-write Cow<'_, str>, then guarantee a freshly-owned String allocation
        String::from_utf8_lossy(cstr.to_bytes()).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    // --- build_offsets tests ---

    #[test]
    fn build_offsets_exact_division() {
        let offsets = build_offsets(30, 10).unwrap();
        assert_eq!(offsets, vec![0, 10, 20, 30]);
    }

    #[test]
    fn build_offsets_non_exact_division() {
        let offsets = build_offsets(25, 10).unwrap();
        assert_eq!(offsets, vec![0, 10, 20, 25]);
    }

    #[test]
    fn build_offsets_stream_exceeds_row_count() {
        let offsets = build_offsets(5, 10).unwrap();
        assert_eq!(offsets, vec![0, 5]);
    }

    #[test]
    fn build_offsets_single_row() {
        let offsets = build_offsets(1, 10).unwrap();
        assert_eq!(offsets, vec![0, 1]);
    }

    #[test]
    fn build_offsets_equal_stream_and_rows() {
        let offsets = build_offsets(10, 10).unwrap();
        assert_eq!(offsets, vec![0, 10]);
    }

    #[test]
    fn build_offsets_zero_rows() {
        let offsets = build_offsets(0, 10).unwrap();
        assert_eq!(offsets, vec![0]);
        // No windows produced for zero rows
        assert_eq!(offsets.windows(2).count(), 0);
    }

    #[test]
    fn build_offsets_windows_produce_valid_pairs() {
        let offsets = build_offsets(25, 10).unwrap();
        let pairs: Vec<_> = offsets.windows(2).map(|w| (w[0], w[1])).collect();
        assert_eq!(pairs, vec![(0, 10), (10, 20), (20, 25)]);
    }

    #[test]
    fn build_offsets_single_chunk_windows() {
        let offsets = build_offsets(5, 10).unwrap();
        let pairs: Vec<_> = offsets.windows(2).map(|w| (w[0], w[1])).collect();
        assert_eq!(pairs, vec![(0, 5)]);
    }

    #[test]
    fn build_offsets_large_dataset() {
        let offsets = build_offsets(100_000, 10_000).unwrap();
        assert_eq!(offsets.len(), 11);
        assert_eq!(*offsets.first().unwrap(), 0);
        assert_eq!(*offsets.last().unwrap(), 100_000);
    }

    // --- ptr_to_string tests ---

    #[test]
    fn ptr_to_string_null_returns_empty() {
        let result = ptr_to_string(std::ptr::null());
        assert_eq!(result, "");
    }

    #[test]
    fn ptr_to_string_valid_cstring() {
        let cs = CString::new("hello").unwrap();
        let result = ptr_to_string(cs.as_ptr());
        assert_eq!(result, "hello");
    }

    #[test]
    fn ptr_to_string_empty_cstring() {
        let cs = CString::new("").unwrap();
        let result = ptr_to_string(cs.as_ptr());
        assert_eq!(result, "");
    }

    #[test]
    fn ptr_to_string_with_unicode() {
        let cs = CString::new("UTF-8 encoded: café").unwrap();
        let result = ptr_to_string(cs.as_ptr());
        assert_eq!(result, "UTF-8 encoded: café");
    }

    #[test]
    fn ptr_to_string_with_truncated_utf8() {
        // Simulates SAS truncating "café" at a byte boundary mid-character.
        // "café" in UTF-8 is [63, 61, 66, C3, A9]. Truncating after 4 bytes
        // leaves [63, 61, 66, C3] — an incomplete 2-byte sequence.
        // ptr_to_string should replace the dangling 0xC3 with U+FFFD.
        // Safety: we need a null-terminated buffer for CStr::from_ptr.
        // Build one explicitly so the test is self-contained.
        let mut buf = b"caf\xC3".to_vec();
        buf.push(0); // null terminator
        let ptr = buf.as_ptr() as *const i8;

        let result = ptr_to_string(ptr);
        assert_eq!(result, "caf\u{FFFD}");
    }

    #[test]
    fn ptr_to_string_with_invalid_continuation_byte() {
        // 0xFF is never valid in UTF-8
        let mut buf = b"hello\xFFworld".to_vec();
        buf.push(0);
        let ptr = buf.as_ptr() as *const i8;

        let result = ptr_to_string(ptr);
        assert_eq!(result, "hello\u{FFFD}world");
    }
}
