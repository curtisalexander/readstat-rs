//! FFI callback functions invoked by the `ReadStat` C library during parsing.
//!
//! The `ReadStat` C parser uses a callback-driven architecture: as it reads a `.sas7bdat`
//! file, it invokes registered callbacks for metadata, variables, and values. Each
//! callback receives a raw `*mut c_void` context pointer that is cast back to the
//! appropriate Rust struct ([`ReadStatMetadata`](crate::ReadStatMetadata) or
//! [`ReadStatData`](crate::ReadStatData)) to accumulate parsed results.

use chrono::DateTime;
use log::debug;
use num_traits::FromPrimitive;
use std::os::raw::{c_char, c_int, c_void};

use crate::{
    common::ptr_to_string,
    err::ReadStatError,
    formats,
    rs_data::{ColumnBuilder, ReadStatData},
    rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata},
    rs_var::{ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass},
};

/// Return codes for `ReadStat` C callback functions.
///
/// Mirrors the `readstat_handler_t` enum from the C API. Only `OK` and `ABORT`
/// are currently used; `SKIP_VARIABLE` is included for completeness with the
/// C API contract.
#[allow(dead_code, non_camel_case_types)]
#[derive(Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
}

// C callback functions

/// FFI callback that extracts file-level metadata from the `ReadStat` C parser.
///
/// Called once during parsing. Populates the [`ReadStatMetadata`] struct
/// (accessed via the `ctx` pointer) with row/variable counts, encoding,
/// timestamps, compression, and endianness.
///
/// # Safety
///
/// - `metadata` must be a valid pointer to a `readstat_metadata_t` produced by the C parser.
/// - `ctx` must be a valid pointer to a [`ReadStatMetadata`] instance that outlives this call.
/// - This function must only be called by the `ReadStat` C library as a registered callback.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
pub(crate) extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *ctx.cast::<ReadStatMetadata>() };

    // get metadata
    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    let vc: c_int = unsafe { readstat_sys::readstat_get_var_count(metadata) };
    let table_name = unsafe { ptr_to_string(readstat_sys::readstat_get_table_name(metadata)) };
    let file_label = unsafe { ptr_to_string(readstat_sys::readstat_get_file_label(metadata)) };
    let file_encoding =
        unsafe { ptr_to_string(readstat_sys::readstat_get_file_encoding(metadata)) };
    let version: c_int = unsafe { readstat_sys::readstat_get_file_format_version(metadata) };
    let is_64bit = unsafe { readstat_sys::readstat_get_file_format_is_64bit(metadata) };
    let ct = DateTime::from_timestamp(
        unsafe { readstat_sys::readstat_get_creation_time(metadata) },
        0,
    )
    .unwrap_or_default()
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();
    let mt = DateTime::from_timestamp(
        unsafe { readstat_sys::readstat_get_modified_time(metadata) },
        0,
    )
    .unwrap_or_default()
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();

    #[allow(clippy::useless_conversion)]
    let compression =
        FromPrimitive::from_i32(unsafe { readstat_sys::readstat_get_compression(metadata) } as i32)
            .unwrap_or(ReadStatCompress::None);

    #[allow(clippy::useless_conversion)]
    let endianness =
        FromPrimitive::from_i32(unsafe { readstat_sys::readstat_get_endianness(metadata) } as i32)
            .unwrap_or(ReadStatEndian::None);

    debug!("row_count is {rc}");
    debug!("var_count is {vc}");
    debug!("table_name is {table_name}");
    debug!("file_label is {file_label}");
    debug!("file_encoding is {file_encoding}");
    debug!("version is {version}");
    debug!("is_64bit is {is_64bit}");
    debug!("creation_time is {ct}");
    debug!("modified_time is {mt}");
    debug!("compression is {compression:#?}");
    debug!("endianness is {endianness:#?}");

    // insert into ReadStatMetadata struct
    m.row_count = rc;
    m.var_count = vc;
    m.table_name = table_name;
    m.file_label = file_label;
    m.file_encoding = file_encoding;
    m.version = version;
    m.is_64bit = is_64bit != 0;
    m.creation_time = ct;
    m.modified_time = mt;
    m.compression = compression;
    m.endianness = endianness;

    debug!("metadata struct is {m:#?}");

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

/// FFI callback that extracts per-variable metadata from the `ReadStat` C parser.
///
/// Called once for each variable (column) in the dataset. Populates a
/// [`ReadStatVarMetadata`] entry in the [`ReadStatMetadata::vars`] map
/// with the variable's name, type, label, and SAS format classification.
///
/// # Safety
///
/// - `variable` must be a valid pointer to a `readstat_variable_t` produced by the C parser.
/// - `ctx` must be a valid pointer to a [`ReadStatMetadata`] instance that outlives this call.
/// - This function must only be called by the `ReadStat` C library as a registered callback.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
pub(crate) extern "C" fn handle_variable(
    index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *ctx.cast::<ReadStatMetadata>() };

    // get variable metadata
    #[allow(clippy::useless_conversion)]
    let var_type =
        FromPrimitive::from_i32(
            unsafe { readstat_sys::readstat_variable_get_type(variable) } as i32,
        )
        .unwrap_or(ReadStatVarType::Unknown);

    #[allow(clippy::useless_conversion)]
    let var_type_class =
        FromPrimitive::from_i32(
            unsafe { readstat_sys::readstat_variable_get_type_class(variable) } as i32,
        )
        .unwrap_or(ReadStatVarTypeClass::Numeric);

    let var_name = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_name(variable)) };
    let var_label = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_label(variable)) };
    let var_format = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_format(variable)) };
    let var_format_class = formats::match_var_format(&var_format);
    let storage_width =
        unsafe { readstat_sys::readstat_variable_get_storage_width(variable) } as usize;
    let display_width =
        unsafe { readstat_sys::readstat_variable_get_display_width(variable) } as i32;

    debug!("var_type is {var_type:#?}");
    debug!("var_type_class is {var_type_class:#?}");
    debug!("var_name is {var_name}");
    debug!("var_label is {var_label}");
    debug!("var_format is {var_format}");
    debug!("var_format_class is {var_format_class:#?}");
    debug!("storage_width is {storage_width}");
    debug!("display_width is {display_width}");

    // insert into BTreeMap within ReadStatMetadata struct
    m.vars.insert(
        index,
        ReadStatVarMetadata::new(
            var_name,
            var_type,
            var_type_class,
            var_label,
            var_format,
            var_format_class,
            storage_width,
            display_width,
        ),
    );

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

/// SAS epoch (1960-01-01) to Unix epoch (1970-01-01) offset in days.
pub(crate) const DAY_SHIFT: i32 = 3653;
/// SAS epoch to Unix epoch offset in seconds.
pub(crate) const SEC_SHIFT: i64 = 315_619_200;

/// Scale factor for rounding to 14 decimal places: `10^14`.
pub(crate) const ROUND_SCALE: f64 = 1e14;

/// Rounds an f64 to 14 decimal places using pure arithmetic.
///
/// Eliminates the string formatting roundtrip entirely. For values like 4.6
/// that can't be exactly represented in IEEE 754, this cleans up trailing
/// noise (e.g. `4.6000000000000005` → `4.6`).
///
/// Splits into integer and fractional parts before scaling to avoid overflow:
/// large SAS datetime values (~1.9e9) multiplied by 1e14 would exceed f64's
/// exact integer range (2^53), causing rounding errors.
#[inline]
pub(crate) fn round_decimal_f64(v: f64) -> f64 {
    if !v.is_finite() {
        return v;
    }
    let int_part = v.trunc();
    let frac_part = v.fract(); // always in (-1, 1), so frac * 1e14 < 1e14 < 2^53
    let rounded_frac = (frac_part * ROUND_SCALE).round() / ROUND_SCALE;
    int_part + rounded_frac
}

/// Rounds an f32 to 14 decimal places using pure arithmetic.
#[inline]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn round_decimal_f32(v: f32) -> f32 {
    if !v.is_finite() {
        return v;
    }
    // Promote to f64 for the rounding to avoid f32 precision loss
    let v64 = f64::from(v);
    let int_part = v64.trunc();
    let frac_part = v64.fract();
    let rounded_frac = (frac_part * ROUND_SCALE).round() / ROUND_SCALE;
    (int_part + rounded_frac) as f32
}

/// Converts an `f64` to `i64`, returning `None` for non-finite or out-of-range
/// values instead of silently saturating (the behaviour of an `as` cast).
///
/// Used by the date/time value arms so that an out-of-range SAS datetime
/// surfaces as [`ReadStatError::DateOverflow`] rather than a clamped value.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn checked_f64_to_i64(v: f64) -> Option<i64> {
    // i64::MAX as f64 rounds up to 2^63, which is not representable as i64, so
    // use a strict upper bound to keep the subsequent `as` cast exact.
    if v.is_finite() && v >= i64::MIN as f64 && v < i64::MAX as f64 {
        Some(v as i64)
    } else {
        None
    }
}

/// Converts an `f64` to `i32`, returning `None` for non-finite or out-of-range
/// values instead of silently saturating.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn checked_f64_to_i32(v: f64) -> Option<i32> {
    if v.is_finite() && v >= f64::from(i32::MIN) && v < f64::from(i32::MAX) {
        Some(v as i32)
    } else {
        None
    }
}

/// Converts a SAS datetime (seconds since 1960-01-01, possibly fractional) to
/// a Unix-epoch timestamp at the given sub-second `scale` (1e3 for ms, 1e6 for
/// µs, 1e9 for ns). Rounds rather than truncates: f64 representation error at
/// SAS-datetime magnitudes (~1.9e9 s) is larger than one sub-second unit, so
/// truncation would land one unit low about half the time.
#[inline]
fn sas_datetime_to_unix_subsec(val: f64, scale: f64) -> Option<i64> {
    #[allow(clippy::cast_precision_loss)]
    checked_f64_to_i64(((val - SEC_SHIFT as f64) * scale).round())
}

/// Converts a SAS time (seconds since midnight, possibly fractional) to
/// microseconds, rounding rather than truncating.
#[inline]
fn sas_time_to_us(val: f64) -> Option<i64> {
    checked_f64_to_i64((val * 1_000_000.0).round())
}

/// FFI callback that extracts a single cell value during row parsing.
///
/// Called for every cell in every row. Appends the value directly into the
/// appropriate typed Arrow [`ColumnBuilder`] in [`ReadStatData::builders`],
/// eliminating intermediate `String` allocations for string columns.
/// Tracks row completion for progress reporting.
///
/// # Safety
///
/// - `variable` must be a valid pointer to a `readstat_variable_t` produced by the C parser.
/// - `value` must be a valid `readstat_value_t` produced by the C parser.
/// - `ctx` must be a valid pointer to a [`ReadStatData`] instance that outlives this call.
/// - This function must only be called by the `ReadStat` C library as a registered callback.
#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub(crate) extern "C" fn handle_value(
    obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let d = unsafe { &mut *ctx.cast::<ReadStatData>() };

    // get index, type, and missingness
    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };
    let value_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_value_type(value) };
    let is_missing: c_int = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    debug!("chunk_rows_to_process is {}", d.chunk_rows_to_process);
    debug!("chunk_row_start is {}", d.chunk_row_start);
    debug!("chunk_row_end is {}", d.chunk_row_end);
    debug!("chunk_rows_processed is {}", d.chunk_rows_processed);
    debug!("var_count is {}", d.var_count);
    debug!("obs_index is {obs_index}");
    debug!("var_index is {var_index}");
    debug!("value_type is {value_type:#?}");
    debug!("is_missing is {is_missing}");

    // Determine the column index for storage, applying column filter if active
    let col_index = if let Some(ref filter) = d.column_filter {
        if let Some(&mapped) = filter.get(&var_index) {
            mapped
        } else {
            // This variable is not selected; skip it but still check row boundary
            if var_index == (d.total_var_count - 1) {
                d.chunk_rows_processed += 1;
                if let Some(trp) = &d.total_rows_processed {
                    trp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            return ReadStatHandler::READSTAT_HANDLER_OK as c_int;
        }
    } else {
        var_index
    };

    // Records a builder/value mismatch and aborts parsing gracefully. A panic
    // here would be an abort: this is an `extern "C"` callback, so unwinding
    // is not an option. Reachable only if the file's data section disagrees
    // with the metadata the builders were built from (e.g. the file changed on
    // disk between the metadata and data parses).
    macro_rules! type_mismatch_abort {
        () => {{
            d.abort_error = Some(ReadStatError::Other(format!(
                "ReadStat value type did not match the expected Arrow builder for column index {col_index}"
            )));
            return ReadStatHandler::READSTAT_HANDLER_ABORT as c_int;
        }};
    }

    // Append value directly into the typed Arrow builder
    let Some(builder) = d.builders.get_mut(col_index as usize) else {
        type_mismatch_abort!();
    };

    // Records a date/time conversion overflow and aborts parsing.
    macro_rules! date_overflow_abort {
        () => {{
            d.abort_error = Some(ReadStatError::DateOverflow);
            return ReadStatHandler::READSTAT_HANDLER_ABORT as c_int;
        }};
    }

    match value_type {
        readstat_sys::readstat_type_e_READSTAT_TYPE_STRING
        | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
            let ColumnBuilder::Str(sb) = builder else {
                type_mismatch_abort!();
            };
            if is_missing == 1 {
                sb.append_null();
            } else {
                let ptr = unsafe { readstat_sys::readstat_string_value(value) };
                if ptr.is_null() {
                    sb.append_null();
                } else {
                    let cstr = unsafe { std::ffi::CStr::from_ptr(ptr) };
                    // Fast path: valid UTF-8 (the common case for SAS data)
                    if let Ok(s) = cstr.to_str() {
                        sb.append_value(s);
                    } else {
                        // Lossy fallback for rare non-UTF-8 data
                        let s = String::from_utf8_lossy(cstr.to_bytes());
                        sb.append_value(s.as_ref());
                    }
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
            if is_missing == 1 {
                builder.append_null();
            } else {
                let v = unsafe { readstat_sys::readstat_int8_value(value) };
                debug!("value is {v:#?}");
                // Schema maps Int8 → Int16, so widen
                if let ColumnBuilder::Int16(b) = builder {
                    b.append_value(i16::from(v));
                } else {
                    type_mismatch_abort!();
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
            if is_missing == 1 {
                builder.append_null();
            } else {
                let v = unsafe { readstat_sys::readstat_int16_value(value) };
                debug!("value is {v:#?}");
                if let ColumnBuilder::Int16(b) = builder {
                    b.append_value(v);
                } else {
                    type_mismatch_abort!();
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
            if is_missing == 1 {
                builder.append_null();
            } else {
                let v = unsafe { readstat_sys::readstat_int32_value(value) };
                debug!("value is {v:#?}");
                if let ColumnBuilder::Int32(b) = builder {
                    b.append_value(v);
                } else {
                    type_mismatch_abort!();
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
            if is_missing == 1 {
                builder.append_null();
            } else {
                let raw = unsafe { readstat_sys::readstat_float_value(value) };
                debug!("value (before parsing) is {raw:#?}");
                let val = round_decimal_f32(raw);
                debug!("value (after parsing) is {val:#?}");
                if let ColumnBuilder::Float32(b) = builder {
                    b.append_value(val);
                } else {
                    type_mismatch_abort!();
                }
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
            let var_format_class = d.vars.get(&col_index).and_then(|vm| vm.var_format_class);

            if is_missing == 1 {
                builder.append_null();
            } else {
                let raw = unsafe { readstat_sys::readstat_double_value(value) };
                debug!("value (before parsing) is {raw:#?}");
                let val = round_decimal_f64(raw);
                debug!("value (after parsing) is {val:#?}");

                match var_format_class {
                    None => {
                        if let ColumnBuilder::Float64(b) = builder {
                            b.append_value(val);
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::Date) => {
                        if let ColumnBuilder::Date32(b) = builder {
                            match checked_f64_to_i32(val)
                                .and_then(|days| days.checked_sub(DAY_SHIFT))
                            {
                                Some(shifted) => b.append_value(shifted),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::DateTime) => {
                        if let ColumnBuilder::TimestampSecond(b) = builder {
                            match checked_f64_to_i64(val).and_then(|s| s.checked_sub(SEC_SHIFT)) {
                                Some(shifted) => b.append_value(shifted),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => {
                        if let ColumnBuilder::TimestampMillisecond(b) = builder {
                            match sas_datetime_to_unix_subsec(val, 1e3) {
                                Some(v) => b.append_value(v),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                        if let ColumnBuilder::TimestampMicrosecond(b) = builder {
                            match sas_datetime_to_unix_subsec(val, 1e6) {
                                Some(v) => b.append_value(v),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => {
                        if let ColumnBuilder::TimestampNanosecond(b) = builder {
                            match sas_datetime_to_unix_subsec(val, 1e9) {
                                Some(v) => b.append_value(v),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::Time) => {
                        if let ColumnBuilder::Time32Second(b) = builder {
                            match checked_f64_to_i32(val) {
                                Some(v) => b.append_value(v),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                    Some(ReadStatVarFormatClass::TimeWithMicroseconds) => {
                        if let ColumnBuilder::Time64Microsecond(b) = builder {
                            match sas_time_to_us(val) {
                                Some(v) => b.append_value(v),
                                None => date_overflow_abort!(),
                            }
                        } else {
                            type_mismatch_abort!();
                        }
                    }
                }
            }
        }
        _ => {
            d.abort_error = Some(ReadStatError::Other(format!(
                "ReadStat returned an unsupported value type ({value_type}) for column index {col_index}"
            )));
            return ReadStatHandler::READSTAT_HANDLER_ABORT as c_int;
        }
    }

    // if row is complete (use total_var_count for boundary detection)
    if var_index == (d.total_var_count - 1) {
        d.chunk_rows_processed += 1;
        if let Some(trp) = &d.total_rows_processed {
            trp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

#[cfg(test)]
mod tests {
    use super::*;

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        // --- round_decimal_f64 ---

        proptest! {
            #[test]
            fn round_f64_is_idempotent(v in any::<f64>()) {
                let once = round_decimal_f64(v);
                let twice = round_decimal_f64(once);
                // Use value equality, not bit equality: rounding tiny negatives
                // to zero may flip -0.0 → 0.0 (both equal per IEEE 754).
                prop_assert!((once == twice) || (once.is_nan() && twice.is_nan()),
                    "not idempotent: round({}) = {}, round(round({})) = {}", v, once, v, twice);
            }

            #[test]
            fn round_f64_preserves_sign(v in any::<f64>().prop_filter("finite nonzero", |v| v.is_finite() && *v != 0.0)) {
                let rounded = round_decimal_f64(v);
                prop_assert_eq!(v.is_sign_positive(), rounded.is_sign_positive());
            }

            #[test]
            fn round_f64_preserves_finiteness(v in any::<f64>()) {
                let rounded = round_decimal_f64(v);
                prop_assert_eq!(v.is_finite(), rounded.is_finite());
            }

            #[test]
            fn round_f64_bounded_error(v in any::<f64>().prop_filter("finite", |v| v.is_finite())) {
                let rounded = round_decimal_f64(v);
                let error = (v - rounded).abs();
                // Rounding to 14 decimal places gives at most 0.5e-14 = 5e-15 error in
                // exact arithmetic.  However, the final `int_part + rounded_frac` addition
                // cannot be more precise than 1 ULP of the result.  For |v| in [32, 64) that
                // ULP is 2^-47 ≈ 7.1e-15, which exceeds 5e-15 — so the bound must scale
                // with the magnitude of v.
                let magnitude_error = v.abs() * f64::EPSILON;
                prop_assert!(error <= 5e-15 + magnitude_error,
                    "error {} too large for input {} (bound {})", error, v, 5e-15 + magnitude_error);
            }

            #[test]
            fn round_f64_passthrough_nonfinite(v in prop::num::f64::ANY.prop_filter("non-finite", |v| !v.is_finite())) {
                let rounded = round_decimal_f64(v);
                prop_assert_eq!(v.to_bits(), rounded.to_bits());
            }
        }

        // --- round_decimal_f32 ---

        proptest! {
            #[test]
            fn round_f32_is_idempotent(v in any::<f32>()) {
                let once = round_decimal_f32(v);
                let twice = round_decimal_f32(once);
                // Use value equality, not bit equality: rounding tiny negatives
                // to zero may flip -0.0 → 0.0 (both equal per IEEE 754).
                prop_assert!((once == twice) || (once.is_nan() && twice.is_nan()),
                    "not idempotent: round({}) = {}, round(round({})) = {}", v, once, v, twice);
            }

            #[test]
            fn round_f32_preserves_sign(v in any::<f32>().prop_filter("finite nonzero", |v| v.is_finite() && *v != 0.0)) {
                let rounded = round_decimal_f32(v);
                prop_assert_eq!(v.is_sign_positive(), rounded.is_sign_positive());
            }

            #[test]
            fn round_f32_preserves_finiteness(v in any::<f32>()) {
                let rounded = round_decimal_f32(v);
                prop_assert_eq!(v.is_finite(), rounded.is_finite());
            }

            #[test]
            fn round_f32_passthrough_nonfinite(v in prop::num::f32::ANY.prop_filter("non-finite", |v| !v.is_finite())) {
                let rounded = round_decimal_f32(v);
                prop_assert_eq!(v.to_bits(), rounded.to_bits());
            }
        }

        // --- Epoch shift arithmetic ---

        proptest! {
            /// Any valid SAS date value (days since 1960-01-01) within the representable
            /// i32 range should not overflow when shifted to Unix epoch.
            #[test]
            fn day_shift_no_overflow(sas_days in (i32::MIN + DAY_SHIFT)..=i32::MAX) {
                let shifted = sas_days.checked_sub(DAY_SHIFT);
                prop_assert!(shifted.is_some(), "DAY_SHIFT overflow for sas_days={}", sas_days);
            }

            /// Any valid SAS datetime value (seconds since 1960-01-01) within the
            /// representable i64 range should not overflow when shifted to Unix epoch.
            #[test]
            fn sec_shift_no_overflow(sas_secs in (i64::MIN + SEC_SHIFT)..=i64::MAX) {
                let shifted = sas_secs.checked_sub(SEC_SHIFT);
                prop_assert!(shifted.is_some(), "SEC_SHIFT overflow for sas_secs={}", sas_secs);
            }

            /// Round-trip: SAS days → Unix days → SAS days
            #[test]
            fn day_shift_round_trip(sas_days in (i32::MIN + DAY_SHIFT)..=i32::MAX) {
                let unix_days = sas_days - DAY_SHIFT;
                let back = unix_days + DAY_SHIFT;
                prop_assert_eq!(sas_days, back);
            }

            /// Round-trip: SAS seconds → Unix seconds → SAS seconds
            #[test]
            fn sec_shift_round_trip(sas_secs in (i64::MIN + SEC_SHIFT)..=i64::MAX) {
                let unix_secs = sas_secs - SEC_SHIFT;
                let back = unix_secs + SEC_SHIFT;
                prop_assert_eq!(sas_secs, back);
            }
        }
    } // end property_tests

    mod subsecond_conversion {
        use super::*;

        /// A modern SAS datetime with .123 fractional seconds. The nearest f64
        /// to `…800.123` lies just below it, so truncation (the old behavior)
        /// yielded `…122` ms; rounding must yield `…123`.
        #[test]
        fn datetime_ms_rounds_instead_of_truncates() {
            // 2021-01-20 12:30:00.123 as a SAS datetime (seconds since 1960)
            let sas = 1_926_851_400.123_f64;
            let ms = sas_datetime_to_unix_subsec(sas, 1e3).unwrap();
            assert_eq!(ms % 1000, 123, "millisecond component must survive");
            assert_eq!(ms, (1_926_851_400 - SEC_SHIFT) * 1000 + 123);
        }

        #[test]
        fn datetime_us_rounds_instead_of_truncates() {
            let sas = 1_926_851_400.123_456_f64;
            let us = sas_datetime_to_unix_subsec(sas, 1e6).unwrap();
            assert_eq!(us % 1_000_000, 123_456);
        }

        #[test]
        fn datetime_ns_rounds_to_f64_precision() {
            // At ~1.9e9 seconds an f64 holds ~µs precision; the ns conversion
            // must still round to the nearest representable value rather than
            // truncate below it.
            let sas = 1_926_851_400.123_f64;
            let ns = sas_datetime_to_unix_subsec(sas, 1e9).unwrap();
            let expected = (1_926_851_400 - SEC_SHIFT) * 1_000_000_000 + 123_000_000;
            assert!(
                (ns - expected).abs() <= 1_000,
                "ns conversion off by more than f64 precision: {ns} vs {expected}"
            );
        }

        #[test]
        fn time_us_rounds_instead_of_truncates() {
            // 13:45:07.123456 as a SAS time (seconds since midnight)
            let sas = 49_507.123_456_f64;
            let us = sas_time_to_us(sas).unwrap();
            assert_eq!(us, 49_507_123_456);
        }

        #[test]
        fn datetime_ms_non_finite_is_none() {
            assert_eq!(sas_datetime_to_unix_subsec(f64::NAN, 1e3), None);
            assert_eq!(sas_datetime_to_unix_subsec(f64::INFINITY, 1e9), None);
        }

        /// Sweep many fractional values: the converted ms component must match
        /// the decimal fraction exactly for all of them (the old truncation
        /// failed for roughly half).
        #[test]
        fn datetime_ms_exact_across_fractions() {
            let base = 2_000_000_000_i64; // SAS seconds, year ~2023
            for frac in 0..1000 {
                #[allow(clippy::cast_precision_loss)]
                let sas = base as f64 + f64::from(frac) / 1000.0;
                let ms = sas_datetime_to_unix_subsec(sas, 1e3).unwrap();
                assert_eq!(
                    ms,
                    (base - SEC_SHIFT) * 1000 + i64::from(frac),
                    "wrong ms for fraction .{frac:03}"
                );
            }
        }
    }
}
