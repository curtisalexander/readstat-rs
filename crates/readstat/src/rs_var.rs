//! Variable types and format classification for SAS data.
//!
//! [`ReadStatVarFormatClass`] classifies SAS format strings into semantic categories
//! (Date, `DateTime`, Time, and their sub-second precision variants), which determines
//! the Arrow data type used during conversion.
//!
//! [`ReadStatVarType`] and [`ReadStatVarTypeClass`] map `ReadStat` C type codes to Rust
//! enums, used during schema construction and builder allocation.

use num_derive::FromPrimitive;
use serde::Serialize;

/// Semantic classification of a SAS format string.
///
/// Determines the Arrow data type used for date/time/datetime variables:
///
/// | Variant | Arrow Type |
/// |---------|------------|
/// | `Date` | `Date32` |
/// | `DateTime` | `Timestamp(Second)` |
/// | `DateTimeWithMilliseconds` | `Timestamp(Millisecond)` |
/// | `DateTimeWithMicroseconds` | `Timestamp(Microsecond)` |
/// | `DateTimeWithNanoseconds` | `Timestamp(Nanosecond)` |
/// | `Time` | `Time32(Second)` |
/// | `TimeWithMicroseconds` | `Time64(Microsecond)` |
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReadStatVarFormatClass {
    /// Date format (e.g. `DATE9`, `MMDDYY10`). Maps to Arrow `Date32`.
    Date,
    /// `DateTime` format with second precision (e.g. `DATETIME22`).
    DateTime,
    /// `DateTime` format with millisecond precision (e.g. `DATETIME22.3`).
    DateTimeWithMilliseconds,
    /// `DateTime` format with microsecond precision (e.g. `DATETIME22.6`).
    DateTimeWithMicroseconds,
    /// `DateTime` format with nanosecond precision (e.g. `DATETIME22.9`).
    DateTimeWithNanoseconds,
    /// Time format with second precision (e.g. `TIME8`).
    Time,
    /// Time format with microsecond precision (e.g. `TIME15.6`).
    TimeWithMicroseconds,
}

/// The storage type of a SAS variable, as reported by the `ReadStat` C library.
#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarType {
    /// Variable-length string.
    String = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING as isize,
    /// 8-bit signed integer.
    Int8 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 as isize,
    /// 16-bit signed integer.
    Int16 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 as isize,
    /// 32-bit signed integer.
    Int32 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 as isize,
    /// 32-bit floating point.
    Float = readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT as isize,
    /// 64-bit floating point (also used for dates/times via format class).
    Double = readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE as isize,
    /// String reference (interned string).
    StringRef = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF as isize,
    /// Unknown or unrecognized type.
    Unknown,
}

/// High-level type class of a SAS variable: string or numeric.
#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarTypeClass {
    /// Character/string data.
    String = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_STRING as isize,
    /// Numeric data (integers, floats, dates, times).
    Numeric = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_NUMERIC as isize,
}
