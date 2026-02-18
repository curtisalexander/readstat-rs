//! Variable types and value dispatch for SAS data.
//!
//! [`ReadStatVar`] is the core typed value enum representing a single cell from a SAS
//! dataset. It handles the mapping from ReadStat C types to Rust types, including
//! epoch-shifted date/time conversions (SAS epoch: 1960-01-01, Unix epoch: 1970-01-01).
//!
//! [`ReadStatVarFormatClass`] classifies SAS format strings into semantic categories
//! (Date, DateTime, Time, and their sub-second precision variants), which determines
//! the Arrow data type used during conversion.

use log::debug;
use num_derive::FromPrimitive;
use serde::Serialize;
use std::{collections::BTreeMap, os::raw::c_int};

use crate::{common::ptr_to_string, err::ReadStatError, rs_metadata::ReadStatVarMetadata};

/// Significant digits preserved during float formatting.
const DIGITS: usize = 14;
/// SAS epoch (1960-01-01) to Unix epoch (1970-01-01) offset in days.
const DAY_SHIFT: i32 = 3653;
/// SAS epoch to Unix epoch offset in seconds.
const SEC_SHIFT: i64 = 315619200;

/// A typed value from a single cell in a SAS dataset.
///
/// Each variant wraps an `Option` where `None` represents a SAS system missing value.
/// Date and time variants store epoch-shifted values ready for Arrow conversion:
/// - Dates: days since Unix epoch (1970-01-01)
/// - DateTimes: seconds/millis/micros/nanos since Unix epoch
/// - Times: seconds or microseconds since midnight
#[derive(Debug, Clone)]
pub enum ReadStatVar {
    /// UTF-8 string value.
    ReadStat_String(Option<String>),
    /// 8-bit signed integer.
    ReadStat_i8(Option<i8>),
    /// 16-bit signed integer.
    ReadStat_i16(Option<i16>),
    /// 32-bit signed integer.
    ReadStat_i32(Option<i32>),
    /// 32-bit floating point.
    ReadStat_f32(Option<f32>),
    /// 64-bit floating point.
    ReadStat_f64(Option<f64>),
    /// Date as days since Unix epoch (Arrow `Date32`).
    ReadStat_Date(Option<i32>),
    /// DateTime as seconds since Unix epoch (Arrow `Timestamp(Second)`).
    ReadStat_DateTime(Option<i64>),
    /// DateTime as milliseconds since Unix epoch (Arrow `Timestamp(Millisecond)`).
    ReadStat_DateTimeWithMilliseconds(Option<i64>),
    /// DateTime as microseconds since Unix epoch (Arrow `Timestamp(Microsecond)`).
    ReadStat_DateTimeWithMicroseconds(Option<i64>),
    /// DateTime as nanoseconds since Unix epoch (Arrow `Timestamp(Nanosecond)`).
    ReadStat_DateTimeWithNanoseconds(Option<i64>),
    /// Time as seconds since midnight (Arrow `Time32(Second)`).
    ReadStat_Time(Option<i32>),
    /// Time as microseconds since midnight (Arrow `Time64(Microsecond)`).
    ReadStat_TimeWithMicroseconds(Option<i64>),
}

impl ReadStatVar {
    /// Extracts a typed Rust value from a raw ReadStat C value.
    ///
    /// Dispatches on `value_type` to call the appropriate ReadStat C accessor,
    /// handles missing values, and applies epoch shifts for date/time types
    /// based on the variable's [`ReadStatVarFormatClass`].
    pub fn get_readstat_value(
        value: readstat_sys::readstat_value_t,
        value_type: readstat_sys::readstat_type_t,
        is_missing: c_int,
        vars: &BTreeMap<i32, ReadStatVarMetadata>,
        var_index: i32,
    ) -> Result<Self, ReadStatError> {
        match value_type {
            readstat_sys::readstat_type_e_READSTAT_TYPE_STRING
            | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
                if is_missing == 1 {
                    // return
                    Ok(Self::ReadStat_String(None))
                } else {
                    // get value
                    let value =
                        unsafe { ptr_to_string(readstat_sys::readstat_string_value(value)) };

                    // debug
                    debug!("value is {:#?}", &value);

                    // return
                    Ok(Self::ReadStat_String(Some(value)))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
                if is_missing == 1 {
                    Ok(Self::ReadStat_i8(None))
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_int8_value(value) };

                    // debug
                    debug!("value is {:#?}", value);

                    // return
                    Ok(Self::ReadStat_i8(Some(value)))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
                if is_missing == 1 {
                    Ok(Self::ReadStat_i16(None))
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_int16_value(value) };

                    // debug
                    debug!("value is {:#?}", value);

                    // return
                    Ok(Self::ReadStat_i16(Some(value)))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
                if is_missing == 1 {
                    Ok(Self::ReadStat_i32(None))
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_int32_value(value) };

                    // debug
                    debug!("value is {:#?}", value);

                    // return
                    Ok(Self::ReadStat_i32(Some(value)))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
                if is_missing == 1 {
                    Ok(Self::ReadStat_f32(None))
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_float_value(value) };

                    // debug
                    debug!("value (before parsing) is {:#?}", value);

                    let formatted = format!("{1:.0$}", DIGITS, value);
                    let value: f32 = lexical::parse(&formatted)
                        .map_err(|_| ReadStatError::NumericParse(formatted))?;

                    // debug
                    debug!("value (after parsing) is {:#?}", value);

                    // return
                    Ok(Self::ReadStat_f32(Some(value)))
                }
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
                let var_format_class = vars
                    .get(&var_index)
                    .ok_or(ReadStatError::VarIndexNotFound { index: var_index })?
                    .var_format_class;

                if is_missing == 1 {
                    match var_format_class {
                        None => Ok(Self::ReadStat_f64(None)),
                        Some(fc) => match fc {
                            ReadStatVarFormatClass::Date => Ok(Self::ReadStat_Date(None)),
                            ReadStatVarFormatClass::DateTime => Ok(Self::ReadStat_DateTime(None)),
                            ReadStatVarFormatClass::DateTimeWithMilliseconds => {
                                Ok(Self::ReadStat_DateTimeWithMilliseconds(None))
                            }
                            ReadStatVarFormatClass::DateTimeWithMicroseconds => {
                                Ok(Self::ReadStat_DateTimeWithMicroseconds(None))
                            }
                            ReadStatVarFormatClass::DateTimeWithNanoseconds => {
                                Ok(Self::ReadStat_DateTimeWithNanoseconds(None))
                            }
                            ReadStatVarFormatClass::Time => Ok(Self::ReadStat_Time(None)),
                            ReadStatVarFormatClass::TimeWithMicroseconds => {
                                Ok(Self::ReadStat_TimeWithMicroseconds(None))
                            }
                        },
                    }
                } else {
                    // get value
                    let value = unsafe { readstat_sys::readstat_double_value(value) };

                    // debug
                    debug!("value (before parsing) is {:#?}", value);

                    let formatted = format!("{1:.0$}", DIGITS, value);
                    let value: f64 = lexical::parse(&formatted)
                        .map_err(|_| ReadStatError::NumericParse(formatted))?;

                    // debug
                    debug!("value (after parsing) is {:#?}", value);

                    // is double a value or is it really a date, time, or datetime?
                    match var_format_class {
                        None => Ok(Self::ReadStat_f64(Some(value))),
                        Some(fc) => match fc {
                            ReadStatVarFormatClass::Date => Ok(Self::ReadStat_Date(Some(
                                (value as i32)
                                    .checked_sub(DAY_SHIFT)
                                    .ok_or(ReadStatError::DateOverflow)?,
                            ))),
                            ReadStatVarFormatClass::DateTime => Ok(Self::ReadStat_DateTime(Some(
                                (value as i64)
                                    .checked_sub(SEC_SHIFT)
                                    .ok_or(ReadStatError::DateOverflow)?,
                            ))),
                            ReadStatVarFormatClass::DateTimeWithMilliseconds => {
                                Ok(Self::ReadStat_DateTimeWithMilliseconds(Some(
                                    ((value - SEC_SHIFT as f64) * 1000.0) as i64,
                                )))
                            }
                            ReadStatVarFormatClass::DateTimeWithMicroseconds => {
                                Ok(Self::ReadStat_DateTimeWithMicroseconds(Some(
                                    ((value - SEC_SHIFT as f64) * 1000000.0) as i64,
                                )))
                            }
                            ReadStatVarFormatClass::DateTimeWithNanoseconds => {
                                Ok(Self::ReadStat_DateTimeWithNanoseconds(Some(
                                    ((value - SEC_SHIFT as f64) * 1000000000.0) as i64,
                                )))
                            }
                            ReadStatVarFormatClass::Time => Ok(Self::ReadStat_Time(Some(value as i32))),
                            ReadStatVarFormatClass::TimeWithMicroseconds => {
                                Ok(Self::ReadStat_TimeWithMicroseconds(Some(
                                    (value * 1000000.0) as i64,
                                )))
                            }
                        },
                    }
                }
            }
            // exhaustive
            _ => unreachable!(),
        }
    }
}

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
    /// DateTime format with second precision (e.g. `DATETIME22`).
    DateTime,
    /// DateTime format with millisecond precision (e.g. `DATETIME22.3`).
    DateTimeWithMilliseconds,
    /// DateTime format with microsecond precision (e.g. `DATETIME22.6`).
    DateTimeWithMicroseconds,
    /// DateTime format with nanosecond precision (e.g. `DATETIME22.9`).
    DateTimeWithNanoseconds,
    /// Time format with second precision (e.g. `TIME8`).
    Time,
    /// Time format with microsecond precision (e.g. `TIME15.6`).
    TimeWithMicroseconds,
}

/// The storage type of a SAS variable, as reported by the ReadStat C library.
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
