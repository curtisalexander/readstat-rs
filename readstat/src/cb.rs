use arrow::array::{
    ArrayRef, Date32Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int8Builder, StringBuilder, Time32SecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use log::debug;
use num_traits::FromPrimitive;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Arc;

use crate::formats;
use crate::rs_data::ReadStatData;
use crate::rs_metadata::{
    ReadStatCompress, ReadStatEndian, ReadStatFormatClass, ReadStatVar, ReadStatVarMetadata,
    ReadStatVarType, ReadStatVarTypeClass,
};

const DIGITS: usize = 14;
const DAY_SHIFT: i32 = 3653;
const SEC_SHIFT: i64 = 315619200;

// C types
#[allow(dead_code)]
#[derive(Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
}

// String out from C pointer
fn ptr_to_string(x: *const i8) -> String {
    if x.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(x).to_str().unwrap().to_owned() }
    }
}

// C callback functions

// TODO: May need a version of handle_metadata that only gets metadata
//       and a version that does very little and instead metadata handling occurs
//       in handle_value function
//       As an example see the below from the readstat binary
//         https://github.com/WizardMac/ReadStat/blob/master/src/bin/readstat.c#L98
pub extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let mut d = unsafe { &mut *(ctx as *mut ReadStatData) };

    // get metadata
    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    let vc: c_int = unsafe { readstat_sys::readstat_get_var_count(metadata) };
    let table_name = unsafe { ptr_to_string(readstat_sys::readstat_get_table_name(metadata)) };
    let file_label = unsafe { ptr_to_string(readstat_sys::readstat_get_file_label(metadata)) };
    let file_encoding =
        unsafe { ptr_to_string(readstat_sys::readstat_get_file_encoding(metadata)) };
    let version: c_int = unsafe { readstat_sys::readstat_get_file_format_version(metadata) };
    let is64bit = unsafe { readstat_sys::readstat_get_file_format_is_64bit(metadata) };
    let ct = NaiveDateTime::from_timestamp(
        unsafe { readstat_sys::readstat_get_creation_time(metadata) },
        0,
    )
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();
    let mt = NaiveDateTime::from_timestamp(
        unsafe { readstat_sys::readstat_get_modified_time(metadata) },
        0,
    )
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();
    let compression = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_get_compression(metadata) as i32
    }) {
        Some(t) => t,
        None => ReadStatCompress::None,
    };
    let endianness = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_get_endianness(metadata) as i32
    }) {
        Some(t) => t,
        None => ReadStatEndian::None,
    };

    // allocate
    d.cols = Vec::with_capacity(vc as usize);

    debug!("row_count is {}", rc);
    debug!("var_count is {}", vc);
    debug!("table_name is {}", &table_name);
    debug!("file_label is {}", &file_label);
    debug!("file_encoding is {}", &file_encoding);
    debug!("version is {}", version);
    debug!("is64bit is {}", is64bit);
    debug!("creation_time is {}", &ct);
    debug!("modified_time is {}", &mt);
    debug!("compression is {:#?}", &compression);
    debug!("endianness is {:#?}", &endianness);

    // insert into ReadStatMetadata struct
    d.metadata.row_count = rc;
    d.metadata.var_count = vc;
    d.metadata.table_name = table_name;
    d.metadata.file_label = file_label;
    d.metadata.file_encoding = file_encoding;
    d.metadata.version = version;
    d.metadata.is64bit = is64bit;
    d.metadata.creation_time = ct;
    d.metadata.modified_time = mt;
    d.metadata.compression = compression;
    d.metadata.endianness = endianness;

    debug!("d.metadata struct is {:#?}", &d.metadata);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_metadata_row_count_only(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let mut d = unsafe { &mut *(ctx as *mut ReadStatData) };

    // get metadata
    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    debug!("row_count is {}", rc);

    // insert into ReadStatMetadata struct
    d.metadata.row_count = rc;
    debug!("d.metadata struct is {:#?}", &d.metadata);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_variable(
    index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let d = unsafe { &mut *(ctx as *mut ReadStatData) };

    // get variable metadata
    let var_type = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_variable_get_type(variable) as i32
    }) {
        Some(t) => t,
        None => ReadStatVarType::Unknown,
    };

    let var_type_class = match FromPrimitive::from_i32(unsafe {
        readstat_sys::readstat_variable_get_type_class(variable) as i32
    }) {
        Some(t) => t,
        None => ReadStatVarTypeClass::Numeric,
    };

    let var_name = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_name(variable)) };
    let var_label = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_label(variable)) };
    let var_format = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_format(variable)) };
    let var_format_class = formats::match_var_format(&var_format);

    debug!("var_type is {:#?}", &var_type);
    debug!("var_type_class is {:#?}", &var_type_class);
    debug!("var_name is {}", &var_name);
    debug!("var_label is {}", &var_label);
    debug!("var_format is {}", &var_format);
    debug!("var_format_class is {:#?}", &var_format_class);

    // insert into BTreeMap within ReadStatMetadata struct
    d.metadata.vars.insert(
        index,
        ReadStatVarMetadata::new(
            var_name.clone(),
            var_type,
            var_type_class,
            var_label,
            var_format,
            var_format_class,
        ),
    );

    // build up Schema
    let var_dt = match &var_type {
        ReadStatVarType::String | ReadStatVarType::StringRef | ReadStatVarType::Unknown => {
            DataType::Utf8
        }
        ReadStatVarType::Int8 | ReadStatVarType::Int16 => DataType::Int16,
        ReadStatVarType::Int32 => DataType::Int32,
        ReadStatVarType::Float => DataType::Float32,
        ReadStatVarType::Double => match var_format_class {
            Some(ReadStatFormatClass::Date) => DataType::Date32,
            Some(ReadStatFormatClass::DateTime) => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
            }
            Some(ReadStatFormatClass::DateTimeWithMilliseconds) => {
                // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
            }
            Some(ReadStatFormatClass::DateTimeWithMicroseconds) => {
                // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            Some(ReadStatFormatClass::DateTimeWithNanoseconds) => {
                // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
            }
            Some(ReadStatFormatClass::Time) => DataType::Time32(arrow::datatypes::TimeUnit::Second),
            None => DataType::Float64,
        },
    };

    d.schema = Schema::try_merge(vec![
        d.schema.clone(),
        Schema::new(vec![Field::new(&var_name, var_dt, true)]),
    ])
    .unwrap();

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value(
    obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let d = unsafe { &mut *(ctx as *mut ReadStatData) };

    // get index, type, and missingness
    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };
    let value_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_value_type(value) };
    let is_missing: c_int = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    debug!("row_count is {}", d.metadata.row_count);
    debug!("var_count is {}", d.metadata.var_count);
    debug!("obs_index is {}", obs_index);
    debug!("var_index is {}", var_index);
    debug!("value_type is {:#?}", &value_type);
    debug!("is_missing is {}", is_missing);

    // allocate columns
    if obs_index == 0 && var_index == 0 {
        d.allocate_cols(d.batch_rows_to_process);
    };

    // get value and push into cols
    match value_type {
        readstat_sys::readstat_type_e_READSTAT_TYPE_STRING
        | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
            // get value
            let value = unsafe {
                CStr::from_ptr(readstat_sys::readstat_string_value(value))
                    .to_str()
                    .unwrap()
                    .to_owned()
            };
            // debug
            debug!("value is {:#?}", &value);
            // append to builder
            if is_missing == 0 {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(value)
                    .unwrap();
            } else {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_null()
                    .unwrap();
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
            // get value
            let value = unsafe { readstat_sys::readstat_int8_value(value) };
            // debug
            debug!("value is {:#?}", value);
            // append to builder
            if is_missing == 0 {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Int8Builder>()
                    .unwrap()
                    .append_value(value)
                    .unwrap();
            } else {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Int8Builder>()
                    .unwrap()
                    .append_null()
                    .unwrap();
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
            // get value
            let value = unsafe { readstat_sys::readstat_int16_value(value) };
            // debug
            debug!("value is {:#?}", value);
            // append to builder
            if is_missing == 0 {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .unwrap()
                    .append_value(value)
                    .unwrap();
            } else {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Int16Builder>()
                    .unwrap()
                    .append_null()
                    .unwrap();
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
            // get value
            let value = unsafe { readstat_sys::readstat_int32_value(value) };
            // debug
            debug!("value is {:#?}", value);
            // append to builder
            if is_missing == 0 {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .unwrap()
                    .append_value(value)
                    .unwrap();
            } else {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .unwrap()
                    .append_null()
                    .unwrap();
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
            // Format as string to truncate float to only contain 14 decimal digits
            // Parse back into float so that the trailing zeroes are trimmed when serializing
            // TODO: Is there an alternative that does not require conversion from and to a float?  // get value
            let value = unsafe { readstat_sys::readstat_float_value(value) };
            let value =
                lexical::parse::<f32, _>(format!("{1:.0$}", DIGITS, lexical::to_string(value)))
                    .unwrap();
            // debug
            debug!("value is {:#?}", value);
            // append to builder
            if is_missing == 0 {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .unwrap()
                    .append_value(value)
                    .unwrap();
            } else {
                d.cols[var_index as usize]
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .unwrap()
                    .append_null()
                    .unwrap();
            }
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
            // Format as string to truncate float to only contain 14 decimal digits
            // Parse back into float so that the trailing zeroes are trimmed when serializing
            // TODO: Is there an alternative that does not require conversion from and to a float?  // get value
            let value = unsafe { readstat_sys::readstat_double_value(value) };
            debug!("value (before truncation) is {:#?}", value);
            let value: f64 = lexical::parse(format!("{1:.0$}", DIGITS, value)).unwrap();
            // debug
            debug!("value (after truncation) is {:#?}", value);

            // is double actually a date?
            let value = match d.metadata.vars.get(&var_index).unwrap().var_format_class {
                None => ReadStatVar::ReadStat_f64(value),
                Some(fc) => match fc {
                    ReadStatFormatClass::Date => {
                        ReadStatVar::ReadStat_Date((value as i32).checked_sub(DAY_SHIFT).unwrap())
                    }
                    ReadStatFormatClass::DateTime => ReadStatVar::ReadStat_DateTime(
                        (value as i64).checked_sub(SEC_SHIFT).unwrap(),
                    ),
                    ReadStatFormatClass::DateTimeWithMilliseconds => {
                        ReadStatVar::ReadStat_DateTime(
                            (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000,
                        )
                    }
                    ReadStatFormatClass::DateTimeWithMicroseconds => {
                        ReadStatVar::ReadStat_DateTime(
                            (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000000,
                        )
                    }
                    ReadStatFormatClass::DateTimeWithNanoseconds => ReadStatVar::ReadStat_DateTime(
                        (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000000000,
                    ),
                    ReadStatFormatClass::Time => ReadStatVar::ReadStat_Time(value as i32),
                },
            };

            // append to builder
            match value {
                ReadStatVar::ReadStat_Date(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<Date32Builder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<Date32Builder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                ReadStatVar::ReadStat_DateTime(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampSecondBuilder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampSecondBuilder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                ReadStatVar::ReadStat_DateTimeWithMilliseconds(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampMillisecondBuilder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampMillisecondBuilder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                ReadStatVar::ReadStat_DateTimeWithMicroseconds(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampMicrosecondBuilder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampMicrosecondBuilder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                ReadStatVar::ReadStat_DateTimeWithNanoseconds(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampNanosecondBuilder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<TimestampNanosecondBuilder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                ReadStatVar::ReadStat_Time(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<Time32SecondBuilder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<Time32SecondBuilder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                ReadStatVar::ReadStat_f64(v) => {
                    if is_missing == 0 {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap()
                            .append_value(v)
                            .unwrap();
                    } else {
                        d.cols[var_index as usize]
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap()
                            .append_null()
                            .unwrap();
                    }
                }
                // exhaustive
                _ => unreachable!(),
            }
        }
        // exhaustive
        _ => unreachable!(),
    }

    // increment
    if var_index == (d.metadata.var_count - 1) {
        d.batch_rows_processed += 1;
    };

    // if last variable for a row, check to see if data should be finalized and written
    if var_index == (d.metadata.var_count - 1)
        && (obs_index as usize) == (d.batch_rows_to_process - 1)
    {
        let arrays: Vec<ArrayRef> = d.cols.iter_mut().map(|builder| builder.finish()).collect();

        d.batch = RecordBatch::try_new(Arc::new(d.schema.clone()), arrays).unwrap();

        // reset
        d.cols.clear();
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
