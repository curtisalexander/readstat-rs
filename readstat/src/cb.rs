use arrow2::array::{MutableUtf8Array, Int8Vec, Int16Vec, Int32Vec, Int64Vec, Float32Vec, Float64Vec};
use arrow2::datatypes::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use log::debug;
use num_traits::FromPrimitive;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};

use crate::{
    formats,
    rs_data::ReadStatData,
    rs_metadata::{
        ReadStatCompress, ReadStatEndian, ReadStatFormatClass, ReadStatVar, ReadStatVarMetadata,
        ReadStatVarType, ReadStatVarTypeClass,
    },
    ReadStatMetadata,
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
unsafe fn ptr_to_string(x: *const i8) -> String {
    if x.is_null() {
        String::new()
    } else {
        CStr::from_ptr(x).to_str().unwrap().to_owned()
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
    let mut m = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

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
    m.row_count = rc;
    m.var_count = vc;
    m.table_name = table_name;
    m.file_label = file_label;
    m.file_encoding = file_encoding;
    m.version = version;
    m.is64bit = is64bit;
    m.creation_time = ct;
    m.modified_time = mt;
    m.compression = compression;
    m.endianness = endianness;

    debug!("metadata struct is {:#?}", &m);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

/*
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
*/

pub extern "C" fn handle_variable(
    index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

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
    m.vars.insert(
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

    debug!("chunk_rows_to_process is {}", d.chunk_rows_to_process);
    debug!("chunk_row_start is {}", d.chunk_row_start);
    debug!("chunk_row_end is {}", d.chunk_row_end);
    debug!("chunk_rows_processed is {}", d.chunk_rows_processed);
    debug!("var_count is {}", d.var_count);
    debug!("obs_index is {}", obs_index);
    debug!("var_index is {}", var_index);
    debug!("value_type is {:#?}", &value_type);
    debug!("is_missing is {}", is_missing);

    // get value and push into arrays
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

            // append to array
            d.arrays[var_index as usize]
                .as_mut_any()
                .downcast_mut::<MutableUtf8Array<i32>>()
                .unwrap()
                .push(if is_missing == 0 { Some(value) } else { None });
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
            // get value
            let value = unsafe { readstat_sys::readstat_int8_value(value) };

            // debug
            debug!("value is {:#?}", value);

            // append to array
            d.arrays[var_index as usize]
                .as_mut_any()
                .downcast_mut::<Int8Vec>()
                .unwrap()
                .push(if is_missing == 0 { Some(value) } else { None });
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
            // get value
            let value = unsafe { readstat_sys::readstat_int16_value(value) };

            // debug
            debug!("value is {:#?}", value);

            // append to array
            d.arrays[var_index as usize]
                .as_mut_any()
                .downcast_mut::<Int16Vec>()
                .unwrap()
                .push(if is_missing == 0 { Some(value) } else { None });
        }
        readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
            // get value
            let value = unsafe { readstat_sys::readstat_int32_value(value) };

            // debug
            debug!("value is {:#?}", value);

            // append to array
            d.arrays[var_index as usize]
                .as_mut_any()
                .downcast_mut::<Int32Vec>()
                .unwrap()
                .push(if is_missing == 0 { Some(value) } else { None });
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

            // append to array
            d.arrays[var_index as usize]
                .as_mut_any()
                .downcast_mut::<Float32Vec>()
                .unwrap()
                .push(if is_missing == 0 { Some(value) } else { None });
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
            let value = match d.vars.get(&var_index).unwrap().var_format_class {
                None => ReadStatVar::ReadStat_f64(value),
                Some(fc) => match fc {
                    ReadStatFormatClass::Date => {
                        ReadStatVar::ReadStat_Date((value as i32).checked_sub(DAY_SHIFT).unwrap())
                    }
                    ReadStatFormatClass::DateTime => ReadStatVar::ReadStat_DateTime(
                        (value as i64).checked_sub(SEC_SHIFT).unwrap(),
                    ),
                    ReadStatFormatClass::DateTimeWithMilliseconds => {
                        ReadStatVar::ReadStat_DateTimeWithMilliseconds(
                            (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000,
                        )
                    }
                    ReadStatFormatClass::DateTimeWithMicroseconds => {
                        ReadStatVar::ReadStat_DateTimeWithMicroseconds(
                            (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000000,
                        )
                    }
                    ReadStatFormatClass::DateTimeWithNanoseconds => ReadStatVar::ReadStat_DateTimeWithNanoseconds(
                        (value as i64).checked_sub(SEC_SHIFT).unwrap() * 1000000000,
                    ),
                    ReadStatFormatClass::Time => ReadStatVar::ReadStat_Time(value as i32),
                },
            };

            // append to builder
            match value {
                ReadStatVar::ReadStat_Date(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Int32Vec>()
                        .unwrap()
                        .to(DataType::Date32)
                        .push(if is_missing == 0 { Some(v) } else { None })
                }
                ReadStatVar::ReadStat_DateTime(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Int64Vec>()
                        .unwrap()
                        .to(DataType::Timestamp(TimeUnit::Second, None))
                        .push(if is_missing == 0 { Some(v) } else { None });
                }
                ReadStatVar::ReadStat_DateTimeWithMilliseconds(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Int64Vec>()
                        .unwrap()
                        .to(DataType::Timestamp(TimeUnit::Millisecond, None))
                        .push(if is_missing == 0 { Some(v) } else { None });
                }
                ReadStatVar::ReadStat_DateTimeWithMicroseconds(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Int64Vec>()
                        .unwrap()
                        .to(DataType::Timestamp(TimeUnit::Microsecond, None))
                        .push(if is_missing == 0 { Some(v) } else { None });
                }
                ReadStatVar::ReadStat_DateTimeWithNanoseconds(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Int64Vec>()
                        .unwrap()
                        .to(DataType::Timestamp(TimeUnit::Nanosecond, None))
                        .push(if is_missing == 0 { Some(v) } else { None });
                }
                ReadStatVar::ReadStat_Time(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Int32Vec>()
                        .unwrap()
                        .to(DataType::Time32(TimeUnit::Second))
                        .push(if is_missing == 0 { Some(v) } else { None });
                }
                ReadStatVar::ReadStat_f64(v) => {
                    // append to array
                    d.arrays[var_index as usize]
                        .as_mut_any()
                        .downcast_mut::<Float64Vec>()
                        .unwrap()
                        .push(if is_missing == 0 { Some(v) } else { None });
                }
                // exhaustive
                _ => unreachable!(),
            }
        }
        // exhaustive
        _ => unreachable!(),
    }



    // if row is complete
    if var_index == (d.var_count - 1) {
        d.chunk_rows_processed += 1;
        if let Some(trp) = &d.total_rows_processed {
            trp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    };

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
