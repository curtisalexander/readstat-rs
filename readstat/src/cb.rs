use chrono::{Duration, NaiveDateTime, TimeZone, Utc};
use log::debug;
use num_traits::FromPrimitive;
use readstat_sys;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};

use crate::rs::{
    ReadStatCompress, ReadStatData, ReadStatEndian, ReadStatVar, ReadStatVarIndexAndName,
    ReadStatVarMetadata, ReadStatVarType, ReadStatVarTypeClass,
};
use crate::Reader;

const ROWS: usize = 10000;

// C types
#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
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
    let table_name_ptr = unsafe { readstat_sys::readstat_get_table_name(metadata) };
    let table_name = if table_name_ptr == std::ptr::null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(table_name_ptr).to_str().unwrap().to_owned() }
    };
    let file_label_ptr = unsafe { readstat_sys::readstat_get_file_label(metadata) };
    let file_label = if file_label_ptr == std::ptr::null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(file_label_ptr).to_str().unwrap().to_owned() }
    };
    let file_encoding_ptr = unsafe { readstat_sys::readstat_get_file_encoding(metadata) };
    let file_encoding = if file_encoding_ptr == std::ptr::null() {
        String::new()
    } else {
        unsafe {
            CStr::from_ptr(file_encoding_ptr)
                .to_str()
                .unwrap()
                .to_owned()
        }
    };
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

    // insert into ReadStatData struct
    d.row_count = rc;
    d.var_count = vc;
    d.table_name = table_name;
    d.file_label = file_label;
    d.file_encoding = file_encoding;
    d.version = version;
    d.is64bit = is64bit;
    d.creation_time = ct;
    d.modified_time = mt;
    d.compression = compression;
    d.endianness = endianness;

    debug!("d struct is {:#?}", d);

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
    let var_name_ptr = unsafe { readstat_sys::readstat_variable_get_name(variable) };
    let var_name = if var_name_ptr == std::ptr::null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(var_name_ptr).to_str().unwrap().to_owned() }
    };
    let var_label_ptr = unsafe { readstat_sys::readstat_variable_get_label(variable) };
    let var_label = if var_label_ptr == std::ptr::null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(var_label_ptr).to_str().unwrap().to_owned() }
    };

    let var_format_ptr = unsafe { readstat_sys::readstat_variable_get_format(variable) };
    let var_format = if var_format_ptr == std::ptr::null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(var_format_ptr).to_str().unwrap().to_owned() }
    };

    debug!("var_type is {:#?}", &var_type);
    debug!("var_type_class is {:#?}", &var_type_class);
    debug!("var_name is {}", &var_name);
    debug!("var_label is {}", &var_label);
    debug!("var_format is {}", &var_format);

    // insert into BTreeMap within ReadStatData struct
    d.vars.insert(
        ReadStatVarIndexAndName::new(index, var_name),
        ReadStatVarMetadata::new(var_type, var_type_class, var_label, var_format),
    );

    debug!("d struct is {:#?}", d);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value(
    #[allow(unused_variables)] obs_index: c_int,
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

    // if first row and first variable, allocate row and rows
    if obs_index == 0 && var_index == 0 {
        // Vec containing a single row, needs capacity = number of variables
        d.row = Vec::with_capacity(d.var_count as usize);
        // Vec containing all rows, needs capacity = number of rows
        // d.rows = Vec::with_capacity(d.row_count as usize);
        // Allocate rows
        d.rows = match d.reader {
            Reader::stream => {
                if d.row_count < ROWS as i32 {
                    Vec::with_capacity(d.row_count as usize)
                } else {
                    Vec::with_capacity(ROWS)
                }
            }
            Reader::mem => Vec::with_capacity(d.row_count as usize),
        }
    }

    debug!("var_index is {}", var_index);
    debug!("value_type is {:#?}", &value_type);
    debug!("is_missing is {}", is_missing);

    // get value and push into row within ReadStatData struct
    if is_missing == 0 {
        let mut value: ReadStatVar = match value_type {
            readstat_sys::readstat_type_e_READSTAT_TYPE_STRING
            | readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF => {
                ReadStatVar::ReadStat_String(unsafe {
                    CStr::from_ptr(readstat_sys::readstat_string_value(value))
                        .to_str()
                        .unwrap()
                        .to_owned()
                })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 => {
                ReadStatVar::ReadStat_i8(unsafe { readstat_sys::readstat_int8_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 => {
                ReadStatVar::ReadStat_i16(unsafe { readstat_sys::readstat_int16_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 => {
                ReadStatVar::ReadStat_i32(unsafe { readstat_sys::readstat_int32_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT => {
                ReadStatVar::ReadStat_f32(unsafe { readstat_sys::readstat_float_value(value) })
            }
            readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE => {
                ReadStatVar::ReadStat_f64(unsafe { readstat_sys::readstat_double_value(value) })
            }
            // exhaustive
            _ => unreachable!(),
        };

        debug!("value is {:#?}", value);

        // TODO: check if date/datetime format
        let (_, v) = d.vars.iter().find(|(k, _)| k.var_index == var_index).unwrap();
        if v.var_format.contains("DATETIME") {
            let f = match value {
                ReadStatVar::ReadStat_f64(f) => f as i64,
                _ => 0 as i64,
            };
            // 315619200 = Number of seconds between SAS start date (1960-01-01) and Unix start date (1970-01-01)
            // 315619200 = 60s*60m*24hr*365d*10y + 60s*60m*24hr*3d (3 leap years: 1960, 1964, 1968)
            value = ReadStatVar::ReadStat_String(Utc.timestamp(f, 0).checked_sub_signed(Duration::seconds(315619200)).unwrap().to_rfc3339());
        }

        // push into row
        d.row.push(value);
    } else {
        // For now represent missing values as the unit type
        // When serializing to csv (which is the only output type at the moment),
        //   the unit type is serialized as a missing value
        // For example, the following SAS dataset
        //   | id | name  | age |
        //   |----|-------|-----|
        //   | 4  | Alice | .   |
        //   | 5  | ""    | 30  |
        // would be serialized as the following in csv
        //   id,name,age
        //   4,Alice,,
        //   5,,30
        // And thus any missingness treatment is in fact handled by the tool that
        // consumes the csv file
        let value = ReadStatVar::ReadStat_Missing(());
        debug!("value is {:#?}", &value);

        // push into row
        d.row.push(value);
    }

    // if last variable for a row, push into rows within ReadStatData struct
    if var_index == d.var_count - 1 {
        // collecting ALL rows into memory before ever writing
        // TODO: benchmark changes if were to push (for example) 1,000 rows at a time
        //       into the Vector and then flush to disk in a quasi-streaming fashion
        d.rows.push(d.row.clone());
        // clear row after pushing into rows; has no effect on capacity
        d.row.clear();
    }

    match d.reader {
        Reader::stream => {
            // if rows = buffer limit and last variable then go ahead and write
            if (obs_index % (ROWS as i32 - 1) == 0 || obs_index == d.row_count - 1)
                && var_index == d.var_count - 1
            {
                match d.write() {
                    Ok(()) => (),
                    // Err(e) => d.errors.push(format!("{:#?}", e)),
                    // For now just swallow any errors when writing
                    Err(_) => (),
                };
                d.rows.clear();
            }
        }
        Reader::mem => {
            // if rows = row count and last variable then go ahead and write
            if obs_index == d.row_count - 1 && var_index == d.var_count - 1 {
                match d.write() {
                    Ok(()) => (),
                    // Err(e) => d.errors.push(format!("{:#?}", e)),
                    // For now just swallow any errors when writing
                    Err(_) => (),
                };
            }
        }
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
