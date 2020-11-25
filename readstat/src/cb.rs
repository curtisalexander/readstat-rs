use crate::rs::{ReadStatData, ReadStatMetadata, ReadStatVar, ReadStatVarMetadata};

use log::debug;
use readstat_sys;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};

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
    let mut md = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    let vc: c_int = unsafe { readstat_sys::readstat_get_var_count(metadata) };

    md.row_count = rc;
    md.var_count = vc;

    debug!("md struct is {:#?}", md);
    debug!("row_count is {:#?}", md.row_count);
    debug!("var_count is {:#?}", md.var_count);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_variable(
    #[allow(unused_variables)] index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    let md = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let var_name = unsafe {
        CStr::from_ptr(readstat_sys::readstat_variable_get_name(variable))
            .to_str()
            .unwrap()
            .to_owned()
    };

    let var_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_variable_get_type(variable) };

    debug!("md struct is {:#?}", md);
    debug!("var type pushed is {:#?}", var_type);
    debug!("var pushed is {:#?}", &var_name);

    md.vars
        .insert(ReadStatVarMetadata::new(var_index, var_name), var_type);

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value_print(
    #[allow(unused_variables)] obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    let md = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let val_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_value_type(value) };

    let is_missing: c_int = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    if is_missing == 0 {
        let value: ReadStatVar = match val_type {
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
            // _ => ReadStatVarType::ReadStat_String(String::new()),
            _ => unreachable!(),
        };

        match value {
            ReadStatVar::ReadStat_String(s) => print!("{}", s),
            ReadStatVar::ReadStat_i8(i) => print!("{}", i),
            ReadStatVar::ReadStat_i16(i) => print!("{}", i),
            ReadStatVar::ReadStat_i32(i) => print!("{}", i),
            ReadStatVar::ReadStat_f32(f) => print!("{:.6}", f),
            ReadStatVar::ReadStat_f64(f) => print!("{:.6}", f),
        }
    }

    if var_index == md.var_count - 1 {
        print!("\n");
    } else {
        print!("\t");
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}

pub extern "C" fn handle_value(
    #[allow(unused_variables)] obs_index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    value: readstat_sys::readstat_value_t,
    ctx: *mut c_void,
) -> c_int {
    let d = unsafe { &mut *(ctx as *mut ReadStatData) };
    let md = &mut d.metadata;
    let var_count = md.var_count;

    let var_index: c_int = unsafe { readstat_sys::readstat_variable_get_index(variable) };

    let value_type: readstat_sys::readstat_type_t =
        unsafe { readstat_sys::readstat_value_type(value) };

    let is_missing: c_int = unsafe { readstat_sys::readstat_value_is_system_missing(value) };

    if var_index == 0 {
        d.row = Vec::with_capacity(var_count as usize);
    }

    if is_missing == 0 {
        let value: ReadStatVar = match value_type {
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

        d.row.push(value);
    }

    if var_index == md.var_count - 1 {
        let row = d.row.clone();
        d.rows.push(row);
        d.row.clear();
    }

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
