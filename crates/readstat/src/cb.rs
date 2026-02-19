//! FFI callback functions invoked by the ReadStat C library during parsing.
//!
//! The ReadStat C parser uses a callback-driven architecture: as it reads a `.sas7bdat`
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
    formats,
    rs_data::ReadStatData,
    rs_metadata::{ReadStatCompress, ReadStatEndian, ReadStatMetadata, ReadStatVarMetadata},
    rs_var::{ReadStatVar, ReadStatVarType, ReadStatVarTypeClass},
};

// C types
#[allow(dead_code)]
#[derive(Debug)]
#[repr(C)]
enum ReadStatHandler {
    READSTAT_HANDLER_OK,
    READSTAT_HANDLER_ABORT,
    READSTAT_HANDLER_SKIP_VARIABLE,
}

// C callback functions

/// FFI callback that extracts file-level metadata from the ReadStat C parser.
///
/// Called once during parsing. Populates the [`ReadStatMetadata`] struct
/// (accessed via the `ctx` pointer) with row/variable counts, encoding,
/// timestamps, compression, and endianness.
pub extern "C" fn handle_metadata(
    metadata: *mut readstat_sys::readstat_metadata_t,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    // get metadata
    let rc: c_int = unsafe { readstat_sys::readstat_get_row_count(metadata) };
    let vc: c_int = unsafe { readstat_sys::readstat_get_var_count(metadata) };
    let table_name = unsafe { ptr_to_string(readstat_sys::readstat_get_table_name(metadata)) };
    let file_label = unsafe { ptr_to_string(readstat_sys::readstat_get_file_label(metadata)) };
    let file_encoding =
        unsafe { ptr_to_string(readstat_sys::readstat_get_file_encoding(metadata)) };
    let version: c_int = unsafe { readstat_sys::readstat_get_file_format_version(metadata) };
    let is64bit = unsafe { readstat_sys::readstat_get_file_format_is_64bit(metadata) };
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
    let compression = match FromPrimitive::from_i32(
        unsafe { readstat_sys::readstat_get_compression(metadata) } as i32,
    ) {
        Some(t) => t,
        None => ReadStatCompress::None,
    };

    #[allow(clippy::useless_conversion)]
    let endianness = match FromPrimitive::from_i32(
        unsafe { readstat_sys::readstat_get_endianness(metadata) } as i32,
    ) {
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

/// FFI callback that extracts per-variable metadata from the ReadStat C parser.
///
/// Called once for each variable (column) in the dataset. Populates a
/// [`ReadStatVarMetadata`] entry in the [`ReadStatMetadata::vars`] map
/// with the variable's name, type, label, and SAS format classification.
pub extern "C" fn handle_variable(
    index: c_int,
    variable: *mut readstat_sys::readstat_variable_t,
    #[allow(unused_variables)] val_labels: *const c_char,
    ctx: *mut c_void,
) -> c_int {
    // dereference ctx pointer
    let m = unsafe { &mut *(ctx as *mut ReadStatMetadata) };

    // get variable metadata
    #[allow(clippy::useless_conversion)]
    let var_type = match FromPrimitive::from_i32(
        unsafe { readstat_sys::readstat_variable_get_type(variable) } as i32,
    ) {
        Some(t) => t,
        None => ReadStatVarType::Unknown,
    };

    #[allow(clippy::useless_conversion)]
    let var_type_class = match FromPrimitive::from_i32(
        unsafe { readstat_sys::readstat_variable_get_type_class(variable) } as i32,
    ) {
        Some(t) => t,
        None => ReadStatVarTypeClass::Numeric,
    };

    let var_name = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_name(variable)) };
    let var_label = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_label(variable)) };
    let var_format = unsafe { ptr_to_string(readstat_sys::readstat_variable_get_format(variable)) };
    let var_format_class = formats::match_var_format(&var_format);
    let storage_width =
        unsafe { readstat_sys::readstat_variable_get_storage_width(variable) } as usize;
    let display_width =
        unsafe { readstat_sys::readstat_variable_get_display_width(variable) } as i32;

    debug!("var_type is {:#?}", &var_type);
    debug!("var_type_class is {:#?}", &var_type_class);
    debug!("var_name is {}", &var_name);
    debug!("var_label is {}", &var_label);
    debug!("var_format is {}", &var_format);
    debug!("var_format_class is {:#?}", &var_format_class);
    debug!("storage_width is {}", storage_width);
    debug!("display_width is {}", display_width);

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

/// FFI callback that extracts a single cell value during row parsing.
///
/// Called for every cell in every row. Converts the raw C value to a typed
/// [`ReadStatVar`] and pushes it into the appropriate column vector in
/// [`ReadStatData::cols`]. Tracks row completion for progress reporting.
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

    // Determine the column index for storage, applying column filter if active
    let col_index = if let Some(ref filter) = d.column_filter {
        match filter.get(&var_index) {
            Some(&mapped) => mapped,
            None => {
                // This variable is not selected; skip it but still check row boundary
                if var_index == (d.total_var_count - 1) {
                    d.chunk_rows_processed += 1;
                    if let Some(trp) = &d.total_rows_processed {
                        trp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                return ReadStatHandler::READSTAT_HANDLER_OK as c_int;
            }
        }
    } else {
        var_index
    };

    // get value and push into arrays
    let value = match ReadStatVar::get_readstat_value(value, value_type, is_missing, &d.vars, col_index) {
        Ok(v) => v,
        Err(e) => {
            d.errors.push(format!("{}", e));
            return ReadStatHandler::READSTAT_HANDLER_ABORT as c_int;
        }
    };

    // push into cols
    d.cols[col_index as usize].push(value);

    // if row is complete (use total_var_count for boundary detection)
    if var_index == (d.total_var_count - 1) {
        d.chunk_rows_processed += 1;
        if let Some(trp) = &d.total_rows_processed {
            trp.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    };

    ReadStatHandler::READSTAT_HANDLER_OK as c_int
}
