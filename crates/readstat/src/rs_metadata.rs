//! File-level and variable-level metadata extracted from `.sas7bdat` files.
//!
//! [`ReadStatMetadata`] holds file-level properties (row/variable counts, encoding,
//! compression, timestamps) and per-variable metadata ([`ReadStatVarMetadata`]) including
//! names, types, labels, and SAS format strings. After parsing, it builds an Arrow
//! [`Schema`](arrow::datatypes::Schema) that maps SAS types to Arrow data types.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use colored::Colorize;
use log::debug;
use num_derive::FromPrimitive;
use serde::Serialize;
use std::{collections::{BTreeMap, BTreeSet, HashMap}, ffi::c_void, os::raw::c_int, path::Path};

use crate::cb::{handle_metadata, handle_variable};
use crate::err::{check_c_error, ReadStatError};
use crate::rs_parser::ReadStatParser;
use crate::rs_path::ReadStatPath;
use crate::rs_var::{ReadStatVarFormatClass, ReadStatVarType, ReadStatVarTypeClass};

/// File-level metadata extracted from a `.sas7bdat` file.
///
/// Populated by the `handle_metadata` and `handle_variable` FFI callbacks during parsing.
/// After parsing, call [`read_metadata`](ReadStatMetadata::read_metadata) to populate
/// all fields and build the Arrow [`Schema`].
#[derive(Clone, Debug, Serialize)]
pub struct ReadStatMetadata {
    /// Number of rows (observations) in the dataset.
    pub row_count: c_int,
    /// Number of variables (columns) in the dataset.
    pub var_count: c_int,
    /// Internal table name from the SAS file header.
    pub table_name: String,
    /// User-assigned file label.
    pub file_label: String,
    /// Character encoding of the file (e.g. `"UTF-8"`, `"WINDOWS-1252"`).
    pub file_encoding: String,
    /// SAS file format version number.
    pub version: c_int,
    /// Whether the file uses 64-bit format (0 = 32-bit, 1 = 64-bit).
    pub is64bit: c_int,
    /// File creation timestamp (formatted as `YYYY-MM-DD HH:MM:SS`).
    pub creation_time: String,
    /// File modification timestamp (formatted as `YYYY-MM-DD HH:MM:SS`).
    pub modified_time: String,
    /// Compression method used in the file.
    pub compression: ReadStatCompress,
    /// Byte order (endianness) of the file.
    pub endianness: ReadStatEndian,
    /// Per-variable metadata, keyed by variable index.
    pub vars: BTreeMap<i32, ReadStatVarMetadata>,
    /// Arrow schema derived from variable types. Not serialized.
    #[serde(skip_serializing)]
    pub schema: Schema,
}

impl Default for ReadStatMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadStatMetadata {
    /// Creates a new `ReadStatMetadata` with default (empty) values.
    pub fn new() -> Self {
        Self {
            row_count: 0,
            var_count: 0,
            table_name: String::new(),
            file_label: String::new(),
            file_encoding: String::new(),
            version: 0,
            is64bit: 0,
            creation_time: String::new(),
            modified_time: String::new(),
            compression: ReadStatCompress::None,
            endianness: ReadStatEndian::None,
            vars: BTreeMap::new(),
            schema: Schema::empty(),
        }
    }

    fn initialize_schema(&self) -> Schema {
        // build up Schema
        let fields: Vec<Field> = self
            .vars
            .values()
            .map(|vm| {
                let var_dt = match &vm.var_type {
                    ReadStatVarType::String
                    | ReadStatVarType::StringRef
                    | ReadStatVarType::Unknown => DataType::Utf8,
                    ReadStatVarType::Int8 | ReadStatVarType::Int16 => DataType::Int16,
                    ReadStatVarType::Int32 => DataType::Int32,
                    ReadStatVarType::Float => DataType::Float32,
                    ReadStatVarType::Double => match &vm.var_format_class {
                        Some(ReadStatVarFormatClass::Date) => DataType::Date32,
                        Some(ReadStatVarFormatClass::DateTime) => {
                            DataType::Timestamp(TimeUnit::Second, None)
                        }
                        Some(ReadStatVarFormatClass::DateTimeWithMilliseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(TimeUnit::Millisecond, None)
                        }
                        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(TimeUnit::Microsecond, None)
                        }
                        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => {
                            // DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None)
                            DataType::Timestamp(TimeUnit::Nanosecond, None)
                        }
                        Some(ReadStatVarFormatClass::Time) => DataType::Time32(TimeUnit::Second),
                        Some(ReadStatVarFormatClass::TimeWithMicroseconds) => {
                            DataType::Time64(TimeUnit::Microsecond)
                        }
                        None => DataType::Float64,
                    },
                };

                // Add column label as field metadata if not empty
                let mut field = Field::new(&vm.var_name, var_dt, true);
                if !vm.var_label.is_empty() {
                    let mut metadata = HashMap::new();
                    metadata.insert("label".to_string(), vm.var_label.clone());
                    field = field.with_metadata(metadata);
                }
                field
            })
            .collect();

        // Add table label as schema metadata if not empty
        if !self.file_label.is_empty() {
            let mut schema_metadata = HashMap::new();
            schema_metadata.insert("table_label".to_string(), self.file_label.clone());
            Schema::new_with_metadata(fields, schema_metadata)
        } else {
            Schema::new(fields)
        }
    }

    /// Parses metadata from the `.sas7bdat` file referenced by `rsp`.
    ///
    /// Sets up the ReadStat C parser with metadata and variable handlers, then
    /// invokes parsing. On success, builds the Arrow [`Schema`] from the
    /// discovered variable types. If `skip_row_count` is `true`, sets a row
    /// limit of 1 to skip counting all rows (faster for metadata-only queries).
    pub fn read_metadata(
        &mut self,
        rsp: &ReadStatPath,
        skip_row_count: bool,
    ) -> Result<(), ReadStatError> {
        debug!("Path as C string is {:?}", &rsp.cstring_path);
        let ppath = rsp.cstring_path.as_ptr();

        // spinner
        /*
        if !self.no_progress {
            self.pb = Some(ProgressBar::new(!0));
        }
        if let Some(pb) = &self.pb {
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("[{spinner:.green} {elapsed_precise}] {msg}"),
            );
            let msg = format!(
                "Parsing sas7bdat metadata from file {}",
                &self.path.to_string_lossy().bright_red()
            );
            pb.set_message(msg);
            pb.enable_steady_tick(120);
        }
        */
        let _msg = format!(
            "Parsing sas7bdat metadata from file {}",
            &rsp.path.to_string_lossy().bright_red()
        );

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let row_limit = if skip_row_count { Some(1) } else { None };

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(handle_metadata))?
            .set_variable_handler(Some(handle_variable))?
            .set_row_limit(row_limit)?
            .parse_sas7bdat(ppath, ctx);

        /*
        if let Some(pb) = &self.pb {
            pb.finish_and_clear();
        }
        */

        check_c_error(error as i32)?;

        // if successful, initialize schema
        self.schema = self.initialize_schema();
        Ok(())
    }

    /// Parses a columns file, returning column names.
    ///
    /// Lines starting with `#` are treated as comments and blank lines are skipped.
    /// Each remaining line is trimmed and used as a column name.
    pub fn parse_columns_file(path: &Path) -> Result<Vec<String>, ReadStatError> {
        let contents = std::fs::read_to_string(path)?;
        let names: Vec<String> = contents
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| line.to_string())
            .collect();
        Ok(names)
    }

    /// Validates column names against the dataset's variables and returns a mapping
    /// of original variable index to new contiguous index.
    ///
    /// Returns `Ok(None)` if `columns` is `None` (no filtering requested).
    /// Returns `Err(ColumnsNotFound)` if any requested names are not in the dataset.
    pub fn resolve_selected_columns(
        &self,
        columns: Option<Vec<String>>,
    ) -> Result<Option<BTreeMap<i32, i32>>, ReadStatError> {
        let columns = match columns {
            Some(c) => c,
            None => return Ok(None),
        };

        // Deduplicate while preserving order isn't needed - we use dataset order
        let requested: BTreeSet<String> = columns.into_iter().collect();

        // Build a name -> index lookup
        let name_to_index: HashMap<&str, i32> = self
            .vars
            .iter()
            .map(|(&idx, vm)| (vm.var_name.as_str(), idx))
            .collect();

        // Check for invalid names
        let not_found: Vec<String> = requested
            .iter()
            .filter(|name| !name_to_index.contains_key(name.as_str()))
            .cloned()
            .collect();

        if !not_found.is_empty() {
            let available: Vec<String> = self
                .vars
                .values()
                .map(|vm| vm.var_name.clone())
                .collect();
            return Err(ReadStatError::ColumnsNotFound {
                requested: not_found,
                available,
            });
        }

        // Build mapping: original_var_index -> new_contiguous_index
        // Iterate in original dataset order (BTreeMap is sorted by key)
        let mut mapping = BTreeMap::new();
        let mut new_index = 0i32;
        for (&orig_index, vm) in &self.vars {
            if requested.contains(&vm.var_name) {
                mapping.insert(orig_index, new_index);
                new_index += 1;
            }
        }

        Ok(Some(mapping))
    }

    /// Returns a new `ReadStatMetadata` with only the selected variables,
    /// re-keyed with contiguous indices starting from 0.
    pub fn filter_to_selected_columns(&self, mapping: &BTreeMap<i32, i32>) -> Self {
        let mut new_vars = BTreeMap::new();
        for (&orig_index, &new_index) in mapping {
            if let Some(vm) = self.vars.get(&orig_index) {
                new_vars.insert(new_index, vm.clone());
            }
        }

        let mut filtered = self.clone();
        filtered.vars = new_vars;
        filtered.var_count = mapping.len() as c_int;
        filtered.schema = filtered.initialize_schema();
        filtered
    }
}

/// Compression method used in a `.sas7bdat` file.
#[derive(Clone, Debug, Default, FromPrimitive, Serialize)]
pub enum ReadStatCompress {
    /// No compression.
    #[default]
    None = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_NONE as isize,
    /// Row-level (RLE) compression.
    Rows = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_ROWS as isize,
    /// Binary (RDC) compression.
    Binary = readstat_sys::readstat_compress_e_READSTAT_COMPRESS_BINARY as isize,
}

/// Byte order (endianness) of a `.sas7bdat` file.
#[derive(Clone, Debug, Default, FromPrimitive, Serialize)]
pub enum ReadStatEndian {
    /// Endianness not specified.
    #[default]
    None = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_NONE as isize,
    /// Little-endian byte order.
    Little = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_LITTLE as isize,
    /// Big-endian byte order.
    Big = readstat_sys::readstat_endian_e_READSTAT_ENDIAN_BIG as isize,
}

/// Metadata for a single variable (column) in a SAS dataset.
#[derive(Clone, Debug, Serialize)]
pub struct ReadStatVarMetadata {
    /// Variable name as defined in the SAS file.
    pub var_name: String,
    /// Storage type of the variable.
    pub var_type: ReadStatVarType,
    /// High-level type class (string or numeric).
    pub var_type_class: ReadStatVarTypeClass,
    /// User-assigned variable label (may be empty).
    pub var_label: String,
    /// SAS format string (e.g. `"DATE9"`, `"BEST12"`).
    pub var_format: String,
    /// Semantic format class derived from the format string, if date/time-related.
    pub var_format_class: Option<ReadStatVarFormatClass>,
}

impl ReadStatVarMetadata {
    /// Creates a new `ReadStatVarMetadata` with the given field values.
    pub fn new(
        var_name: String,
        var_type: ReadStatVarType,
        var_type_class: ReadStatVarTypeClass,
        var_label: String,
        var_format: String,
        var_format_class: Option<ReadStatVarFormatClass>,
    ) -> Self {
        Self {
            var_name,
            var_type,
            var_type_class,
            var_label,
            var_format,
            var_format_class,
        }
    }
}
