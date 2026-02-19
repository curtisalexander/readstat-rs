//! File-level and variable-level metadata extracted from `.sas7bdat` files.
//!
//! [`ReadStatMetadata`] holds file-level properties (row/variable counts, encoding,
//! compression, timestamps) and per-variable metadata ([`ReadStatVarMetadata`]) including
//! names, types, labels, and SAS format strings. After parsing, it builds an Arrow
//! [`Schema`](arrow::datatypes::Schema) that maps SAS types to Arrow data types.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use log::debug;
use num_derive::FromPrimitive;
use serde::Serialize;
use std::{collections::{BTreeMap, BTreeSet, HashMap}, ffi::{c_void, CString}, os::raw::c_int, path::Path};

use crate::cb::{handle_metadata, handle_variable};
use crate::err::{check_c_error, ReadStatError};
use crate::rs_buffer_io::ReadStatBufferCtx;
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
                            DataType::Timestamp(TimeUnit::Millisecond, None)
                        }
                        Some(ReadStatVarFormatClass::DateTimeWithMicroseconds) => {
                            DataType::Timestamp(TimeUnit::Microsecond, None)
                        }
                        Some(ReadStatVarFormatClass::DateTimeWithNanoseconds) => {
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

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let row_limit = if skip_row_count { Some(1) } else { None };

        let error = ReadStatParser::new()
            .set_metadata_handler(Some(handle_metadata))?
            .set_variable_handler(Some(handle_variable))?
            .set_row_limit(row_limit)?
            .parse_sas7bdat(ppath, ctx);

        check_c_error(error as i32)?;

        // if successful, initialize schema
        self.schema = self.initialize_schema();
        Ok(())
    }

    /// Parses metadata from an in-memory byte slice containing `.sas7bdat` data.
    ///
    /// Equivalent to [`read_metadata`](ReadStatMetadata::read_metadata) but reads from
    /// a `&[u8]` buffer instead of a file path. Useful for WASM targets, cloud storage,
    /// HTTP uploads, and testing without filesystem access.
    pub fn read_metadata_from_bytes(
        &mut self,
        bytes: &[u8],
        skip_row_count: bool,
    ) -> Result<(), ReadStatError> {
        let mut buffer_ctx = ReadStatBufferCtx::new(bytes);

        let ctx = self as *mut ReadStatMetadata as *mut c_void;

        let error: readstat_sys::readstat_error_t = readstat_sys::readstat_error_e_READSTAT_OK;
        debug!("Initially, error ==> {}", &error);

        let row_limit = if skip_row_count { Some(1) } else { None };

        // Dummy path — custom I/O handlers ignore it
        let dummy_path = CString::new("").unwrap();

        let error = buffer_ctx
            .configure_parser(
                ReadStatParser::new()
                    .set_metadata_handler(Some(handle_metadata))?
                    .set_variable_handler(Some(handle_variable))?
                    .set_row_limit(row_limit)?,
            )?
            .parse_sas7bdat(dummy_path.as_ptr(), ctx);

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Create a test metadata instance with the given variable names.
    fn test_metadata(var_names: &[&str]) -> ReadStatMetadata {
        let mut md = ReadStatMetadata::new();
        for (i, name) in var_names.iter().enumerate() {
            md.vars.insert(
                i as i32,
                ReadStatVarMetadata::new(
                    name.to_string(),
                    ReadStatVarType::Double,
                    ReadStatVarTypeClass::Numeric,
                    String::new(),
                    "BEST12".to_string(),
                    None,
                ),
            );
        }
        md.var_count = var_names.len() as c_int;
        md.schema = md.initialize_schema();
        md
    }

    // --- resolve_selected_columns ---

    #[test]
    fn resolve_columns_none_returns_none() {
        let md = test_metadata(&["a", "b", "c"]);
        assert!(md.resolve_selected_columns(None).unwrap().is_none());
    }

    #[test]
    fn resolve_columns_valid_subset() {
        let md = test_metadata(&["a", "b", "c"]);
        let mapping = md
            .resolve_selected_columns(Some(vec!["a".into(), "c".into()]))
            .unwrap()
            .unwrap();
        assert_eq!(mapping.len(), 2);
        // "a" is at original index 0, mapped to new index 0
        assert_eq!(mapping[&0], 0);
        // "c" is at original index 2, mapped to new index 1
        assert_eq!(mapping[&2], 1);
    }

    #[test]
    fn resolve_columns_invalid_name_errors() {
        let md = test_metadata(&["a", "b", "c"]);
        let err = md
            .resolve_selected_columns(Some(vec!["a".into(), "nonexistent".into()]))
            .unwrap_err();
        match err {
            ReadStatError::ColumnsNotFound { requested, available } => {
                assert_eq!(requested, vec!["nonexistent"]);
                assert_eq!(available, vec!["a", "b", "c"]);
            }
            other => panic!("Expected ColumnsNotFound, got {other:?}"),
        }
    }

    #[test]
    fn resolve_columns_all_columns() {
        let md = test_metadata(&["x", "y", "z"]);
        let mapping = md
            .resolve_selected_columns(Some(vec!["x".into(), "y".into(), "z".into()]))
            .unwrap()
            .unwrap();
        assert_eq!(mapping.len(), 3);
        assert_eq!(mapping[&0], 0);
        assert_eq!(mapping[&1], 1);
        assert_eq!(mapping[&2], 2);
    }

    // --- filter_to_selected_columns ---

    #[test]
    fn filter_produces_contiguous_indices() {
        let md = test_metadata(&["a", "b", "c", "d"]);
        let mapping = md
            .resolve_selected_columns(Some(vec!["b".into(), "d".into()]))
            .unwrap()
            .unwrap();
        let filtered = md.filter_to_selected_columns(&mapping);

        assert_eq!(filtered.var_count, 2);
        assert_eq!(filtered.vars[&0].var_name, "b");
        assert_eq!(filtered.vars[&1].var_name, "d");
    }

    #[test]
    fn filter_preserves_schema() {
        let md = test_metadata(&["a", "b", "c"]);
        let mapping = md
            .resolve_selected_columns(Some(vec!["b".into()]))
            .unwrap()
            .unwrap();
        let filtered = md.filter_to_selected_columns(&mapping);

        assert_eq!(filtered.schema.fields().len(), 1);
        assert_eq!(filtered.schema.fields()[0].name(), "b");
    }

    // --- initialize_schema ---

    #[test]
    fn schema_string_type() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "name".into(),
            ReadStatVarType::String,
            ReadStatVarTypeClass::String,
            String::new(),
            "$30".into(),
            None,
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();
        assert_eq!(*schema.fields()[0].data_type(), DataType::Utf8);
    }

    #[test]
    fn schema_float64_type() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "value".into(),
            ReadStatVarType::Double,
            ReadStatVarTypeClass::Numeric,
            String::new(),
            "BEST12".into(),
            None,
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();
        assert_eq!(*schema.fields()[0].data_type(), DataType::Float64);
    }

    #[test]
    fn schema_date_type() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "dt".into(),
            ReadStatVarType::Double,
            ReadStatVarTypeClass::Numeric,
            String::new(),
            "DATE9".into(),
            Some(ReadStatVarFormatClass::Date),
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();
        assert_eq!(*schema.fields()[0].data_type(), DataType::Date32);
    }

    #[test]
    fn schema_datetime_type() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "ts".into(),
            ReadStatVarType::Double,
            ReadStatVarTypeClass::Numeric,
            String::new(),
            "DATETIME22".into(),
            Some(ReadStatVarFormatClass::DateTime),
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();
        assert_eq!(
            *schema.fields()[0].data_type(),
            DataType::Timestamp(TimeUnit::Second, None)
        );
    }

    #[test]
    fn schema_time_type() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "tm".into(),
            ReadStatVarType::Double,
            ReadStatVarTypeClass::Numeric,
            String::new(),
            "TIME8".into(),
            Some(ReadStatVarFormatClass::Time),
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();
        assert_eq!(
            *schema.fields()[0].data_type(),
            DataType::Time32(TimeUnit::Second)
        );
    }

    #[test]
    fn schema_int32_type() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "count".into(),
            ReadStatVarType::Int32,
            ReadStatVarTypeClass::Numeric,
            String::new(),
            String::new(),
            None,
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();
        assert_eq!(*schema.fields()[0].data_type(), DataType::Int32);
    }

    #[test]
    fn schema_with_labels_metadata() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "col".into(),
            ReadStatVarType::Double,
            ReadStatVarTypeClass::Numeric,
            "My Label".into(),
            "BEST12".into(),
            None,
        ));
        md.var_count = 1;
        md.file_label = "My Table".into();
        let schema = md.initialize_schema();

        // Field metadata
        let field_meta = schema.fields()[0].metadata();
        assert_eq!(field_meta.get("label").unwrap(), "My Label");

        // Schema metadata
        let schema_meta = schema.metadata();
        assert_eq!(schema_meta.get("table_label").unwrap(), "My Table");
    }

    #[test]
    fn schema_no_labels_no_metadata() {
        let mut md = ReadStatMetadata::new();
        md.vars.insert(0, ReadStatVarMetadata::new(
            "col".into(),
            ReadStatVarType::Double,
            ReadStatVarTypeClass::Numeric,
            String::new(),
            "BEST12".into(),
            None,
        ));
        md.var_count = 1;
        let schema = md.initialize_schema();

        assert!(schema.fields()[0].metadata().is_empty());
        assert!(schema.metadata().is_empty());
    }

    // --- parse_columns_file ---

    #[test]
    fn parse_columns_file_normal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cols.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "col_a").unwrap();
        writeln!(f, "col_b").unwrap();
        writeln!(f, "col_c").unwrap();

        let names = ReadStatMetadata::parse_columns_file(&path).unwrap();
        assert_eq!(names, vec!["col_a", "col_b", "col_c"]);
    }

    #[test]
    fn parse_columns_file_with_comments_and_blanks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cols.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "# This is a comment").unwrap();
        writeln!(f, "col_a").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "  col_b  ").unwrap();
        writeln!(f, "# Another comment").unwrap();

        let names = ReadStatMetadata::parse_columns_file(&path).unwrap();
        assert_eq!(names, vec!["col_a", "col_b"]);
    }

    #[test]
    fn parse_columns_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cols.txt");
        std::fs::File::create(&path).unwrap();

        let names = ReadStatMetadata::parse_columns_file(&path).unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn parse_columns_file_nonexistent() {
        let path = Path::new("/nonexistent/path/cols.txt");
        assert!(ReadStatMetadata::parse_columns_file(path).is_err());
    }

    // --- ReadStatMetadata defaults ---

    #[test]
    fn default_metadata() {
        let md = ReadStatMetadata::new();
        assert_eq!(md.row_count, 0);
        assert_eq!(md.var_count, 0);
        assert!(md.table_name.is_empty());
        assert!(md.vars.is_empty());
        assert!(md.schema.fields().is_empty());
    }
}
