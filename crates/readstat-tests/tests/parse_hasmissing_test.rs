use arrow::datatypes::DataType;
use arrow_array::Array;
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_hasmissing() {
    // Read only first 5 rows
    let (_rsp, _md, d) = common::setup_and_read_rows("hasmissing.sas7bdat", 0, 5);

    assert_eq!(d.var_count, 9);
    assert_eq!(d.chunk_rows_to_process, 5);

    // Variable 0 metadata
    let m = common::get_metadata(&d, 0);
    assert!(matches!(
        m.var_type_class,
        readstat::ReadStatVarTypeClass::String
    ));
    assert!(matches!(m.var_type, readstat::ReadStatVarType::String));
    assert!(m.var_format_class.is_none());
    assert_eq!(m.var_format, "$");
    assert!(matches!(d.schema.fields[0].data_type(), DataType::Utf8));

    let batch = d.batch.as_ref().unwrap();

    // Non-missing string value
    let string_col = common::get_string_col(batch, 0);
    assert_eq!(string_col.value(0), "00101");

    // Non-missing numeric value from column with missing values
    let float_col = common::get_f64_col(batch, 3);
    assert_eq!(float_col.value(1), 33.3);

    // Column 4 has 1 missing value
    let col_with_missing = common::get_f64_col(batch, 4);
    assert_eq!(col_with_missing.null_count(), 1);
    assert!(!col_with_missing.is_null(0), "Row 0 should not be null");
    assert!(col_with_missing.is_null(1), "Row 1 should be null");
}

#[test]
fn parse_hasmissing_metadata() {
    let (_rsp, md, d) = common::setup_and_read("hasmissing.sas7bdat");

    common::assert_metadata(
        &md,
        &ExpectedMetadata {
            row_count: 50,
            var_count: 9,
            table_name: "HASMISSING",
            file_label: "",
            file_encoding: "WINDOWS-1252",
            version: 9,
            is64bit: 0,
            creation_time: "2014-11-18 14:44:33",
            modified_time: "2014-11-18 14:44:33",
        },
    );

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // 0 - ID (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "$");
    assert!(matches!(adt, DataType::Utf8));

    // 1..8 - All numeric Double -> Float64 with no format
    for i in 1..=8 {
        let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, i);
        assert!(
            matches!(vtc, readstat::ReadStatVarTypeClass::Numeric),
            "var {i} type class"
        );
        assert!(
            matches!(vt, readstat::ReadStatVarType::Double),
            "var {i} type"
        );
        assert!(vfc.is_none(), "var {i} format class");
        assert_eq!(vf, "", "var {i} format");
        assert!(matches!(adt, DataType::Float64), "var {i} arrow type");
    }
}
