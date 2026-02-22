use arrow::datatypes::DataType;
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_malformed_utf8_metadata() {
    let (_rsp, md, d) = common::setup_and_read("malformed_utf8.sas7bdat");

    common::assert_metadata(
        &md,
        &ExpectedMetadata {
            row_count: 2,
            var_count: 3,
            table_name: "",
            file_label: "",
            file_encoding: "UTF-8",
            version: 9,
            is64bit: 1,
            creation_time: "2026-02-20 23:44:31",
            modified_time: "2026-02-20 23:44:31",
        },
    );

    // 0 - trunc_cafe (String -> Utf8, $4)
    let (vtc, vt, vfc, _vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert!(matches!(adt, DataType::Utf8));

    // 1 - trunc_naive (String -> Utf8, $3)
    let (vtc, vt, vfc, _vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert!(matches!(adt, DataType::Utf8));

    // 2 - ok_col (String -> Utf8, $20)
    let (vtc, vt, vfc, _vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert!(matches!(adt, DataType::Utf8));
}

#[test]
fn parse_malformed_utf8_data() {
    let (_rsp, _md, d) = common::setup_and_read("malformed_utf8.sas7bdat");
    let batch = d.batch.as_ref().unwrap();

    // Column 0: trunc_cafe ($4)
    // "café" truncated at byte 4 → "caf" + dangling 0xC3 → lossy replacement
    let col = common::get_string_col(batch, 0);
    assert_eq!(
        col.value(0),
        "caf\u{FFFD}",
        "truncated café should use replacement char"
    );
    assert_eq!(col.value(1), "abc", "pure ASCII should be unchanged");

    // Column 1: trunc_naive ($3)
    // "naïve" truncated at byte 3 → "na" + dangling 0xC3 → lossy replacement
    let col = common::get_string_col(batch, 1);
    assert_eq!(
        col.value(0),
        "na\u{FFFD}",
        "truncated naïve should use replacement char"
    );
    assert_eq!(col.value(1), "xy", "pure ASCII should be unchanged");

    // Column 2: ok_col ($20) — wide enough for valid UTF-8
    let col = common::get_string_col(batch, 2);
    assert_eq!(
        col.value(0),
        "café",
        "full-width column preserves valid UTF-8"
    );
    assert_eq!(col.value(1), "hello");
}
