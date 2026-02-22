use arrow::datatypes::DataType;
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_somedata_metadata() {
    let (_rsp, md, d) = common::setup_and_read("somedata.sas7bdat");

    common::assert_metadata(
        &md,
        &ExpectedMetadata {
            row_count: 50,
            var_count: 10,
            table_name: "SOMEDATA",
            file_label: "",
            file_encoding: "WINDOWS-1252",
            version: 9,
            is64bit: 0,
            creation_time: "2008-09-30 16:23:56",
            modified_time: "2008-09-30 16:23:56",
        },
    );

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    assert!(common::contains_var(&d, 0));
    assert!(!common::contains_var(&d, 100));

    // 0 - ID (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, "");
    assert!(matches!(adt, DataType::Float64));

    // 1 - GP (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "");
    assert!(matches!(adt, DataType::Utf8));

    // 9 - GENDER (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 9);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "");
    assert!(matches!(adt, DataType::Utf8));

    // 2..8 - All numeric Double -> Float64 with no format
    for i in 2..=8 {
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

#[test]
fn parse_somedata_data() {
    let (_rsp, _md, d) = common::setup_and_read("somedata.sas7bdat");

    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_rows(), 50);

    // Row 0
    let id = common::get_f64_col(batch, 0);
    let gp = common::get_string_col(batch, 1);
    let age = common::get_f64_col(batch, 2);
    let time1 = common::get_f64_col(batch, 3);
    let time2 = common::get_f64_col(batch, 4);
    let status = common::get_f64_col(batch, 7);
    let sex = common::get_f64_col(batch, 8);
    let gender = common::get_string_col(batch, 9);

    assert_eq!(id.value(0), 101.0);
    assert_eq!(gp.value(0), "A");
    assert_eq!(age.value(0), 12.0);
    assert_eq!(time1.value(0), 22.3);
    assert_eq!(time2.value(0), 25.3);
    assert_eq!(status.value(0), 5.0);
    assert_eq!(sex.value(0), 0.0);
    assert_eq!(gender.value(0), "Female");

    // Row 2
    assert_eq!(id.value(2), 110.0);
    assert_eq!(gp.value(2), "A");
    assert_eq!(age.value(2), 12.0);
    assert_eq!(sex.value(2), 1.0);
    assert_eq!(gender.value(2), "Male");
}
