use arrow::datatypes::DataType;
use arrow_array::Array;
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_cars_metadata() {
    let (_rsp, md, d) = common::setup_and_read("cars.sas7bdat");

    common::assert_metadata(&md, &ExpectedMetadata {
        row_count: 1081,
        var_count: 13,
        table_name: "CARS",
        file_label: "Written by SAS",
        file_encoding: "WINDOWS-1252",
        version: 9,
        is64bit: 0,
        creation_time: "2008-09-30 12:55:01",
        modified_time: "2008-09-30 12:55:01",
    });

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables - bounds check
    assert!(common::contains_var(&d, 0));
    assert!(!common::contains_var(&d, 100));

    // 0 - Brand (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "");
    assert!(matches!(adt, DataType::Utf8));
    let brand_meta = common::get_metadata(&d, 0);
    assert!(brand_meta.storage_width > 0, "Brand storage_width should be > 0");

    // 1 - Model (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, "");
    assert!(matches!(adt, DataType::Utf8));
    let model_meta = common::get_metadata(&d, 1);
    assert!(model_meta.storage_width > 0, "Model storage_width should be > 0");

    // 2..12 - All numeric Double -> Float64 with no format
    for i in 2..=12 {
        let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, i);
        assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric), "var {i} type class");
        assert!(matches!(vt, readstat::ReadStatVarType::Double), "var {i} type");
        assert!(vfc.is_none(), "var {i} format class");
        assert_eq!(vf, "", "var {i} format");
        assert!(matches!(adt, DataType::Float64), "var {i} arrow type");
        let num_meta = common::get_metadata(&d, i);
        assert_eq!(num_meta.storage_width, 8, "var {i} storage_width should be 8 for numeric");
    }
}

#[test]
fn parse_cars_data() {
    let (_rsp, _md, d) = common::setup_and_read("cars.sas7bdat");

    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1081);

    // Row 0
    let brand = common::get_string_col(batch, 0);
    let model = common::get_string_col(batch, 1);
    let minivan = common::get_f64_col(batch, 2);
    let engine_size = common::get_f64_col(batch, 6);
    let cylinders = common::get_f64_col(batch, 7);
    let city_mpg = common::get_f64_col(batch, 8);
    let hwy_mpg = common::get_f64_col(batch, 9);
    let hybrid = common::get_f64_col(batch, 12);

    assert_eq!(brand.value(0), "TOYOTA");
    assert_eq!(model.value(0), "Prius");
    assert_eq!(minivan.value(0), 0.0);
    assert_eq!(engine_size.value(0), 1.5);
    assert_eq!(cylinders.value(0), 4.0);
    assert_eq!(city_mpg.value(0), 60.0);
    assert_eq!(hwy_mpg.value(0), 51.0);
    assert_eq!(hybrid.value(0), 1.0);

    // Row 1
    assert_eq!(brand.value(1), "HONDA");
    assert_eq!(model.value(1), "Civic Hybrid");

    // Row 574 — CADILLAC DeVille, EngineSize should be exactly 4.6
    // This catches IEEE 754 representation noise (4.6000000000000005)
    assert_eq!(brand.value(574), "CADILLAC");
    assert_eq!(model.value(574), "DeVille");
    assert_eq!(engine_size.value(574), 4.6);

    // Scan all EngineSize values for IEEE 754 noise in the trailing digits.
    // Values like 1.9, 2.3, 2.8, 3.3, 3.8, 4.6, 5.6, 6.1 are not exactly
    // representable in binary64 and would show as e.g. 4.6000000000000005
    // without the precision-rounding step.
    for i in 0..batch.num_rows() {
        if engine_size.is_null(i) {
            continue;
        }
        let v = engine_size.value(i);
        let displayed = format!("{}", v);
        assert!(
            !displayed.contains("0000000"),
            "row {i}: EngineSize {v} has IEEE 754 noise ({displayed})"
        );
    }
}
