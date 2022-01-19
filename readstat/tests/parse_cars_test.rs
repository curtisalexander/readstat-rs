use arrow::datatypes::DataType;

mod common;

fn init() -> readstat::ReadStatData {
    // setup path
    let rsp = common::setup_path("cars.sas7bdat").unwrap();

    // parse sas7bdat
    readstat::ReadStatData::new(rsp)
        .set_reader(Some(readstat::Reader::mem))
        .set_is_test(true)
}

#[test]
fn parse_cars_metadata() {
    let mut d = init();

    let error = d.get_metadata().unwrap();
    assert_eq!(error, readstat::ReadStatError::READSTAT_OK as u32);

    // row count
    assert_eq!(d.row_count, 1081);

    // variable count
    assert_eq!(d.var_count, 13);

    // table name
    assert_eq!(d.table_name, String::from("CARS"));

    // table label
    assert_eq!(d.file_label, String::from("Written by SAS"));

    // file encoding
    assert_eq!(d.file_encoding, String::from("WINDOWS-1252"));

    // format version
    assert_eq!(d.version, 9);

    // bitness
    assert_eq!(d.is64bit, 0);

    // creation time
    assert_eq!(d.creation_time, "2008-09-30 14:55:01");

    // modified time
    assert_eq!(d.modified_time, "2008-09-30 14:55:01");

    // compression
    assert!(matches!(d.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(d.endianness, readstat::ReadStatEndian::Little));

    // variables - contains variable
    assert!(common::contains_var(&d, String::from("Brand"), 0));

    // variables - does not contain variable
    assert!(!common::contains_var(&d, String::from("Brand"), 1));

    // variables

    // 0 - Brand
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Brand"), 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Utf8));

    // 1 - Model
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Model"), 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Utf8));

    // 2 - Minivan
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Minivan"), 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 3 - Wagon
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Wagon"), 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 4 - Pickup
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Pickup"), 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 5 - Automatic
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Automatic"), 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 6 - EngineSize
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("EngineSize"), 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 7 - Cylinders
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Cylinders"), 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 8 - CityMPG
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("CityMPG"), 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 9 - HwyMPG
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("HwyMPG"), 9);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 10 - SUV
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("SUV"), 10);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 11 - AWD
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("AWD"), 11);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 12 - Hybrid
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, String::from("Hybrid"), 12);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));
}
