use arrow::datatypes::{self, DataType};
use path_abs::PathAbs;
use std::{collections::BTreeMap, env};

fn get_metadata<'a>(
    vars: &'a BTreeMap<readstat::ReadStatVarIndexAndName, readstat::ReadStatVarMetadata>,
    var_index: i32,
    var_name: String,
) -> &'a readstat::ReadStatVarMetadata {
    vars.get(&readstat::ReadStatVarIndexAndName::new(var_index, var_name))
        .unwrap()
}

fn get_var_attrs<'a>(
    //d: &'a readstat::ReadStatData,
    m: &readstat::ReadStatVarMetadata,
    s: &'a datatypes::Schema,
    var_index: usize,
) -> (
    readstat::ReadStatVarTypeClass,
    readstat::ReadStatVarType,
    Option<readstat::ReadStatFormatClass>,
    String,
    &'a DataType,
) {
    (
        m.var_type_class,
        m.var_type,
        m.var_format_class,
        m.var_format.clone(),
        s.field(var_index).data_type(),
    )
}

#[test]
fn parse_cars_metadata() {
    // setup path
    let project_dir = PathAbs::new(env!("CARGO_MANIFEST_DIR")).unwrap();
    let data_dir = project_dir.as_path().join("tests").join("data");
    let sas_path = data_dir.join("cars.sas7bdat");
    let rsp = readstat::ReadStatPath::new(sas_path, None, None).unwrap();

    // parse sas7bdat
    let mut d = readstat::ReadStatData::new(rsp)
        .set_reader(readstat::Reader::mem)
        .set_is_test(true);
    let error = d.get_metadata().unwrap();

    assert_eq!(error, readstat_sys::readstat_error_e_READSTAT_OK as u32);

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

    // variables - contains keys
    let vars = d.vars;

    let contains_key = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        0,
        String::from("Brand"),
    ));
    assert!(contains_key);

    let contains_key_wrong_index = vars.contains_key(&readstat::ReadStatVarIndexAndName::new(
        1,
        String::from("Brand"),
    ));
    assert!(!contains_key_wrong_index);

    // variables

    // 0 - Brand
    let m = get_metadata(&vars, 0, String::from("Brand"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Utf8));

    // 1 - Model
    let m = get_metadata(&vars, 1, String::from("Model"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Utf8));

    // 2 - Minivan
    let m = get_metadata(&vars, 2, String::from("Minivan"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 3 - Wagon
    let m = get_metadata(&vars, 3, String::from("Wagon"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 4 - Pickup
    let m = get_metadata(&vars, 4, String::from("Pickup"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 5 - Automatic
    let m = get_metadata(&vars, 5, String::from("Automatic"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 6 - EngineSize
    let m = get_metadata(&vars, 6, String::from("EngineSize"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 7 - Cylinders
    let m = get_metadata(&vars, 7, String::from("Cylinders"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 8 - CityMPG
    let m = get_metadata(&vars, 8, String::from("CityMPG"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 9 - HwyMPG
    let m = get_metadata(&vars, 9, String::from("HwyMPG"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 9);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 10 - SUV
    let m = get_metadata(&vars, 10, String::from("SUV"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 10);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 11 - AWD
    let m = get_metadata(&vars, 11, String::from("AWD"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 11);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 12 - Hybrid
    let m = get_metadata(&vars, 12, String::from("Hybrid"));
    let (vtc, vt, vfc, vf, adt) = get_var_attrs(&m, &d.schema, 12);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));
}
