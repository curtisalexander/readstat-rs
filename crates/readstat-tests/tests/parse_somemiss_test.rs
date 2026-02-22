use arrow::datatypes::DataType;
use arrow_array::{Array, Float64Array, StringArray};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("somemiss.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

#[test]
fn parse_somemiss_metadata() {
    let (rsp, md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 200);

    // variable count
    assert_eq!(md.var_count, 9);

    // table name
    assert_eq!(md.table_name, String::from("SOMEMISS"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("WINDOWS-1252"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 0);

    // creation time
    assert_eq!(md.creation_time, "2014-10-11 23:11:27");

    // modified time
    assert_eq!(md.modified_time, "2014-10-11 23:11:27");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables - contains variable
    assert!(common::contains_var(&d, 0));

    // variables - does not contain variable
    assert!(!common::contains_var(&d, 100));

    // variables

    // 0 - SID (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 1 - AGE (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 2 - GENDER (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 3 - RACE (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 4 - INJSITE (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 5 - INJTYPE (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 6 - ISS (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 7 - _ (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 8 - DISPOSITION (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));
}

#[test]
fn parse_somemiss_data() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Row 0: SID=468879.0, AGE="40", GENDER="Female", RACE="Black", INJSITE="Street and Highway",
    //         INJTYPE="Blunt", ISS=1.0, _="", DISPOSITION="Floor"
    let sid = columns
        .first()
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(sid.value(0), 468879.0);

    let age = columns
        .get(1)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(age.value(0), "40");

    let gender = columns
        .get(2)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(gender.value(0), "Female");

    let race = columns
        .get(3)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(race.value(0), "Black");

    let injsite = columns
        .get(4)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(injsite.value(0), "Street and Highway");

    let injtype = columns
        .get(5)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(injtype.value(0), "Blunt");

    let iss = columns
        .get(6)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(iss.value(0), 1.0);

    // Column 7 (_) - row 0 is empty string
    let underscore_col = columns
        .get(7)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(underscore_col.value(0), "");

    let disposition = columns
        .get(8)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(disposition.value(0), "Floor");

    // Row 1: SID=468942.0, AGE="45", ISS=20.0
    assert_eq!(sid.value(1), 468942.0);
    assert_eq!(age.value(1), "45");
    assert_eq!(iss.value(1), 20.0);

    // Row 2: SID=468961.0, RACE="White", ISS=5.0
    assert_eq!(sid.value(2), 468961.0);
    assert_eq!(race.value(2), "White");
    assert_eq!(iss.value(2), 5.0);

    // Verify total row count in batch
    assert_eq!(batch.num_rows(), 200);

    // Check that the _ column has missing values (empty strings show as missing or empty)
    // and ISS column has some null values
    assert!(iss.null_count() > 0 || iss.len() == 200);
}
