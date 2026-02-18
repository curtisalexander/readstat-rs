use arrow::datatypes::DataType;
use arrow_array::{Float64Array, StringArray};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("somedata.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

#[test]
fn parse_somedata_metadata() {
    let (rsp, md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 50);

    // variable count
    assert_eq!(md.var_count, 10);

    // table name
    assert_eq!(md.table_name, String::from("SOMEDATA"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("WINDOWS-1252"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 0);

    // creation time
    assert_eq!(md.creation_time, "2008-09-30 16:23:56");

    // modified time
    assert_eq!(md.modified_time, "2008-09-30 16:23:56");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables - contains variable
    assert!(common::contains_var(&d, 0));

    // variables - does not contain variable
    assert!(!common::contains_var(&d, 100));

    // variables

    // 0 - ID (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 1 - GP (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Utf8));

    // 2 - AGE (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 3 - TIME1 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 4 - TIME2 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 5 - TIME3 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 6 - TIME4 (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 7 - STATUS (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 8 - SEX (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Float64));

    // 9 - GENDER (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 9);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from(""));
    assert!(matches!(adt, DataType::Utf8));
}

#[test]
fn parse_somedata_data() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Row 0: ID=101.0, GP="A", AGE=12.0, TIME1=22.3, TIME2=25.3, TIME3=28.2, TIME4=30.6, STATUS=5.0, SEX=0.0, GENDER="Female"
    let id = columns
        .get(0)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(id.value(0), 101.0);

    let gp = columns
        .get(1)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(gp.value(0), "A");

    let age = columns
        .get(2)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(age.value(0), 12.0);

    let time1 = columns
        .get(3)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(time1.value(0), 22.3);

    let time2 = columns
        .get(4)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(time2.value(0), 25.3);

    let status = columns
        .get(7)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(status.value(0), 5.0);

    let sex = columns
        .get(8)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(sex.value(0), 0.0);

    let gender = columns
        .get(9)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(gender.value(0), "Female");

    // Row 2: ID=110.0, GP="A", AGE=12.0, SEX=1.0, GENDER="Male"
    assert_eq!(id.value(2), 110.0);
    assert_eq!(gp.value(2), "A");
    assert_eq!(age.value(2), 12.0);
    assert_eq!(sex.value(2), 1.0);
    assert_eq!(gender.value(2), "Male");

    // Verify total row count in batch
    assert_eq!(batch.num_rows(), 50);
}
