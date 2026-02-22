use arrow::datatypes::DataType;
use arrow_array::{Float64Array, StringArray};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("messydata.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

#[test]
fn parse_messydata_metadata() {
    let (rsp, md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 80);

    // variable count
    assert_eq!(md.var_count, 15);

    // table name
    assert_eq!(md.table_name, String::from("MESSYDATA"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("WINDOWS-1252"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 0);

    // creation time
    assert_eq!(md.creation_time, "2014-10-11 15:35:04");

    // modified time
    assert_eq!(md.modified_time, "2014-10-11 15:35:04");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    // variables - contains variable
    assert!(common::contains_var(&d, 0));

    // variables - does not contain variable
    assert!(!common::contains_var(&d, 100));

    // variables

    // 0 - Subject (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 1 - DateArrived (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 1);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 2 - TimeArrive (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 2);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 3 - DateLeft (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 3);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 4 - TimeLeft (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 4);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 5 - Married (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 5);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 6 - Single (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 6);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 7 - Age (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 7);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 8 - Gender (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 8);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 9 - Education (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 9);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 10 - Race (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 10);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 11 - How_Arrived (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 11);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 12 - Top_Reason (String)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 12);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::String));
    assert!(matches!(vt, readstat::ReadStatVarType::String));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("$"));
    assert!(matches!(adt, DataType::Utf8));

    // 13 - Arrival (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 13);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));

    // 14 - Satisfaction (Numeric)
    let (vtc, vt, vfc, vf, adt) = common::get_var_attrs(&d, 14);
    assert!(matches!(vtc, readstat::ReadStatVarTypeClass::Numeric));
    assert!(matches!(vt, readstat::ReadStatVarType::Double));
    assert!(vfc.is_none());
    assert_eq!(vf, String::from("BEST"));
    assert!(matches!(adt, DataType::Float64));
}

#[test]
fn parse_messydata_data() {
    let (rsp, _md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    let columns = batch.columns();

    // Row 0: Subject=1.0, DateArrived="2/7/2005", Gender="M", Married=1.0, Arrival=101.5, Satisfaction=84.7
    let subject = columns
        .first()
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(subject.value(0), 1.0);

    let date_arrived = columns
        .get(1)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(date_arrived.value(0), "2/7/2005");

    let gender = columns
        .get(8)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(gender.value(0), "M");

    let married = columns
        .get(5)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(married.value(0), 1.0);

    let arrival = columns
        .get(13)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(arrival.value(0), 101.5);

    let satisfaction = columns
        .get(14)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(satisfaction.value(0), 84.7);

    // Row 2: Subject=3.0, Gender="F", Single=1.0
    assert_eq!(subject.value(2), 3.0);
    assert_eq!(gender.value(2), "F");

    let single = columns
        .get(6)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(single.value(2), 1.0);

    // Verify total row count in batch
    assert_eq!(batch.num_rows(), 80);
}
