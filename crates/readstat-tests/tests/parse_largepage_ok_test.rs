use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    // setup path
    let rsp = common::setup_path("rand_ds_largepage_ok.sas7bdat").unwrap();

    // setup metadata
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    // parse sas7bdat
    // read the entire dataset
    let d = readstat::ReadStatData::new().set_no_progress(true).init(
        md.clone(),
        0,
        md.row_count as u32,
    );

    (rsp, md, d)
}

#[test]
fn parse_largepage_ok() {
    let (rsp, md, mut d) = init();

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    // row count
    assert_eq!(md.row_count, 2000);

    // variable count
    assert_eq!(md.var_count, 110);

    // table name
    assert_eq!(md.table_name, String::from("RAND_DS_LARGEPAGE_OK"));

    // table label
    assert_eq!(md.file_label, String::from(""));

    // file encoding
    assert_eq!(md.file_encoding, String::from("UTF-8"));

    // format version
    assert_eq!(md.version, 9);

    // bitness
    assert_eq!(md.is64bit, 1);

    // creation time
    assert_eq!(md.creation_time, "2021-07-25 22:02:30");

    // modified time
    assert_eq!(md.modified_time, "2021-07-25 22:02:30");

    // compression
    assert!(matches!(md.compression, readstat::ReadStatCompress::None));

    // endianness
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));
}
