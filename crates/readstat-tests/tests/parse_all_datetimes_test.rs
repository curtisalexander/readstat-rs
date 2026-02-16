use arrow::datatypes::{DataType, TimeUnit};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath, ReadStatVarFormatClass};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("all_datetimes.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

#[test]
fn all_datetime_value_columns_have_datetime_format_class() {
    let (rsp, _md, mut d) = init();
    d.read_data(&rsp).unwrap();

    let var_count = d.vars.len() as i32;
    let mut checked = 0;

    for idx in (3..var_count).step_by(2) {
        let m = common::get_metadata(&d, idx);
        let col_name = d.schema.fields[idx as usize].name().clone();

        assert!(
            col_name.ends_with("_value"),
            "Column at index {} should be a _value column, got: {}",
            idx,
            col_name
        );

        assert_eq!(
            m.var_format_class,
            Some(ReadStatVarFormatClass::DateTime),
            "Column {} (format={}) should have DateTime format class",
            col_name,
            m.var_format
        );

        assert!(
            matches!(
                d.schema.fields[idx as usize].data_type(),
                DataType::Timestamp(TimeUnit::Second, None)
            ),
            "Column {} (format={}) should have Timestamp(Second, None) arrow type, got {:?}",
            col_name,
            m.var_format,
            d.schema.fields[idx as usize].data_type()
        );

        checked += 1;
    }

    // 37 datetime formats
    assert_eq!(checked, 37, "Expected 37 datetime format columns");
}
