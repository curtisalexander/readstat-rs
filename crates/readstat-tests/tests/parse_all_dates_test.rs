use arrow::datatypes::DataType;
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath, ReadStatVarFormatClass};

mod common;

fn init() -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path("all_dates.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

#[test]
fn all_date_value_columns_have_date_format_class() {
    let (rsp, _md, mut d) = init();
    d.read_data(&rsp).unwrap();

    // Value columns are at odd indices starting from 3 (3, 5, 7, ...)
    // Structure: d_as_str(0), d_as_n(1), fmt1_label(2), fmt1_value(3), fmt2_label(4), fmt2_value(5), ...
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
            Some(ReadStatVarFormatClass::Date),
            "Column {} (format={}) should have Date format class",
            col_name,
            m.var_format
        );

        assert!(
            matches!(
                d.schema.fields[idx as usize].data_type(),
                DataType::Date32
            ),
            "Column {} (format={}) should have Date32 arrow type, got {:?}",
            col_name,
            m.var_format,
            d.schema.fields[idx as usize].data_type()
        );

        checked += 1;
    }

    // 63 date formats
    assert_eq!(checked, 63, "Expected 63 date format columns");
}
