#![allow(clippy::float_cmp)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]

use arrow::datatypes::{DataType, TimeUnit};
use common::ExpectedMetadata;

mod common;

#[test]
fn parse_all_types_metadata() {
    let (_rsp, md, d) = common::setup_and_read_skip_row_count("all_types.sas7bdat");

    // skip_row_count=true reports the count honestly as unknown.
    assert_eq!(md.row_count, None);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&md.to_json().unwrap()).unwrap()["row_count"],
        serde_json::Value::Null
    );
    let mut comparable = md.clone();
    comparable.row_count = Some(1);
    common::assert_metadata(
        &comparable,
        &ExpectedMetadata {
            row_count: 1,
            var_count: 10,
            table_name: "",
            file_label: "",
            file_encoding: "UTF-8",
            version: 9,
            is_64bit: true,
            creation_time: "2026-02-18 02:32:45",
            modified_time: "2026-02-18 02:32:45",
        },
    );

    assert!(matches!(md.compression, readstat::ReadStatCompress::None));
    assert!(matches!(md.endianness, readstat::ReadStatEndian::Little));

    assert!(common::contains_var(&d, 0));
    assert!(!common::contains_var(&d, 100));

    // Verify key variable types
    let (_, _, vfc, vf, adt) = common::get_var_attrs(&d, 0);
    assert!(vfc.is_none());
    assert_eq!(vf, "BEST12");
    assert!(matches!(adt, DataType::Float64));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 4);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Date));
    assert!(matches!(adt, DataType::Date32));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 5);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::DateTime));
    assert!(matches!(adt, DataType::Timestamp(TimeUnit::Second, None)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 6);
    assert_eq!(
        vfc,
        Some(readstat::ReadStatVarFormatClass::DateTimeWithMilliseconds)
    );
    assert!(matches!(
        adt,
        DataType::Timestamp(TimeUnit::Millisecond, None)
    ));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 7);
    assert_eq!(
        vfc,
        Some(readstat::ReadStatVarFormatClass::DateTimeWithMicroseconds)
    );
    assert!(matches!(
        adt,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 8);
    assert_eq!(vfc, Some(readstat::ReadStatVarFormatClass::Time));
    assert!(matches!(adt, DataType::Time32(TimeUnit::Second)));

    let (_, _, vfc, _, adt) = common::get_var_attrs(&d, 9);
    assert_eq!(
        vfc,
        Some(readstat::ReadStatVarFormatClass::TimeWithMicroseconds)
    );
    assert!(matches!(adt, DataType::Time64(TimeUnit::Microsecond)));
}
