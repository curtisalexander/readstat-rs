use lazy_static::lazy_static;
use regex::Regex;
use crate::rs::ReadStatFormatClass;

pub fn match_var_format(v: &str) -> Option<ReadStatFormatClass> {
    lazy_static!(
        static ref RE_DATE: Regex = Regex::new(
            r#"(?xi)
            (^DATE[0-9]*$) |
            (^YYMMDD[0-9]*$)
            "#
        ).unwrap();
    );
    lazy_static!(
        static ref RE_DATETIME: Regex = Regex::new(
            r#"(?xi)
            (^DATETIME[0-9]*$) |
            "#
        ).unwrap();
    );
    lazy_static!(
        static ref RE_TIME: Regex = Regex::new(
            r#"(?xi)
            (^TIME[0-9]*$) |
            "#
        ).unwrap();
    );

    if RE_DATE.is_match(v) {
        Some(ReadStatFormatClass::Date)
    }
    else if RE_DATETIME.is_match(v) {
        Some(ReadStatFormatClass::DateTime)
    }
    else if RE_TIME.is_match(v) {
        Some(ReadStatFormatClass::Time)
    } else {
        None
    }
}