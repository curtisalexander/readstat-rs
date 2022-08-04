use num_derive::FromPrimitive;
use serde::Serialize;

#[derive(Debug, Clone)]
pub enum ReadStatVar {
    ReadStat_String(String),
    ReadStat_i8(i8),
    ReadStat_i16(i16),
    ReadStat_i32(i32),
    ReadStat_f32(f32),
    ReadStat_f64(f64),
    ReadStat_Missing(()),
    ReadStat_Date(i32),
    ReadStat_DateTime(i64),
    ReadStat_DateTimeWithMilliseconds(i64),
    ReadStat_DateTimeWithMicroseconds(i64),
    ReadStat_DateTimeWithNanoseconds(i64),
    ReadStat_Time(i32),
    // TODO
    // ReadStat_TimeWithMilliseconds(i32),
    // ReadStat_TimeWithMicroseconds(i32),
    // ReadStat_TimeWithNanoseconds(i32),
}

impl ReadStatVar {
    fn get_readstat_value() {
        todo!()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReadStatVarFormatClass {
    Date,
    DateTime,
    DateTimeWithMilliseconds,
    DateTimeWithMicroseconds,
    DateTimeWithNanoseconds,
    Time,
}

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarType {
    String = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING as isize,
    Int8 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT8 as isize,
    Int16 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT16 as isize,
    Int32 = readstat_sys::readstat_type_e_READSTAT_TYPE_INT32 as isize,
    Float = readstat_sys::readstat_type_e_READSTAT_TYPE_FLOAT as isize,
    Double = readstat_sys::readstat_type_e_READSTAT_TYPE_DOUBLE as isize,
    StringRef = readstat_sys::readstat_type_e_READSTAT_TYPE_STRING_REF as isize,
    Unknown,
}

#[derive(Clone, Copy, Debug, FromPrimitive, Serialize)]
pub enum ReadStatVarTypeClass {
    String = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_STRING as isize,
    Numeric = readstat_sys::readstat_type_class_e_READSTAT_TYPE_CLASS_NUMERIC as isize,
}
