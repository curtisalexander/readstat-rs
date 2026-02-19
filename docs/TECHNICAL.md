[< Back to README](../README.md)

# Technical Details

## Floating Point Values
:warning: Decimal values are rounded to contain only 14 decimal digits!

For example, the number `1.1234567890123456` created within SAS would be returned as `1.12345678901235` within Rust.

Why does this happen?  Is this an implementation error?  No, rounding to only 14 decimal digits has been _purposely implemented_ within the Rust code.

As a specific example, when testing with the [cars.sas7bdat](../crates/readstat-tests/tests/data/README.md) dataset (which was created originally on Windows), the numeric value `4.6` as observed within SAS was being returned as `4.600000000000001` (15 digits) within Rust.  Values created on Windows with an x64 processor are only accurate to 15 digits.

For comparison, the [ReadStat binary](https://github.com/WizardMac/ReadStat#command-line-usage) [truncates to 14 decimal places](https://github.com/WizardMac/ReadStat/blob/master/src/bin/write/mod_csv.c#L147) when writing to `csv`.

Finally, SAS represents all numeric values in floating-point representation which creates a challenge for **all** parsed numerics!

### Sources
- [How SAS Stores Numeric Values](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n00dmtao82eizen1e6yziw3s31da)
- [Accuracy on x64 Windows Processors](https://documentation.sas.com/?cdcId=pgmsascdc&cdcVersion=9.4_3.5&docsetId=lrcon&docsetTarget=p0ji1unv6thm0dn1gp4t01a1u0g6.htm&locale=en#n0pd8l179ai8odn17nncb4izqq3d)
    - SAS on Windows with x64 processors can only represent 15 digits
- [Floating-point arithmetic may give inaccurate results in Excel](https://docs.microsoft.com/en-us/office/troubleshoot/excel/floating-point-arithmetic-inaccurate-result)

## Date, Time, and Datetimes
All 118 SAS date, time, and datetime formats are recognized and parsed appropriately.  For the full list of supported formats, see [sas_date_time_formats.md](../crates/readstat-tests/util/sas_date_time_formats.md).

:warning: If the format does not match a recognized SAS date, time, or datetime format, or if the value does not have a format applied, then the value will be parsed and read as a numeric value!

### Details
SAS stores [dates, times, and datetimes](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lrcon/p1wj0wt2ebe2a0n1lv4lem9hdc0v.htm) internally as numeric values.  To distinguish among dates, times, datetimes, or numeric values, a SAS format is read from the variable metadata.  If the format matches a recognized SAS date, time, or datetime format then the numeric value is converted and read into memory using one of the Arrow types:
- [Date32Type](https://docs.rs/arrow/latest/arrow/datatypes/struct.Date32Type.html)
- [Time32SecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.Time32SecondType.html)
- [Time64MicrosecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.Time64MicrosecondType.html) &mdash; for time formats with microsecond precision (e.g. `TIME15.6`, decimal places 4&ndash;6)
- [TimestampSecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampSecondType.html)
- [TimestampMillisecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampMillisecondType.html) &mdash; for datetime formats with millisecond precision (e.g. `DATETIME22.3`, decimal places 1&ndash;3)
- [TimestampMicrosecondType](https://docs.rs/arrow/latest/arrow/datatypes/struct.TimestampMicrosecondType.html) &mdash; for datetime formats with microsecond precision (e.g. `DATETIME22.6`, decimal places 4&ndash;6)

If values are read into memory as Arrow date, time, or datetime types, then when they are written &mdash; from an Arrow [`RecordBatch`](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) to `csv`, `feather`, `ndjson`, or `parquet` &mdash; they are treated as dates, times, or datetimes and not as numeric values.
