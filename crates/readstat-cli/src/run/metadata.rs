//! Metadata command output.

use crate::cli::ReadStatCliCommands;
use log::debug;
use path_abs::{PathAbs, PathInfo};
use readstat::{ReadStatError, ReadStatMetadata, ReadStatPath};

fn metadata_to_string(
    md: &ReadStatMetadata,
    rsp: &ReadStatPath,
    as_json: bool,
) -> Result<String, ReadStatError> {
    if as_json {
        return md.to_json();
    }
    use std::fmt::Write;
    let mut out = format!("Metadata for the file {}\n\n", rsp.path.display());
    let _ = writeln!(out, "Row count: {}", md.row_count);
    let _ = writeln!(out, "Variable count: {}", md.var_count);
    let _ = writeln!(out, "Table name: {}", md.table_name);
    let _ = writeln!(out, "Table label: {}", md.file_label);
    let _ = writeln!(out, "File encoding: {}", md.file_encoding);
    let _ = writeln!(out, "Format version: {}", md.version);
    let _ = writeln!(
        out,
        "Bitness: {}",
        if md.is_64bit { "64-bit" } else { "32-bit" }
    );
    let _ = writeln!(out, "Creation time: {}", md.creation_time);
    let _ = writeln!(out, "Modified time: {}", md.modified_time);
    let _ = writeln!(out, "Compression: {:#?}", md.compression);
    let _ = writeln!(out, "Byte order: {:#?}", md.endianness);
    let _ = writeln!(out, "Variable names:");
    for (i, var) in &md.vars {
        let format_class = var
            .var_format_class
            .as_ref()
            .map_or("", |class| match class {
                readstat::ReadStatVarFormatClass::Date => "Date",
                readstat::ReadStatVarFormatClass::DateTime
                | readstat::ReadStatVarFormatClass::DateTimeWithMilliseconds
                | readstat::ReadStatVarFormatClass::DateTimeWithMicroseconds
                | readstat::ReadStatVarFormatClass::DateTimeWithNanoseconds => "DateTime",
                readstat::ReadStatVarFormatClass::Time
                | readstat::ReadStatVarFormatClass::TimeWithMilliseconds
                | readstat::ReadStatVarFormatClass::TimeWithMicroseconds
                | readstat::ReadStatVarFormatClass::TimeWithNanoseconds => "Time",
                _ => "",
            });
        let data_type = md.schema.fields[*i as usize].data_type();
        let _ = writeln!(
            out,
            "{i}: {} {{ type class: {:#?}, type: {:#?}, label: {}, format class: {format_class}, format: {}, arrow data type: {data_type:#?} }}",
            var.var_name, var.var_type_class, var.var_type, var.var_label, var.var_format
        );
    }
    Ok(out)
}

pub(super) fn run(cmd: ReadStatCliCommands) -> Result<(), ReadStatError> {
    let ReadStatCliCommands::Metadata {
        input,
        as_json,
        skip_row_count,
    } = cmd
    else {
        unreachable!()
    };
    let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
    debug!("Retrieving metadata from the file {}", sas_path.display());
    let rsp = ReadStatPath::new(sas_path)?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, skip_row_count)?;
    println!("{}", metadata_to_string(&md, &rsp, as_json)?);
    Ok(())
}
