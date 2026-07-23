//! Small input helpers shared by preview and conversion.

#[cfg(feature = "sql")]
use std::path::Path;
use std::path::PathBuf;

use readstat::ReadStatError;

pub(super) fn resolve_columns(
    columns: Option<Vec<String>>,
    columns_file: Option<PathBuf>,
) -> Result<Option<Vec<String>>, ReadStatError> {
    if let Some(path) = columns_file {
        let contents = std::fs::read_to_string(&path)?;
        let names = contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if names.is_empty() {
            Err(ReadStatError::EmptyColumnsFile(path))
        } else {
            Ok(Some(names))
        }
    } else {
        Ok(columns)
    }
}

#[cfg(feature = "sql")]
pub(super) fn resolve_sql(
    sql: Option<String>,
    sql_file: Option<PathBuf>,
) -> Result<Option<String>, ReadStatError> {
    if let Some(path) = sql_file {
        Ok(Some(readstat::read_sql_file(&path)?))
    } else {
        Ok(sql)
    }
}

#[cfg(feature = "sql")]
pub(super) fn table_name_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("data")
        .to_owned()
}
