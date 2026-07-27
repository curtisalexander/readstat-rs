//! Preview command orchestration.

use std::sync::Arc;

use path_abs::PathAbs;
use readstat::{ProgressCallback, ReadStatError, ReadStatReader};

use crate::cli::{ReadStatCliCommands, Reader};

use super::support::resolve_columns;
#[cfg(feature = "sql")]
use super::support::{resolve_sql, table_name_from_path};

pub(super) fn run(cmd: ReadStatCliCommands) -> Result<(), ReadStatError> {
    let ReadStatCliCommands::Preview {
        input,
        rows,
        reader: reader_mode,
        stream_rows,
        no_progress,
        columns,
        columns_file,
        #[cfg(feature = "sql")]
        sql,
        #[cfg(feature = "sql")]
        sql_file,
    } = cmd
    else {
        unreachable!()
    };

    if matches!(reader_mode, Some(Reader::Mem)) && stream_rows.is_some() {
        return Err(ReadStatError::Other(
            "--stream-rows cannot be used with --reader mem".into(),
        ));
    }

    let path = PathAbs::new(input)?.as_path().to_path_buf();
    let reader = ReadStatReader::from_path(&path)?;
    let available = u32::try_from(
        reader
            .metadata()?
            .row_count
            .ok_or(ReadStatError::RowCountUnavailable)?,
    )?;
    let selected_rows = rows.min(available);
    let mut read = reader.rows(0, Some(selected_rows));
    if let Some(columns) = resolve_columns(columns, columns_file)? {
        read = read.columns(columns);
    }
    if !matches!(reader_mode, Some(Reader::Mem)) {
        read = read.chunk_rows(stream_rows.unwrap_or(10_000));
    }

    let progress = (!no_progress).then(|| Arc::new(PreviewProgress::new(selected_rows)));
    if let Some(progress) = &progress {
        read = read.progress(progress.clone() as Arc<dyn ProgressCallback>);
    }

    let batch = read.read()?;
    if let Some(progress) = progress {
        progress.bar.finish_with_message("Done");
    }

    #[cfg(feature = "sql")]
    let batches = if let Some(query) = resolve_sql(sql, sql_file)? {
        let schema = batch.schema();
        readstat::execute_sql(vec![batch], schema, &table_name_from_path(&path), &query)?
    } else {
        vec![batch]
    };

    #[cfg(not(feature = "sql"))]
    let batches = vec![batch];

    #[cfg(feature = "csv")]
    {
        let stdout = std::io::stdout();
        let mut writer = arrow_csv::WriterBuilder::new()
            .with_header(true)
            .build(stdout);
        for batch in &batches {
            writer.write(batch)?;
        }
    }
    #[cfg(not(feature = "csv"))]
    {
        let _ = batches;
        return Err(ReadStatError::Other(
            "CSV support is required for preview output".into(),
        ));
    }
    #[cfg(feature = "csv")]
    Ok(())
}

struct PreviewProgress {
    bar: indicatif::ProgressBar,
}

impl PreviewProgress {
    fn new(rows: u32) -> Self {
        let bar = indicatif::ProgressBar::new(u64::from(rows));
        bar.set_style(
            indicatif::ProgressStyle::default_bar()
                .template(
                    "[{spinner:.green} {elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} rows {msg}",
                )
                .expect("static progress template is valid")
                .progress_chars("##-"),
        );
        Self { bar }
    }
}

impl ProgressCallback for PreviewProgress {
    fn inc(&self, rows: u64) {
        self.bar.inc(rows);
    }

    fn parsing_started(&self, path: &str) {
        self.bar.set_message(format!("Parsing {path}"));
        self.bar
            .enable_steady_tick(std::time::Duration::from_millis(120));
    }
}
