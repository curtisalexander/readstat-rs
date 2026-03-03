//! CLI dispatch logic for the readstat binary.

use colored::Colorize;
use crossbeam::channel::bounded;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use path_abs::{PathAbs, PathInfo};
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use readstat::{
    OutFormat, ProgressCallback, ReadStatData, ReadStatError, ReadStatMetadata, ReadStatPath,
    ReadStatWriter, WriteConfig, build_offsets,
};

use crate::cli::{ReadStatCli, ReadStatCliCommands, Reader};

/// Default number of rows to read per streaming chunk.
const STREAM_ROWS: u32 = 10000;

/// Capacity of the bounded channel between reader and writer threads.
/// Also used as the batch size for bounded-batch parallel writes.
const CHANNEL_CAPACITY: usize = 10;

/// Determine stream row count based on reader type.
fn resolve_stream_rows(reader: Option<Reader>, stream_rows: Option<u32>, total_rows: u32) -> u32 {
    match reader {
        Some(Reader::Stream) | None => stream_rows.unwrap_or(STREAM_ROWS),
        Some(Reader::Mem) => total_rows,
    }
}

/// [`ProgressCallback`] implementation backed by an `indicatif::ProgressBar`.
struct IndicatifProgress {
    pb: ProgressBar,
}

impl ProgressCallback for IndicatifProgress {
    fn inc(&self, n: u64) {
        self.pb.inc(n);
    }

    fn parsing_started(&self, path: &str) {
        if let Ok(style) =
            ProgressStyle::default_spinner().template("[{spinner:.green} {elapsed_precise}] {msg}")
        {
            self.pb.set_style(style);
        }
        self.pb
            .set_message(format!("Parsing sas7bdat data from file {path}"));
        self.pb
            .enable_steady_tick(std::time::Duration::from_millis(120));
    }
}

/// Create a progress bar if progress is enabled.
fn create_progress(
    no_progress: bool,
    total_rows: u32,
) -> Result<Option<Arc<IndicatifProgress>>, ReadStatError> {
    if no_progress {
        return Ok(None);
    }
    let pb = ProgressBar::new(u64::from(total_rows));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} rows {msg}")
            .map_err(|e| ReadStatError::Other(format!("Progress bar template error: {e}")))?
            .progress_chars("##-"),
    );
    Ok(Some(Arc::new(IndicatifProgress { pb })))
}

/// Resolve column names from `--columns` or `--columns-file` CLI options.
fn resolve_columns(
    columns: Option<Vec<String>>,
    columns_file: Option<PathBuf>,
) -> Result<Option<Vec<String>>, ReadStatError> {
    if let Some(path) = columns_file {
        let names = ReadStatMetadata::parse_columns_file(&path)?;
        if names.is_empty() {
            Ok(None)
        } else {
            Ok(Some(names))
        }
    } else {
        Ok(columns)
    }
}

/// Resolve the SQL query from `--sql` or `--sql-file` CLI options.
#[cfg(feature = "sql")]
fn resolve_sql(
    sql: Option<String>,
    sql_file: Option<PathBuf>,
) -> Result<Option<String>, ReadStatError> {
    if let Some(path) = sql_file {
        Ok(Some(readstat::read_sql_file(&path)?))
    } else {
        Ok(sql)
    }
}

/// Extract a table name from the input file stem (e.g. "cars" from "cars.sas7bdat").
#[cfg(feature = "sql")]
fn table_name_from_path(path: &std::path::Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data")
        .to_string()
}

/// Executes the CLI command specified by the parsed [`ReadStatCli`] arguments.
///
/// This is the main entry point for the CLI binary, dispatching to the
/// `metadata`, `preview`, or `data` subcommand.
pub fn run(rs: ReadStatCli) -> Result<(), ReadStatError> {
    env_logger::init();

    match rs.command {
        cmd @ ReadStatCliCommands::Metadata { .. } => run_metadata(cmd),
        cmd @ ReadStatCliCommands::Preview { .. } => run_preview(cmd),
        cmd @ ReadStatCliCommands::Data { .. } => run_data(cmd),
    }
}

/// Handle the `metadata` subcommand: read and display SAS file metadata.
fn run_metadata(cmd: ReadStatCliCommands) -> Result<(), ReadStatError> {
    let ReadStatCliCommands::Metadata {
        input: in_path,
        as_json,
        no_progress: _,
        skip_row_count,
    } = cmd
    else {
        unreachable!()
    };
    let sas_path = PathAbs::new(in_path)?.as_path().to_path_buf();
    debug!(
        "Retrieving metadata from the file {}",
        &sas_path.to_string_lossy()
    );

    let rsp = ReadStatPath::new(sas_path)?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, skip_row_count)?;
    ReadStatWriter::new().write_metadata(&md, &rsp, as_json)?;
    Ok(())
}

/// Handle the `preview` subcommand: read a limited number of rows and write to stdout as CSV.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn run_preview(cmd: ReadStatCliCommands) -> Result<(), ReadStatError> {
    let ReadStatCliCommands::Preview {
        input,
        rows,
        reader,
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

    #[cfg(feature = "sql")]
    let sql_query = resolve_sql(sql, sql_file)?;

    let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
    debug!(
        "Generating data preview from the file {}",
        &sas_path.to_string_lossy()
    );

    let rsp = ReadStatPath::new(sas_path)?;
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;

    // Resolve column selection
    let col_names = resolve_columns(columns, columns_file)?;
    let column_filter = md.resolve_selected_columns(col_names)?;
    let original_var_count = md.var_count;
    if let Some(ref mapping) = column_filter {
        md = md.filter_to_selected_columns(mapping);
    }

    let column_filter = column_filter.map(Arc::new);
    let total_rows_to_process = std::cmp::min(rows, md.row_count as u32);
    let total_rows_to_stream = resolve_stream_rows(reader, stream_rows, total_rows_to_process);
    let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let progress = create_progress(no_progress, total_rows_to_process)?;

    let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;
    let offsets_pairs = offsets.windows(2);

    let var_count = md.var_count;
    let vars_shared = Arc::new(md.vars);
    let schema_shared = Arc::new(md.schema);

    // Read all chunks into batches
    let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
    for w in offsets_pairs {
        let row_start = w[0];
        let row_end = w[1];

        let mut d = ReadStatData::new()
            .set_column_filter(column_filter.clone(), original_var_count)
            .set_no_progress(no_progress)
            .set_total_rows_to_process(total_rows_to_process as usize)
            .set_total_rows_processed(total_rows_processed.clone())
            .init_shared(
                var_count,
                vars_shared.clone(),
                schema_shared.clone(),
                row_start,
                row_end,
            );

        if let Some(ref p) = progress {
            d = d.set_progress(p.clone() as Arc<dyn ProgressCallback>);
        }

        d.read_data(&rsp)?;

        if let Some(batch) = d.batch {
            all_batches.push(batch);
        }
    }

    if let Some(p) = progress {
        p.pb.finish_with_message("Done");
    }

    // Apply SQL query if provided
    #[cfg(feature = "sql")]
    let all_batches = if let Some(ref query) = sql_query {
        let table_name = table_name_from_path(&rsp.path);
        readstat::execute_sql(all_batches, schema_shared.clone(), &table_name, query)?
    } else {
        all_batches
    };

    // Write all batches to stdout as CSV
    #[cfg(feature = "csv")]
    {
        let stdout = std::io::stdout();
        let mut csv_writer = arrow_csv::WriterBuilder::new()
            .with_header(true)
            .build(stdout);
        for batch in &all_batches {
            csv_writer.write(batch)?;
        }
    }
    #[cfg(not(feature = "csv"))]
    {
        let _ = all_batches;
        return Err(ReadStatError::Other(
            "CSV feature is required for preview output".to_string(),
        ));
    }
    #[cfg(feature = "csv")]
    Ok(())
}

/// Handle the `data` subcommand: read SAS data and write to an output file.
#[allow(
    clippy::too_many_lines,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]
fn run_data(cmd: ReadStatCliCommands) -> Result<(), ReadStatError> {
    let ReadStatCliCommands::Data {
        input,
        output,
        format,
        rows,
        reader,
        stream_rows,
        no_progress,
        overwrite,
        parallel,
        parallel_write,
        #[cfg(feature = "parquet")]
        parallel_write_buffer_mb,
        #[cfg(not(feature = "parquet"))]
            parallel_write_buffer_mb: _,
        compression,
        compression_level,
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

    #[cfg(feature = "sql")]
    let sql_query = resolve_sql(sql, sql_file)?;

    let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
    debug!(
        "Generating data from the file {}",
        &sas_path.to_string_lossy()
    );

    let rsp = ReadStatPath::new(sas_path)?;
    let wc = WriteConfig::new(
        output,
        format.map(Into::into),
        overwrite,
        compression.map(Into::into),
        compression_level,
    )?;

    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;

    // Resolve column selection
    let col_names = resolve_columns(columns, columns_file)?;
    let column_filter = md.resolve_selected_columns(col_names)?;
    let original_var_count = md.var_count;
    if let Some(ref mapping) = column_filter {
        md = md.filter_to_selected_columns(mapping);
    }

    let column_filter = column_filter.map(Arc::new);

    // If no output path then only read metadata; otherwise read data
    match &wc.out_path {
        None => {
            println!(
                "{}: a value was not provided for the parameter {}, thus displaying metadata only\n",
                "Warning".bright_yellow(),
                "--output".bright_cyan()
            );

            let mut md = ReadStatMetadata::new();
            md.read_metadata(&rsp, false)?;
            ReadStatWriter::new().write_metadata(&md, &rsp, false)?;
            Ok(())
        }
        Some(p) => {
            println!(
                "Writing parsed data to file {}",
                p.to_string_lossy().bright_yellow()
            );

            // Determine row count
            let total_rows_to_process = if let Some(r) = rows {
                std::cmp::min(r, md.row_count as u32)
            } else {
                md.row_count as u32
            };

            let total_rows_to_stream =
                resolve_stream_rows(reader, stream_rows, total_rows_to_process);
            let total_rows_processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let progress = create_progress(no_progress, total_rows_to_process)?;

            let offsets = build_offsets(total_rows_to_process, total_rows_to_stream)?;

            let use_parallel_writes =
                parallel && parallel_write && matches!(wc.format, OutFormat::Parquet);

            let input_path = rsp.path.clone();

            #[cfg(feature = "parquet")]
            let out_path_clone = wc.out_path.clone();
            #[cfg(feature = "parquet")]
            let compression_clone = wc.compression;
            #[cfg(feature = "parquet")]
            let compression_level_clone = wc.compression_level;
            #[cfg(feature = "parquet")]
            let buffer_size_bytes = parallel_write_buffer_mb * 1024 * 1024;

            let var_count = md.var_count;
            let vars_shared = Arc::new(md.vars);
            let schema_shared = Arc::new(md.schema);

            #[cfg(feature = "sql")]
            let sql_schema = schema_shared.clone();
            #[cfg(feature = "sql")]
            let sql_table_name = table_name_from_path(&rsp.path);
            #[cfg(feature = "sql")]
            let sql_format = wc.format;

            let (s, r) = bounded(CHANNEL_CAPACITY);
            let progress_thread = progress.clone();
            let wc_thread = wc.clone();

            // Process data in batches (i.e. stream chunks of rows)
            let reader_handle = thread::spawn(move || -> Result<(), ReadStatError> {
                let offsets_pairs: Vec<_> = offsets.windows(2).collect();
                let pairs_cnt = offsets_pairs.len();

                let num_threads = usize::from(!parallel);
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(num_threads)
                    .build()
                    .map_err(|e| {
                        ReadStatError::Other(format!("Failed to build thread pool: {e}"))
                    })?;

                let results: Vec<Result<(ReadStatData, WriteConfig, usize), ReadStatError>> = pool
                    .install(|| {
                        offsets_pairs
                            .par_iter()
                            .map(
                                |w| -> Result<(ReadStatData, WriteConfig, usize), ReadStatError> {
                                    let row_start = w[0];
                                    let row_end = w[1];

                                    let mut d = ReadStatData::new()
                                        .set_column_filter(
                                            column_filter.clone(),
                                            original_var_count,
                                        )
                                        .set_no_progress(no_progress)
                                        .set_total_rows_to_process(total_rows_to_process as usize)
                                        .set_total_rows_processed(total_rows_processed.clone())
                                        .init_shared(
                                            var_count,
                                            vars_shared.clone(),
                                            schema_shared.clone(),
                                            row_start,
                                            row_end,
                                        );

                                    if let Some(ref p) = progress_thread {
                                        d = d.set_progress(p.clone() as Arc<dyn ProgressCallback>);
                                    }

                                    d.read_data(&rsp)?;

                                    Ok((d, wc_thread.clone(), pairs_cnt))
                                },
                            )
                            .collect()
                    });

                let mut errors = Vec::new();
                for result in results {
                    match result {
                        Ok(data) => {
                            if s.send(data).is_err() {
                                errors.push(ReadStatError::Other(
                                    "Error when attempting to send read data for writing"
                                        .to_string(),
                                ));
                            }
                        }
                        Err(e) => errors.push(e),
                    }
                }

                drop(s);

                if !errors.is_empty() {
                    eprintln!("The following errors occurred when processing data:");
                    for e in &errors {
                        eprintln!("    Error: {e:#?}");
                    }
                }

                Ok(())
            });

            // Write

            #[cfg(feature = "sql")]
            let has_sql = sql_query.is_some();
            #[cfg(not(feature = "sql"))]
            let has_sql = false;

            if has_sql {
                #[cfg(feature = "sql")]
                {
                    let query = sql_query
                        .as_ref()
                        .expect("sql_query must be set when has_sql is true");
                    if let Some(out_path) = &out_path_clone {
                        let mut all_batches = Vec::new();
                        for (d, _wc, _) in r.iter() {
                            if let Some(batch) = d.batch {
                                all_batches.push(batch);
                            }
                        }
                        let results =
                            readstat::execute_sql(all_batches, sql_schema, &sql_table_name, query)?;
                        readstat::write_sql_results(
                            &results,
                            out_path,
                            sql_format,
                            compression_clone,
                            compression_level_clone,
                        )?;
                    } else {
                        let mut all_batches = Vec::new();
                        for (d, _wc, _) in r.iter() {
                            if let Some(batch) = d.batch {
                                all_batches.push(batch);
                            }
                        }
                        let _results =
                            readstat::execute_sql(all_batches, sql_schema, &sql_table_name, query)?;
                    }
                }
            } else if use_parallel_writes {
                #[cfg(feature = "parquet")]
                {
                    let temp_dir = if let Some(out_path) = &out_path_clone {
                        match out_path.parent() {
                            Ok(parent) => parent.to_path_buf(),
                            Err(_) => std::env::current_dir()?,
                        }
                    } else {
                        return Err(ReadStatError::Other(
                            "No output path specified for parallel write".to_string(),
                        ));
                    };

                    let mut all_temp_files: Vec<PathBuf> = Vec::new();
                    let mut schema: Option<Arc<arrow_schema::Schema>> = None;
                    let mut batch_idx: usize = 0;

                    loop {
                        let mut batch_group: Vec<(ReadStatData, WriteConfig, usize)> =
                            Vec::with_capacity(CHANNEL_CAPACITY);
                        for item in &r {
                            batch_group.push(item);
                            if batch_group.len() >= CHANNEL_CAPACITY {
                                break;
                            }
                        }

                        if batch_group.is_empty() {
                            break;
                        }

                        if schema.is_none() {
                            schema = Some(batch_group[0].0.schema.clone());
                        }
                        let schema_ref = schema
                            .as_ref()
                            .expect("schema must be set after first batch group");

                        let temp_files: Vec<PathBuf> = batch_group
                            .par_iter()
                            .enumerate()
                            .map(|(i, (d, _wc, _))| -> Result<PathBuf, ReadStatError> {
                                let temp_file = temp_dir
                                    .join(format!(".readstat_temp_{}.parquet", batch_idx + i));

                                if let Some(batch) = &d.batch {
                                    ReadStatWriter::write_batch_to_parquet(
                                        batch,
                                        schema_ref,
                                        &temp_file,
                                        compression_clone,
                                        compression_level_clone,
                                        buffer_size_bytes as usize,
                                    )?;
                                }

                                Ok(temp_file)
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        batch_idx += batch_group.len();
                        // batch_group is implicitly dropped here at the end of the loop body,
                        // freeing ReadStatData/RecordBatch memory before the next iteration
                        all_temp_files.extend(temp_files);
                    }

                    // Merge all temp files into final output
                    if !all_temp_files.is_empty()
                        && let Some(out_path) = &out_path_clone
                    {
                        ReadStatWriter::merge_parquet_files(
                            &all_temp_files,
                            out_path,
                            schema
                                .as_ref()
                                .expect("schema must be set when temp files exist"),
                            compression_clone,
                            compression_level_clone,
                        )?;
                    }
                }
                #[cfg(not(feature = "parquet"))]
                {
                    return Err(ReadStatError::Other(
                        "Parallel writes require the parquet feature".to_string(),
                    ));
                }
            } else {
                // Sequential write mode (default) with BufWriter optimizations
                let mut wtr = ReadStatWriter::new();

                // d (ReadStatData) is implicitly dropped at each iteration boundary,
                // preventing accumulation of RecordBatch memory across chunks
                for (i, (d, wc, pairs_cnt)) in r.iter().enumerate() {
                    wtr.write(&d, &wc)?;

                    if i == (pairs_cnt - 1) {
                        wtr.finish(&d, &wc, &input_path)?;
                    }
                }
            }

            if let Some(p) = progress {
                p.pb.finish_with_message("Done");
            }

            match reader_handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(ReadStatError::Other("Reader thread panicked".to_string()));
                }
            }

            Ok(())
        }
    }
}
