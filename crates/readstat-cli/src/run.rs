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

/// Writes a valid empty output file (header-only CSV, empty Parquet/Feather/
/// NDJSON) when the input contributed zero rows. Without this, a zero-row
/// input would produce no output file at all despite a success exit code.
fn write_empty_output(
    var_count: i32,
    vars: Arc<std::collections::BTreeMap<i32, readstat::ReadStatVarMetadata>>,
    schema: Arc<arrow_schema::Schema>,
    wc: &WriteConfig,
    input_path: &std::path::Path,
) -> Result<(), ReadStatError> {
    let mut d = ReadStatData::new().init_shared(var_count, vars, schema.clone(), 0, 0);
    d.batch = Some(arrow_array::RecordBatch::new_empty(schema));
    let mut wtr = ReadStatWriter::new();
    wtr.write(&d, wc)?;
    let rows = wtr.finish(&d, wc)?;
    print_write_summary(rows, input_path, wc.out_path());
    Ok(())
}

/// Prints the "wrote N rows" summary. The library no longer prints this; the
/// CLI owns all user-facing output.
fn print_write_summary(rows: usize, in_path: &std::path::Path, out_path: Option<&std::path::Path>) {
    let in_f = in_path
        .file_name()
        .map_or_else(|| "___".to_string(), |f| f.to_string_lossy().to_string());
    let out_f = out_path
        .and_then(std::path::Path::file_name)
        .map_or_else(|| "___".to_string(), |f| f.to_string_lossy().to_string());
    println!(
        "In total, wrote {} rows from file {in_f} into {out_f}",
        format_with_commas(rows)
    );
}

/// Formats a number with comma thousands separators (e.g. 1081 -> "1,081").
fn format_with_commas(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len <= 3 {
        return s;
    }
    let mut result = String::with_capacity(len + len / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

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
        // Keep the {pos}/{len} row bar (configured in `create_progress`) and
        // just animate its spinner for liveness while a chunk is parsing — the
        // previous implementation swapped in a message-only spinner, so the row
        // bar never appeared. Set the message to the file being parsed.
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
            .template(
                "[{spinner:.green} {elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} rows {msg}",
            )
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
            // An empty columns file is almost certainly a mistake; selecting ALL
            // columns silently would mask it. Surface it as an error instead.
            Err(ReadStatError::EmptyColumnsFile(path))
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
    // Default to showing warnings (e.g. "file will be overwritten") rather than
    // env_logger's stock `error`-only filter, under which library `warn!`s were
    // invisible. `RUST_LOG` still overrides this.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

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
    println!("{}", ReadStatWriter::metadata_to_string(&md, &rsp, as_json)?);
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

    let offsets = build_offsets(total_rows_to_process, total_rows_to_stream);
    let offsets_pairs = offsets.windows(2);

    let var_count = md.var_count;
    let vars_shared = Arc::new(md.vars);
    let schema_shared = Arc::new(md.schema);

    // Signal "parsing started" once (the library no longer does this per-chunk).
    if let Some(ref p) = progress {
        p.parsing_started(&rsp.path.to_string_lossy());
    }

    // Read all chunks into batches
    let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
    for w in offsets_pairs {
        let row_start = w[0];
        let row_end = w[1];

        let mut d = ReadStatData::new()
            .set_column_filter(column_filter.clone(), original_var_count)
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

    // If no output path then only read metadata; otherwise read data
    match wc.out_path() {
        None => {
            // A SQL query with no destination would be silently discarded —
            // surface it as an error rather than quietly falling through to the
            // metadata-only display.
            #[cfg(feature = "sql")]
            if sql_query.is_some() {
                return Err(ReadStatError::Other(
                    "--sql/--sql-file requires --output: the query result needs a destination file"
                        .to_string(),
                ));
            }

            println!(
                "{}: a value was not provided for the parameter {}, thus displaying metadata only\n",
                "Warning".bright_yellow(),
                "--output".bright_cyan()
            );

            // Column selection does not apply to a metadata-only display, so
            // reuse the metadata already read above rather than parsing again.
            println!("{}", ReadStatWriter::metadata_to_string(&md, &rsp, false)?);
            Ok(())
        }
        Some(p) => {
            println!(
                "Writing parsed data to file {}",
                p.to_string_lossy().bright_yellow()
            );

            // Resolve column selection (only meaningful when writing data).
            let col_names = resolve_columns(columns, columns_file)?;
            let column_filter = md.resolve_selected_columns(col_names)?;
            let original_var_count = md.var_count;
            if let Some(ref mapping) = column_filter {
                md = md.filter_to_selected_columns(mapping);
            }
            let column_filter = column_filter.map(Arc::new);

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

            let offsets = build_offsets(total_rows_to_process, total_rows_to_stream);

            let use_parallel_writes =
                parallel && parallel_write && matches!(wc.format(), OutFormat::Parquet);

            let input_path = rsp.path.clone();

            let var_count = md.var_count;
            let vars_shared = Arc::new(md.vars);
            let schema_shared = Arc::new(md.schema);

            // Computed before `rsp` moves into the reader thread below.
            #[cfg(feature = "sql")]
            let sql_table_name = table_name_from_path(&rsp.path);

            let (s, r) = bounded(CHANNEL_CAPACITY);
            let progress_thread = progress.clone();
            let wc_thread = wc.clone();

            // Arc handles for the writer side (the originals move into the
            // reader thread); used to produce a valid empty output file when
            // the input has zero rows.
            let vars_writer = vars_shared.clone();
            let schema_writer = schema_shared.clone();

            // Signal "parsing started" exactly once (the library no longer does
            // this per-chunk). Must happen before `rsp` moves into the reader
            // thread below.
            if let Some(ref p) = progress {
                p.parsing_started(&rsp.path.to_string_lossy());
            }

            // Spawn the reader thread: it parses chunks and sends them down the
            // channel. `rsp` and the shared metadata move into it here, so all
            // uses of `rsp.path` above must already have happened.
            let reader_handle = spawn_reader(
                ReaderConfig {
                    rsp,
                    offsets,
                    parallel,
                    column_filter,
                    original_var_count,
                    total_rows_processed,
                    var_count,
                    vars: vars_shared,
                    schema: schema_shared,
                    progress: progress_thread,
                    wc: wc_thread,
                },
                s,
            );

            // Everything the write strategies share. `ctx` owns the channel
            // receiver and the reader-thread handle, so it moves into whichever
            // strategy runs; each one drains the channel, joins the reader, and
            // finalizes its output.
            let ctx = WriteContext {
                rx: r,
                reader: reader_handle,
                wc,
                input_path,
                var_count,
                vars: vars_writer,
                schema: schema_writer,
            };

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
                    write_with_sql(ctx, query, &sql_table_name)?;
                }
            } else if use_parallel_writes {
                #[cfg(feature = "parquet")]
                {
                    let buffer_size_bytes = (parallel_write_buffer_mb * 1024 * 1024) as usize;
                    write_parallel_parquet(ctx, buffer_size_bytes)?;
                }
                #[cfg(not(feature = "parquet"))]
                {
                    return Err(ReadStatError::Other(
                        "Parallel writes require the parquet feature".to_string(),
                    ));
                }
            } else {
                write_sequential(ctx)?;
            }

            if let Some(p) = progress {
                p.pb.finish_with_message("Done");
            }

            Ok(())
        }
    }
}

/// Inputs to the reader thread spawned by [`run_data`].
///
/// Bundles the parse configuration so [`spawn_reader`] takes a single named
/// value rather than a long positional argument list.
struct ReaderConfig {
    /// Validated input path; moves into the reader thread.
    rsp: ReadStatPath,
    /// Chunk boundaries from [`build_offsets`]; consumed as `windows(2)` pairs.
    offsets: Vec<u32>,
    /// Whether to parse chunks concurrently on the rayon pool.
    parallel: bool,
    /// Optional original-index → filtered-index column mapping.
    column_filter: Option<Arc<std::collections::BTreeMap<i32, i32>>>,
    /// Unfiltered variable count, for row-boundary detection under filtering.
    original_var_count: i32,
    /// Shared counter of rows processed across all chunks.
    total_rows_processed: Arc<std::sync::atomic::AtomicUsize>,
    /// (Possibly filtered) variable count.
    var_count: i32,
    /// Shared variable metadata.
    vars: Arc<std::collections::BTreeMap<i32, readstat::ReadStatVarMetadata>>,
    /// Shared Arrow schema.
    schema: Arc<arrow_schema::Schema>,
    /// Optional progress callback.
    progress: Option<Arc<IndicatifProgress>>,
    /// Output configuration, sent alongside each chunk for the writer.
    wc: WriteConfig,
}

/// Spawns the reader thread that parses row chunks and sends them to `sender`.
///
/// Any chunk error is returned from the thread so it propagates to the exit
/// code — chunks must never be silently dropped, as that would corrupt the
/// output. The returned handle is joined (via [`join_reader`]) by whichever
/// write strategy drains the channel.
fn spawn_reader(
    cfg: ReaderConfig,
    sender: crossbeam::channel::Sender<(ReadStatData, WriteConfig, usize)>,
) -> thread::JoinHandle<Result<(), ReadStatError>> {
    let ReaderConfig {
        rsp,
        offsets,
        parallel,
        column_filter,
        original_var_count,
        total_rows_processed,
        var_count,
        vars,
        schema,
        progress,
        wc,
    } = cfg;

    thread::spawn(move || -> Result<(), ReadStatError> {
        let offsets_pairs: Vec<_> = offsets.windows(2).collect();
        let pairs_cnt = offsets_pairs.len();

        let parse_chunk = |w: &[u32]| -> Result<ReadStatData, ReadStatError> {
            let row_start = w[0];
            let row_end = w[1];

            let mut d = ReadStatData::new()
                .set_column_filter(column_filter.clone(), original_var_count)
                .set_total_rows_processed(total_rows_processed.clone())
                .init_shared(
                    var_count,
                    vars.clone(),
                    schema.clone(),
                    row_start,
                    row_end,
                );

            if let Some(ref p) = progress {
                d = d.set_progress(p.clone() as Arc<dyn ProgressCallback>);
            }

            d.read_data(&rsp)?;

            Ok(d)
        };

        let send_err = || {
            ReadStatError::Other(
                "Error when attempting to send read data for writing".to_string(),
            )
        };

        if parallel {
            // Parse chunks concurrently on the global rayon pool. This buffers
            // all chunks before sending — output order must be preserved for
            // the writer, so --parallel trades memory for parse speed.
            let results: Vec<Result<ReadStatData, ReadStatError>> =
                offsets_pairs.par_iter().map(|w| parse_chunk(w)).collect();

            for result in results {
                let d = result?;
                sender
                    .send((d, wc.clone(), pairs_cnt))
                    .map_err(|_| send_err())?;
            }
        } else {
            // Default streaming mode: parse and send one chunk at a time. The
            // bounded channel provides backpressure, so memory stays at
            // ~CHANNEL_CAPACITY chunks regardless of file size.
            for w in &offsets_pairs {
                let d = parse_chunk(w)?;
                sender
                    .send((d, wc.clone(), pairs_cnt))
                    .map_err(|_| send_err())?;
            }
        }

        Ok(())
    })
}

/// State shared by every write strategy in [`run_data`].
///
/// Owns the channel receiver and the reader-thread handle so it can move into
/// whichever strategy runs. The metadata fields (`var_count`, `vars`, `schema`)
/// and `input_path` are used to emit a valid empty file when the input has zero
/// rows; the SQL path ignores them.
struct WriteContext {
    /// Receiver of parsed chunks from the reader thread.
    rx: crossbeam::channel::Receiver<(ReadStatData, WriteConfig, usize)>,
    /// Handle to the reader thread, joined before output is finalized.
    reader: thread::JoinHandle<Result<(), ReadStatError>>,
    /// Output configuration (path, format, compression).
    wc: WriteConfig,
    /// Input file path, for the write summary.
    input_path: PathBuf,
    /// Variable count, for emitting an empty file on zero rows.
    var_count: i32,
    /// Variable metadata, for emitting an empty file on zero rows.
    vars: Arc<std::collections::BTreeMap<i32, readstat::ReadStatVarMetadata>>,
    /// Arrow schema, for emitting an empty file on zero rows.
    schema: Arc<arrow_schema::Schema>,
}

/// Joins the reader thread, surfacing either its internal error or a panic.
///
/// Must be called after the channel drains and BEFORE finalizing output:
/// writing a Parquet/Feather footer over missing chunks would produce a
/// silently-corrupt file with exit code 0.
fn join_reader(
    handle: thread::JoinHandle<Result<(), ReadStatError>>,
) -> Result<(), ReadStatError> {
    match handle.join() {
        Ok(res) => res,
        Err(_) => Err(ReadStatError::Other("Reader thread panicked".to_string())),
    }
}

/// Default write path: consume chunks in order, streaming each to the format
/// writer, then finalize. Memory stays bounded because only the most recent
/// chunk is retained — kept solely so `finish` can report the row total.
fn write_sequential(ctx: WriteContext) -> Result<(), ReadStatError> {
    let WriteContext {
        rx,
        reader,
        wc,
        input_path,
        var_count,
        vars,
        schema,
    } = ctx;

    let mut wtr = ReadStatWriter::new();

    // Each chunk replaces `last`, dropping the previous chunk's RecordBatch
    // memory; `last` is kept so `finish` can report the row total after the
    // channel drains.
    let mut last: Option<(ReadStatData, WriteConfig)> = None;
    for (d, chunk_wc, _pairs_cnt) in rx.iter() {
        wtr.write(&d, &chunk_wc)?;
        last = Some((d, chunk_wc));
    }

    // Check the reader result before finalizing the output file.
    join_reader(reader)?;

    match last {
        Some((d, chunk_wc)) => {
            let rows = wtr.finish(&d, &chunk_wc)?;
            print_write_summary(rows, &input_path, chunk_wc.out_path());
        }
        None => {
            // Zero rows: still produce a valid header-only/empty file.
            write_empty_output(var_count, vars, schema, &wc, &input_path)?;
        }
    }

    Ok(())
}

/// Parallel Parquet write path (only for `--parallel --parallel-write` with
/// Parquet output): write each buffered batch group to a temp file
/// concurrently, then merge the temp files into the final output.
#[cfg(feature = "parquet")]
fn write_parallel_parquet(ctx: WriteContext, buffer_size_bytes: usize) -> Result<(), ReadStatError> {
    let WriteContext {
        rx,
        reader,
        wc,
        input_path,
        var_count,
        vars,
        schema,
    } = ctx;

    let out_path = wc.out_path().map(std::path::Path::to_path_buf);
    let compression = wc.compression();
    let compression_level = wc.compression_level();

    let temp_dir = if let Some(out_path) = &out_path {
        match out_path.parent() {
            Ok(parent) => parent.to_path_buf(),
            Err(_) => std::env::current_dir()?,
        }
    } else {
        return Err(ReadStatError::Other(
            "No output path specified for parallel write".to_string(),
        ));
    };

    // Stage temp files inside a uniquely-named RAII directory alongside the
    // output. The random suffix prevents two concurrent runs in the same
    // directory from clobbering each other's temp files, and `TempDir`'s Drop
    // removes the directory (and any leftover temp files) even if we bail out
    // early via `?` before the merge.
    let staging = tempfile::Builder::new()
        .prefix(".readstat-parquet-")
        .tempdir_in(&temp_dir)?;

    let mut all_temp_files: Vec<PathBuf> = Vec::new();
    let mut merged_schema: Option<Arc<arrow_schema::Schema>> = None;
    let mut batch_idx: usize = 0;

    loop {
        let mut batch_group: Vec<(ReadStatData, WriteConfig, usize)> =
            Vec::with_capacity(CHANNEL_CAPACITY);
        for item in &rx {
            batch_group.push(item);
            if batch_group.len() >= CHANNEL_CAPACITY {
                break;
            }
        }

        if batch_group.is_empty() {
            break;
        }

        if merged_schema.is_none() {
            merged_schema = Some(batch_group[0].0.schema.clone());
        }
        let schema_ref = merged_schema
            .as_ref()
            .expect("schema must be set after first batch group");

        let temp_files: Vec<PathBuf> = batch_group
            .par_iter()
            .enumerate()
            .map(|(i, (d, _wc, _))| -> Result<PathBuf, ReadStatError> {
                let temp_file = staging
                    .path()
                    .join(format!("part_{}.parquet", batch_idx + i));

                if let Some(batch) = &d.batch {
                    ReadStatWriter::write_batch_to_parquet(
                        batch,
                        schema_ref,
                        &temp_file,
                        compression,
                        compression_level,
                        buffer_size_bytes,
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

    // Check the reader result before producing final output.
    join_reader(reader)?;

    // Merge all temp files into final output
    if all_temp_files.is_empty() {
        // Zero rows: still produce a valid (empty) Parquet file.
        write_empty_output(var_count, vars, schema, &wc, &input_path)?;
    } else if let Some(out_path) = &out_path {
        ReadStatWriter::merge_parquet_files(
            &all_temp_files,
            out_path,
            merged_schema
                .as_ref()
                .expect("schema must be set when temp files exist"),
            compression,
            compression_level,
        )?;
    }

    Ok(())
}

/// SQL write path: collect every batch, run the query through DataFusion, then
/// write the result set to the output file.
#[cfg(feature = "sql")]
fn write_with_sql(ctx: WriteContext, query: &str, table_name: &str) -> Result<(), ReadStatError> {
    let WriteContext {
        rx, reader, wc, schema, ..
    } = ctx;

    let mut all_batches = Vec::new();
    for (d, _wc, _) in rx.iter() {
        if let Some(batch) = d.batch {
            all_batches.push(batch);
        }
    }

    join_reader(reader)?;

    let results = readstat::execute_sql(all_batches, schema, table_name, query)?;
    if let Some(out_path) = wc.out_path() {
        readstat::write_sql_results(
            &results,
            out_path,
            wc.format(),
            wc.compression(),
            wc.compression_level(),
        )?;
    }

    Ok(())
}
