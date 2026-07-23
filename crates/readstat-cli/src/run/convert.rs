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

use super::support::resolve_columns;
#[cfg(feature = "sql")]
use super::support::{resolve_sql, table_name_from_path};
use crate::cli::{ReadStatCliCommands, Reader};

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
    let _ = (var_count, vars);
    let mut wtr = ReadStatWriter::new(wc.clone(), schema.clone())?;
    wtr.write(&arrow_array::RecordBatch::new_empty(schema))?;
    let rows = wtr.finish()?;
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
    eprintln!(
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

/// Handle conversion: read SAS data and write it in the selected format.
#[allow(clippy::too_many_lines)]
pub(super) fn run(cmd: ReadStatCliCommands) -> Result<(), ReadStatError> {
    let ReadStatCliCommands::Convert {
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

    if matches!(reader, Some(Reader::Mem)) && stream_rows.is_some() {
        return Err(ReadStatError::Other(
            "--stream-rows cannot be used with --reader mem".into(),
        ));
    }
    if matches!(reader, Some(Reader::Mem)) && parallel {
        return Err(ReadStatError::Other(
            "--parallel cannot be used with --reader mem; use --reader stream or omit --reader"
                .into(),
        ));
    }

    let sas_path = PathAbs::new(input)?.as_path().to_path_buf();
    debug!(
        "Generating data from the file {}",
        &sas_path.to_string_lossy()
    );

    let rsp = ReadStatPath::new(sas_path)?;
    let mut wc = match (output, format) {
        (None, None) => WriteConfig::new(OutFormat::Csv),
        (None, Some(format)) => WriteConfig::new(format.into()),
        (Some(path), None) => WriteConfig::from_output(path)?,
        (Some(path), Some(format)) => WriteConfig::new(format.into()).output(path)?,
    }
    .overwrite(overwrite);
    if let Some(compression) = compression {
        wc = wc.compression(compression.into(), compression_level)?;
    }
    if wc.out_path().is_none() && !matches!(wc.format(), OutFormat::Csv) {
        return Err(ReadStatError::InvalidOutputConfig(
            "only CSV may be written to stdout; provide --output for this format".into(),
        ));
    }
    if parallel_write && !matches!(wc.format(), OutFormat::Parquet) {
        return Err(ReadStatError::Other(
            "--parallel-write is only supported for Parquet output".into(),
        ));
    }
    #[cfg(feature = "sql")]
    if parallel_write && sql_query.is_some() {
        return Err(ReadStatError::Other(
            "--parallel-write cannot be combined with --sql or --sql-file".into(),
        ));
    }

    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false)?;

    // CSV is streamed to stdout when no output path is supplied.
    match wc.out_path() {
        None | Some(_) => {
            if let Some(p) = wc.out_path() {
                eprintln!(
                    "Writing parsed data to file {}",
                    p.to_string_lossy().bright_yellow()
                );
            }

            // Resolve column selection (only meaningful when writing data).
            let col_names = resolve_columns(columns, columns_file)?;
            let column_filter = md.resolve_selected_columns(col_names)?;
            let original_var_count = md.var_count;
            if let Some(ref mapping) = column_filter {
                md = md.filter_to_selected_columns(mapping);
            }
            let column_filter = column_filter.map(Arc::new);

            // Determine row count. try_from (not `as`): a corrupt header
            // reporting a negative row count must surface as an error, not
            // wrap to ~4 billion rows.
            let row_count = u32::try_from(md.row_count)?;
            let total_rows_to_process = if let Some(r) = rows {
                std::cmp::min(r, row_count)
            } else {
                row_count
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
                    // clap caps the flag at 10240 MB, so this only fails on a
                    // 32-bit usize — where >4 GiB genuinely can't be buffered.
                    let buffer_size_bytes =
                        usize::try_from(parallel_write_buffer_mb.unwrap_or(100) * 1024 * 1024)?;
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

/// Inputs to the conversion reader thread.
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
                .init_shared(var_count, vars.clone(), schema.clone(), row_start, row_end);

            if let Some(ref p) = progress {
                d = d.set_progress(p.clone() as Arc<dyn ProgressCallback>);
            }

            d.read_data(&rsp)?;

            Ok(d)
        };

        let send_err = || {
            ReadStatError::Other("Error when attempting to send read data for writing".to_string())
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

/// State shared by every conversion write strategy.
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
fn join_reader(handle: thread::JoinHandle<Result<(), ReadStatError>>) -> Result<(), ReadStatError> {
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

    let mut wtr = ReadStatWriter::new(wc.clone(), schema.clone())?;

    // Each chunk replaces `last`, dropping the previous chunk's RecordBatch
    // memory; `last` is kept so `finish` can report the row total after the
    // channel drains.
    let mut wrote_any = false;
    for (d, _chunk_wc, _pairs_cnt) in rx.iter() {
        if let Some(batch) = &d.batch {
            wtr.write(batch)?;
            wrote_any = true;
        }
    }

    // Check the reader result before finalizing the output file.
    join_reader(reader)?;

    match wrote_any {
        true => {
            let rows = wtr.finish()?;
            print_write_summary(rows, &input_path, wc.out_path());
        }
        false => {
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
fn write_parallel_parquet(
    ctx: WriteContext,
    buffer_size_bytes: usize,
) -> Result<(), ReadStatError> {
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
    let compression = wc.compression_codec();
    let compression_level = wc.compression_level();

    let temp_dir = if let Some(out_path) = &out_path {
        // Fully qualified std call: with `path_abs::PathInfo` in scope, plain
        // `out_path.parent()` would resolve to the trait's Result-returning
        // method instead of std's Option-returning one.
        match std::path::Path::parent(out_path) {
            Some(parent) => parent.to_path_buf(),
            None => std::env::current_dir()?,
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
    // Rows actually written across all batch groups, for the final summary —
    // mirrors what `finish` reports on the sequential path.
    let mut total_rows: usize = 0;

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
        total_rows += batch_group
            .iter()
            .map(|(d, _, _)| {
                d.batch
                    .as_ref()
                    .map_or(0, arrow_array::RecordBatch::num_rows)
            })
            .sum::<usize>();
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
                        false,
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
            wc.is_overwrite(),
        )?;
        print_write_summary(total_rows, &input_path, Some(out_path));
    }

    Ok(())
}

/// SQL write path: collect every batch, run the query through DataFusion, then
/// write the result set to the output file.
#[cfg(feature = "sql")]
fn write_with_sql(ctx: WriteContext, query: &str, table_name: &str) -> Result<(), ReadStatError> {
    let WriteContext {
        rx,
        reader,
        wc,
        schema,
        ..
    } = ctx;

    let mut all_batches = Vec::new();
    for (d, _wc, _) in rx.iter() {
        if let Some(batch) = d.batch {
            all_batches.push(batch);
        }
    }

    join_reader(reader)?;

    let results = readstat::execute_sql(all_batches, schema.clone(), table_name, query)?;
    let result_schema = results
        .first()
        .map_or_else(|| schema, arrow_array::RecordBatch::schema);
    let mut writer = ReadStatWriter::new(wc, result_schema)?;
    for batch in &results {
        writer.write(batch)?;
    }
    writer.finish()?;

    Ok(())
}
