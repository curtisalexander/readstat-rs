//! Argument-driven Stage 0 benchmark for large external SAS7BDAT files.
//!
//! This intentionally uses only the public, high-level `ReadStatReader` API and
//! discards each emitted batch. See `docs/BENCHMARKING.md` for corpus setup and
//! reproducibility guidance.

use std::{env, error::Error, path::PathBuf, process, time::Instant};

use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath, ReadStatReader, build_offsets};

const DEFAULT_CHUNK_ROWS: u32 = 10_000;

struct Args {
    path: PathBuf,
    chunk_rows: u32,
    mode: Mode,
}

#[derive(Clone, Copy)]
enum Mode {
    OnePass,
    LegacyChunked,
}

fn usage(program: &str) {
    eprintln!(
        "Usage: {program} <FILE.sas7bdat> [--chunk-rows <ROWS>] [--mode one-pass|legacy-chunked]"
    );
}

fn parse_args() -> Result<Args, String> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "large_sas_benchmark".into());
    let mut path = None;
    let mut chunk_rows = DEFAULT_CHUNK_ROWS;
    let mut mode = Mode::OnePass;

    while let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            usage(&program);
            process::exit(0);
        } else if arg == "--chunk-rows" {
            let value = args
                .next()
                .ok_or_else(|| "--chunk-rows requires a value".to_owned())?;
            chunk_rows = parse_chunk_rows(&value)?;
        } else if let Some(value) = arg.strip_prefix("--chunk-rows=") {
            chunk_rows = parse_chunk_rows(value)?;
        } else if arg == "--mode" {
            let value = args
                .next()
                .ok_or_else(|| "--mode requires a value".to_owned())?;
            mode = parse_mode(&value)?;
        } else if let Some(value) = arg.strip_prefix("--mode=") {
            mode = parse_mode(value)?;
        } else if arg.starts_with('-') {
            return Err(format!("unknown option: {arg}"));
        } else if path.replace(PathBuf::from(&arg)).is_some() {
            return Err(format!("unexpected positional argument: {arg}"));
        }
    }

    let path = path.ok_or_else(|| {
        usage(&program);
        "missing SAS7BDAT path".to_owned()
    })?;
    Ok(Args {
        path,
        chunk_rows,
        mode,
    })
}

fn parse_chunk_rows(value: &str) -> Result<u32, String> {
    let rows = value
        .parse::<u32>()
        .map_err(|_| format!("invalid --chunk-rows value: {value}"))?;
    if rows == 0 {
        return Err("--chunk-rows must be greater than zero".to_owned());
    }
    Ok(rows)
}

fn parse_mode(value: &str) -> Result<Mode, String> {
    match value {
        "one-pass" => Ok(Mode::OnePass),
        "legacy-chunked" => Ok(Mode::LegacyChunked),
        _ => Err(format!(
            "invalid --mode value: {value} (expected one-pass or legacy-chunked)"
        )),
    }
}

#[cfg(target_os = "linux")]
fn linux_rss_kib() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    let value = |key: &str| {
        status.lines().find_map(|line| {
            let rest = line.strip_prefix(key)?;
            rest.split_whitespace().next()?.parse::<u64>().ok()
        })
    };
    (value("VmRSS:"), value("VmHWM:"))
}

#[cfg(not(target_os = "linux"))]
fn linux_rss_kib() -> (Option<u64>, Option<u64>) {
    (None, None)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args().map_err(|message| {
        eprintln!("error: {message}");
        process::exit(2);
    });
    let args = args?;
    let source_bytes = std::fs::metadata(&args.path)?.len();
    let started = Instant::now();
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    let parser_invocations = match args.mode {
        Mode::OnePass => {
            ReadStatReader::from_path(&args.path)?
                .chunk_rows(args.chunk_rows)
                .visit(|batch| {
                    rows += u64::try_from(batch.num_rows())?;
                    batches += 1;
                    Ok(())
                })?;
            1 + u64::from(rows > 0)
        }
        Mode::LegacyChunked => {
            let path = ReadStatPath::new(&args.path)?;
            let mut metadata = ReadStatMetadata::new();
            metadata.read_metadata(&path, false)?;
            let total_rows = u32::try_from(
                metadata
                    .row_count
                    .ok_or(readstat::ReadStatError::RowCountUnavailable)?,
            )?;
            for range in build_offsets(total_rows, args.chunk_rows).windows(2) {
                let mut data = ReadStatData::new().init(metadata.clone(), range[0], range[1]);
                data.read_data(&path)?;
                let batch = data.batch.ok_or_else(|| {
                    readstat::ReadStatError::Other("no record batch was produced".into())
                })?;
                rows += u64::try_from(batch.num_rows())?;
                batches += 1;
            }
            1 + batches
        }
    };
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64();
    let rows_per_second = if seconds > 0.0 {
        rows as f64 / seconds
    } else {
        f64::INFINITY
    };
    let mib_per_second = if seconds > 0.0 {
        source_bytes as f64 / (1024.0 * 1024.0) / seconds
    } else {
        f64::INFINITY
    };
    let (current_rss, peak_rss) = linux_rss_kib();

    println!("source: {}", args.path.display());
    println!(
        "mode: {}",
        match args.mode {
            Mode::OnePass => "one-pass",
            Mode::LegacyChunked => "legacy-chunked",
        }
    );
    println!("source bytes: {source_bytes}");
    println!("chunk rows: {}", args.chunk_rows);
    println!("rows: {rows}");
    println!("batches: {batches}");
    println!("expected parser invocations: {parser_invocations}");
    println!("elapsed wall time: {:.6} s", seconds);
    println!("throughput: {:.2} rows/s", rows_per_second);
    println!("throughput: {:.2} MiB/s", mib_per_second);
    match (current_rss, peak_rss) {
        (Some(current), Some(peak)) => {
            println!("current RSS (Linux /proc VmRSS): {current} KiB");
            println!("peak RSS (Linux /proc VmHWM): {peak} KiB");
        }
        _ => println!("RSS: unavailable (reported only on Linux with /proc mounted)"),
    }

    Ok(())
}
