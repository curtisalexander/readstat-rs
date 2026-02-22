use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath, ReadStatWriter};
use std::path::PathBuf;
use tempfile::TempDir;

// Helper to get test data path
fn get_test_data_path(filename: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("readstat-tests")
        .join("tests")
        .join("data")
        .join(filename)
}

// Helper to setup ReadStatPath
fn setup_path(filename: &str) -> ReadStatPath {
    let path = get_test_data_path(filename);
    ReadStatPath::new(path, None, None, false, false, None, None).unwrap()
}

// Helper to setup output path
fn setup_output_path(
    temp_dir: &TempDir,
    filename: &str,
    format: readstat::OutFormat,
) -> ReadStatPath {
    let input_path = get_test_data_path(filename);
    let output_path = temp_dir.path().join(format!("output.{:?}", format));
    ReadStatPath::new(
        input_path,
        Some(output_path),
        Some(format),
        true,
        false,
        None,
        None,
    )
    .unwrap()
}

/// Benchmark: Read metadata only (no data)
fn bench_read_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_reading");

    for filename in &["all_types.sas7bdat", "cars.sas7bdat"] {
        group.bench_with_input(
            BenchmarkId::from_parameter(filename),
            filename,
            |b, &filename| {
                let rsp = setup_path(filename);
                b.iter(|| {
                    let mut md = ReadStatMetadata::new();
                    md.read_metadata(black_box(&rsp), false).unwrap();
                    black_box(md)
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Read full dataset (single chunk)
fn bench_read_data_single_chunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_single_chunk");

    for filename in &["all_types.sas7bdat", "cars.sas7bdat"] {
        let rsp = setup_path(filename);
        let mut md = ReadStatMetadata::new();
        md.read_metadata(&rsp, false).unwrap();

        let rows = md.row_count as u32;
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::from_parameter(filename), filename, |b, _| {
            b.iter(|| {
                let mut d = ReadStatData::new()
                    .set_no_progress(true)
                    .init(md.clone(), 0, rows);
                d.read_data(black_box(&rsp)).unwrap();
                black_box(d)
            });
        });
    }
    group.finish();
}

/// Benchmark: Read dataset in chunks (simulating streaming)
fn bench_read_data_chunked(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_chunked");

    let rsp = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let total_rows = md.row_count as u32;

    for chunk_size in &[1000, 5000, 10000] {
        group.throughput(Throughput::Elements(total_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("chunk_size", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                b.iter(|| {
                    let mut total_read = 0;
                    while total_read < total_rows {
                        let end = std::cmp::min(total_read + chunk_size, total_rows);
                        let mut d = ReadStatData::new().set_no_progress(true).init(
                            md.clone(),
                            total_read,
                            end,
                        );
                        d.read_data(black_box(&rsp)).unwrap();
                        black_box(&d);
                        total_read = end;
                    }
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Arrow RecordBatch conversion
fn bench_arrow_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_conversion");

    let rsp = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let rows = md.row_count as u32;

    // Pre-read the data
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, rows);
    d.read_data(&rsp).unwrap();

    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function("to_record_batch", |b| {
        b.iter(|| {
            // The conversion happens in read_data, but we're measuring
            // the overhead of the entire read + conversion pipeline
            let mut d = ReadStatData::new()
                .set_no_progress(true)
                .init(md.clone(), 0, rows);
            d.read_data(black_box(&rsp)).unwrap();
            let has_batch = d.batch.is_some();
            black_box(has_batch)
        });
    });

    group.finish();
}

/// Benchmark: Write CSV
fn bench_write_csv(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_csv");

    let temp_dir = TempDir::new().unwrap();
    let rsp_in = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();
    let rows = md.row_count as u32;

    // Pre-read the data
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, rows);
    d.read_data(&rsp_in).unwrap();

    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function("sequential", |b| {
        b.iter(|| {
            let rsp_out = setup_output_path(&temp_dir, "cars.sas7bdat", readstat::OutFormat::Csv);
            let mut wtr = ReadStatWriter::new();
            wtr.write(black_box(&d), black_box(&rsp_out)).unwrap();
            wtr.finish(black_box(&d), black_box(&rsp_out)).unwrap();
        });
    });

    group.finish();
}

/// Benchmark: Write Parquet with different compression
fn bench_write_parquet_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_parquet_compression");

    let temp_dir = TempDir::new().unwrap();
    let rsp_in = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();
    let rows = md.row_count as u32;

    // Pre-read the data
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, rows);
    d.read_data(&rsp_in).unwrap();

    group.throughput(Throughput::Elements(rows as u64));

    for compression in &[
        (
            "uncompressed",
            Some(readstat::ParquetCompression::Uncompressed),
        ),
        ("snappy", Some(readstat::ParquetCompression::Snappy)),
        ("zstd", Some(readstat::ParquetCompression::Zstd)),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(compression.0),
            &compression.1,
            |b, comp| {
                b.iter(|| {
                    let input_path = get_test_data_path("cars.sas7bdat");
                    let output_path = temp_dir
                        .path()
                        .join(format!("output_{}.parquet", compression.0));
                    let rsp_out = ReadStatPath::new(
                        input_path,
                        Some(output_path),
                        Some(readstat::OutFormat::Parquet),
                        true,
                        false,
                        *comp,
                        None,
                    )
                    .unwrap();

                    let mut wtr = ReadStatWriter::new();
                    wtr.write(black_box(&d), black_box(&rsp_out)).unwrap();
                    wtr.finish(black_box(&d), black_box(&rsp_out)).unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Parallel write with SpooledTempFile (different buffer sizes)
fn bench_parallel_write_buffer_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_write_buffer_sizes");

    let temp_dir = TempDir::new().unwrap();
    let rsp_in = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();
    let rows = md.row_count as u32;

    // Pre-read the data
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, rows);
    d.read_data(&rsp_in).unwrap();

    group.throughput(Throughput::Elements(rows as u64));

    for buffer_mb in &[1, 10, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("buffer_mb", buffer_mb),
            buffer_mb,
            |b, &buffer_mb| {
                b.iter(|| {
                    let output_path = temp_dir
                        .path()
                        .join(format!("output_buf_{}.parquet", buffer_mb));
                    let buffer_bytes = buffer_mb * 1024 * 1024;

                    if let Some(batch) = &d.batch {
                        ReadStatWriter::write_batch_to_parquet(
                            black_box(batch),
                            black_box(&d.schema),
                            black_box(&output_path),
                            None,
                            None,
                            buffer_bytes,
                        )
                        .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Write different formats
fn bench_write_formats(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_formats");

    let temp_dir = TempDir::new().unwrap();
    let rsp_in = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();
    let rows = md.row_count as u32;

    // Pre-read the data
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, rows);
    d.read_data(&rsp_in).unwrap();

    group.throughput(Throughput::Elements(rows as u64));

    for format in &[
        ("csv", readstat::OutFormat::Csv),
        ("parquet", readstat::OutFormat::Parquet),
        ("feather", readstat::OutFormat::Feather),
        ("ndjson", readstat::OutFormat::Ndjson),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format.0),
            &format.1,
            |b, fmt| {
                b.iter(|| {
                    let rsp_out = setup_output_path(&temp_dir, "cars.sas7bdat", *fmt);
                    let mut wtr = ReadStatWriter::new();
                    wtr.write(black_box(&d), black_box(&rsp_out)).unwrap();
                    wtr.finish(black_box(&d), black_box(&rsp_out)).unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: End-to-end conversion (read + write)
fn bench_end_to_end_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("end_to_end_conversion");

    let temp_dir = TempDir::new().unwrap();
    let rsp_in = setup_path("cars.sas7bdat");
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp_in, false).unwrap();
    let rows = md.row_count as u32;

    group.throughput(Throughput::Elements(rows as u64));

    for format in &[
        ("csv", readstat::OutFormat::Csv),
        ("parquet", readstat::OutFormat::Parquet),
    ] {
        group.bench_with_input(BenchmarkId::new("format", format.0), &format.1, |b, fmt| {
            b.iter(|| {
                // Read
                let mut d = ReadStatData::new()
                    .set_no_progress(true)
                    .init(md.clone(), 0, rows);
                d.read_data(black_box(&rsp_in)).unwrap();

                // Write
                let rsp_out = setup_output_path(&temp_dir, "cars.sas7bdat", *fmt);
                let mut wtr = ReadStatWriter::new();
                wtr.write(black_box(&d), black_box(&rsp_out)).unwrap();
                wtr.finish(black_box(&d), black_box(&rsp_out)).unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark: Compare metadata reading across I/O strategies (file vs mmap vs bytes)
fn bench_io_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_metadata");

    for filename in &["cars.sas7bdat", "rand_ds_largepage_ok.sas7bdat"] {
        let path = get_test_data_path(filename);
        let rsp = setup_path(filename);

        group.bench_with_input(BenchmarkId::new("file", filename), filename, |b, _| {
            b.iter(|| {
                let mut md = ReadStatMetadata::new();
                md.read_metadata(black_box(&rsp), false).unwrap();
                black_box(md)
            });
        });

        group.bench_with_input(BenchmarkId::new("mmap", filename), filename, |b, _| {
            b.iter(|| {
                let mut md = ReadStatMetadata::new();
                md.read_metadata_from_mmap(black_box(&path), false).unwrap();
                black_box(md)
            });
        });

        group.bench_with_input(BenchmarkId::new("bytes", filename), filename, |b, _| {
            // Pre-load file into memory (included in measurement for fairness)
            b.iter(|| {
                let bytes = std::fs::read(&path).unwrap();
                let mut md = ReadStatMetadata::new();
                md.read_metadata_from_bytes(black_box(&bytes), false)
                    .unwrap();
                black_box(md)
            });
        });

        // bytes_preloaded: measure only the parsing, not the file read
        let bytes = std::fs::read(&path).unwrap();
        group.bench_with_input(
            BenchmarkId::new("bytes_preloaded", filename),
            filename,
            |b, _| {
                b.iter(|| {
                    let mut md = ReadStatMetadata::new();
                    md.read_metadata_from_bytes(black_box(&bytes), false)
                        .unwrap();
                    black_box(md)
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Compare single-chunk data reading across I/O strategies
fn bench_io_read_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_read_data");

    for filename in &["cars.sas7bdat", "rand_ds_largepage_ok.sas7bdat"] {
        let path = get_test_data_path(filename);
        let rsp = setup_path(filename);
        let mut md = ReadStatMetadata::new();
        md.read_metadata(&rsp, false).unwrap();
        let rows = md.row_count as u32;
        let file_size = std::fs::metadata(&path).unwrap().len();

        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(BenchmarkId::new("file", filename), filename, |b, _| {
            b.iter(|| {
                let mut d = ReadStatData::new()
                    .set_no_progress(true)
                    .init(md.clone(), 0, rows);
                d.read_data(black_box(&rsp)).unwrap();
                black_box(d)
            });
        });

        group.bench_with_input(BenchmarkId::new("mmap", filename), filename, |b, _| {
            b.iter(|| {
                let mut d = ReadStatData::new()
                    .set_no_progress(true)
                    .init(md.clone(), 0, rows);
                d.read_data_from_mmap(black_box(&path)).unwrap();
                black_box(d)
            });
        });

        let bytes = std::fs::read(&path).unwrap();
        group.bench_with_input(
            BenchmarkId::new("bytes_preloaded", filename),
            filename,
            |b, _| {
                b.iter(|| {
                    let mut d = ReadStatData::new()
                        .set_no_progress(true)
                        .init(md.clone(), 0, rows);
                    d.read_data_from_bytes(black_box(&bytes)).unwrap();
                    black_box(d)
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Compare chunked/streaming reading across I/O strategies
fn bench_io_chunked(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_chunked");

    let filename = "cars.sas7bdat";
    let path = get_test_data_path(filename);
    let rsp = setup_path(filename);
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let total_rows = md.row_count as u32;
    let chunk_size = 500u32;

    group.throughput(Throughput::Elements(total_rows as u64));

    group.bench_function("file", |b| {
        b.iter(|| {
            let mut total_read = 0;
            while total_read < total_rows {
                let end = std::cmp::min(total_read + chunk_size, total_rows);
                let mut d =
                    ReadStatData::new()
                        .set_no_progress(true)
                        .init(md.clone(), total_read, end);
                d.read_data(black_box(&rsp)).unwrap();
                black_box(&d);
                total_read = end;
            }
        });
    });

    group.bench_function("mmap", |b| {
        b.iter(|| {
            let mut total_read = 0;
            while total_read < total_rows {
                let end = std::cmp::min(total_read + chunk_size, total_rows);
                let mut d =
                    ReadStatData::new()
                        .set_no_progress(true)
                        .init(md.clone(), total_read, end);
                d.read_data_from_mmap(black_box(&path)).unwrap();
                black_box(&d);
                total_read = end;
            }
        });
    });

    let bytes = std::fs::read(&path).unwrap();
    group.bench_function("bytes_preloaded", |b| {
        b.iter(|| {
            let mut total_read = 0;
            while total_read < total_rows {
                let end = std::cmp::min(total_read + chunk_size, total_rows);
                let mut d =
                    ReadStatData::new()
                        .set_no_progress(true)
                        .init(md.clone(), total_read, end);
                d.read_data_from_bytes(black_box(&bytes)).unwrap();
                black_box(&d);
                total_read = end;
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_read_metadata,
    bench_read_data_single_chunk,
    bench_read_data_chunked,
    bench_arrow_conversion,
    bench_write_csv,
    bench_write_parquet_compression,
    bench_parallel_write_buffer_sizes,
    bench_write_formats,
    bench_end_to_end_conversion,
    bench_io_metadata,
    bench_io_read_data,
    bench_io_chunked,
);

criterion_main!(benches);
