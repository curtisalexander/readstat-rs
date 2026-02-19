//! Benchmark string allocation in the data reading pipeline.
//!
//! Measures time spent in:
//! 1. Metadata parsing
//! 2. FFI data parsing (handle_value callbacks — string allocation in ptr_to_string)
//! 3. cols_to_batch conversion (clone + StringArray::from)
//!
//! Run with:
//!   cargo test -p readstat-tests --release string_alloc_bench -- --nocapture --ignored

mod common;

use std::time::Instant;

/// Benchmark the full read pipeline on the large AHS 2019 dataset,
/// timing each phase separately.
///
/// Requires the AHS dataset to be downloaded first:
///   ./crates/readstat-tests/util/download_ahs.sh
#[test]
#[ignore] // Large file — run explicitly
fn bench_ahs_string_allocation() {
    let dataset = "_ahs2019n.sas7bdat";

    // Phase 1: Metadata
    let t0 = Instant::now();
    let rsp = common::setup_path(dataset).unwrap();
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let t_metadata = t0.elapsed();

    let string_cols: usize = md.vars.values()
        .filter(|v| matches!(v.var_type_class, readstat::ReadStatVarTypeClass::String))
        .count();
    let numeric_cols: usize = md.vars.values()
        .filter(|v| matches!(v.var_type_class, readstat::ReadStatVarTypeClass::Numeric))
        .count();

    println!("\n=== AHS 2019 String Allocation Benchmark ===");
    println!("Rows: {}, Columns: {} ({} string, {} numeric)",
        md.row_count, md.var_count, string_cols, numeric_cols);
    println!("Total string cells: {}", string_cols as i64 * md.row_count as i64);
    println!();
    println!("Phase 1 — Metadata:       {:>8.2?}", t_metadata);

    // Phase 2+3: Parse then convert, timed separately per chunk
    let stream_rows: u32 = 10_000;
    let offsets = readstat::build_offsets(md.row_count as u32, stream_rows).unwrap();
    let chunks = offsets.windows(2).count();

    let mut total_parse = std::time::Duration::ZERO;
    let mut total_convert = std::time::Duration::ZERO;
    let mut total_rows: usize = 0;

    for (i, window) in offsets.windows(2).enumerate() {
        let (row_start, row_end) = (window[0], window[1]);
        let chunk_rows = (row_end - row_start) as usize;

        let mut d = readstat::ReadStatData::new()
            .set_no_progress(true)
            .init(md.clone(), row_start, row_end);

        // Phase 2: FFI parsing (ptr_to_string allocations happen here)
        let t1 = Instant::now();
        d.parse_data(&rsp).unwrap();
        let t_parse = t1.elapsed();

        // Phase 3: cols_to_batch (clone + StringArray::from)
        let t2 = Instant::now();
        d.cols_to_batch().unwrap();
        let t_convert = t2.elapsed();

        println!("  Chunk {}: {} rows — parse {:>7.2?}, convert {:>7.2?}",
            i, chunk_rows, t_parse, t_convert);

        total_parse += t_parse;
        total_convert += t_convert;
        total_rows += chunk_rows;

        let batch = d.batch.as_ref().unwrap();
        assert_eq!(batch.num_rows(), chunk_rows);
    }

    println!();
    println!("Phase 2 — Parse (FFI):    {:>8.2?}  ({} chunks)", total_parse, chunks);
    println!("Phase 3 — Convert (Arrow):{:>8.2?}  ({} chunks)", total_convert, chunks);
    println!("Total rows processed:     {}", total_rows);

    let total = t_metadata + total_parse + total_convert;
    println!();
    println!("Total wall time:          {:>8.2?}", total);
    println!("  Parse % of total:       {:.1}%", total_parse.as_secs_f64() / total.as_secs_f64() * 100.0);
    println!("  Convert % of total:     {:.1}%", total_convert.as_secs_f64() / total.as_secs_f64() * 100.0);

    // Memory estimates
    let avg_width: f64 = md.vars.values()
        .filter(|v| matches!(v.var_type_class, readstat::ReadStatVarTypeClass::String))
        .map(|v| v.storage_width as f64)
        .sum::<f64>() / string_cols as f64;
    println!();
    println!("Avg string storage_width: {:.1} bytes", avg_width);
    println!("Estimated per-chunk string data: {:.1} MB",
        (string_cols as f64 * stream_rows as f64 * avg_width) / 1_048_576.0);
}

/// Smaller benchmark using cars.sas7bdat for quick iteration.
#[test]
fn bench_cars_string_allocation() {
    let dataset = "cars.sas7bdat";

    let rsp = common::setup_path(dataset).unwrap();
    let mut md = readstat::ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();

    let mut d = readstat::ReadStatData::new()
        .set_no_progress(true)
        .init(md.clone(), 0, md.row_count as u32);

    let t1 = Instant::now();
    d.parse_data(&rsp).unwrap();
    let t_parse = t1.elapsed();

    let t2 = Instant::now();
    d.cols_to_batch().unwrap();
    let t_convert = t2.elapsed();

    let string_cols: usize = md.vars.values()
        .filter(|v| matches!(v.var_type_class, readstat::ReadStatVarTypeClass::String))
        .count();

    println!("\n=== Cars String Allocation Benchmark ===");
    println!("Rows: {}, Columns: {} ({} string)",
        md.row_count, md.var_count, string_cols);
    println!("Parse:   {:>8.2?}", t_parse);
    println!("Convert: {:>8.2?}", t_convert);
    println!("Total:   {:>8.2?}", t_parse + t_convert);

    let batch = d.batch.as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1081);
}
