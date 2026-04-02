#![no_main]
use libfuzzer_sys::fuzz_target;
use readstat::{ReadStatData, ReadStatMetadata};

// Cap rows to avoid OOM from malformed metadata claiming billions of rows.
// The library itself is fine — the CLI streams in 10k-row chunks — but the
// fuzzer needs a hard ceiling so libFuzzer's default 2 GB RSS limit isn't hit.
const MAX_ROWS: u32 = 100_000;

fuzz_target!(|data: &[u8]| {
    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(data, false).is_err() {
        return;
    }
    let row_count = (md.row_count as u32).min(MAX_ROWS);
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md, 0, row_count);
    let _ = d.read_data_from_bytes(data);
});
