#![no_main]
use libfuzzer_sys::fuzz_target;
use readstat::{ReadStatData, ReadStatMetadata};

// Cap rows and columns to avoid OOM from malformed metadata: builders are
// pre-allocated from the file's claimed row_count/storage_width, so the fuzzer
// needs hard ceilings so libFuzzer's default 2 GB RSS limit isn't hit.
const MAX_ROWS: u32 = 100_000;
const MAX_VARS: i32 = 1_000;

fuzz_target!(|data: &[u8]| {
    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(data, false).is_err() {
        return;
    }
    if md.var_count > MAX_VARS {
        return;
    }
    let row_count = (md.row_count as u32).min(MAX_ROWS);
    let mut d = ReadStatData::new().init(md, 0, row_count);
    let _ = d.read_data_from_bytes(data);
});
