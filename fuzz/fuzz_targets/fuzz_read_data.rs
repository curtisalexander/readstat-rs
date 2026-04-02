#![no_main]
use libfuzzer_sys::fuzz_target;
use readstat::{ReadStatData, ReadStatMetadata};

fuzz_target!(|data: &[u8]| {
    let mut md = ReadStatMetadata::new();
    if md.read_metadata_from_bytes(data, false).is_err() {
        return;
    }
    let row_count = md.row_count as u32;
    let mut d = ReadStatData::new()
        .set_no_progress(true)
        .init(md, 0, row_count);
    let _ = d.read_data_from_bytes(data);
});
