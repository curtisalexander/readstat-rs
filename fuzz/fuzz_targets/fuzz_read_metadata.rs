#![no_main]
use libfuzzer_sys::fuzz_target;
use readstat::ReadStatMetadata;

fuzz_target!(|data: &[u8]| {
    let mut md = ReadStatMetadata::new();
    let _ = md.read_metadata_from_bytes(data, false);
});
