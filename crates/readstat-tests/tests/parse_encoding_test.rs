//! Character-encoding coverage for the iconv conversion path.
//!
//! Every other dataset in the corpus is WINDOWS-1252 or UTF-8 with pure-ASCII
//! string cells, so string conversion is a byte-for-byte copy and the iconv
//! machinery inside ReadStat is never meaningfully exercised. The two
//! datasets used here are derived from messydata.sas7bdat by
//! `util/create_encoding_variants.py` (see that script for the exact byte
//! patches) and pin down behavior that differs by iconv implementation:
//! Linux (glibc), macOS (system libiconv), and Windows (vendored win-iconv,
//! which delegates to Win32 codepages).

use arrow_array::StringArray;
use readstat::{ReadStatData, ReadStatMetadata, ReadStatPath};

mod common;

fn init(ds: &str) -> (ReadStatPath, ReadStatMetadata, ReadStatData) {
    let rsp = common::setup_path(ds).unwrap();
    let mut md = ReadStatMetadata::new();
    md.read_metadata(&rsp, false).unwrap();
    let d = ReadStatData::new().init(md.clone(), 0, md.row_count as u32);
    (rsp, md, d)
}

/// WINDOWS-1251 (Cyrillic): the How_Arrived column contains cells whose raw
/// bytes are only meaningful after real iconv conversion — `C1 F3 F1` must
/// come out as the UTF-8 "Бус". A pass proves iconv resolved the encoding
/// name and converted multi-byte output correctly; this is the regression
/// guard for swapping the Windows iconv implementation.
#[test]
fn parse_encoding_1251() {
    let (rsp, md, mut d) = init("messydata_1251.sas7bdat");

    assert_eq!(md.file_encoding, String::from("WINDOWS-1251"));

    let error = d.read_data(&rsp);
    assert!(error.is_ok());

    let batch = d.batch.unwrap();
    assert_eq!(batch.num_rows(), 80);

    let how_arrived = batch
        .column(11)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Row 0 was "Bus" in the 1252 original; its bytes were re-tagged and
    // re-written as WINDOWS-1251 Cyrillic.
    assert_eq!(how_arrived.value(0), "Бус");
    // Row 1 ("Car") is untouched ASCII — identical in every codepage.
    assert_eq!(how_arrived.value(1), "Car");

    // The script patches 10 raw occurrences, but two are ghost row-images in
    // unreferenced page space that the parser never returns. The visible 80
    // rows contain 7 exact "Bus" cells plus one multi-value "Bus, Walk" cell
    // (Cyrillic and ASCII mixed in a single cell after conversion).
    let bus_count = (0..batch.num_rows())
        .filter(|&i| how_arrived.value(i) == "Бус")
        .count();
    assert_eq!(bus_count, 7);
    let bus_walk_count = (0..batch.num_rows())
        .filter(|&i| how_arrived.value(i) == "Бус, Walk")
        .count();
    assert_eq!(bus_walk_count, 1);
}

/// EUC-TW has no Win32 codepage, so the vendored win-iconv must fail cleanly
/// at `iconv_open` with `READSTAT_ERROR_UNSUPPORTED_CHARSET` — never read the
/// file with garbled strings. GNU/glibc and macOS iconv implement EUC-TW in
/// software, so off-Windows the same file parses fine (its data bytes are
/// pure ASCII, which is valid EUC-TW). The platform split is intentional and
/// this test documents it.
#[test]
fn parse_encoding_euctw_platform_split() {
    let rsp = common::setup_path("messydata_euctw.sas7bdat").unwrap();
    let mut md = ReadStatMetadata::new();
    let md_result = md.read_metadata(&rsp, false);

    #[cfg(windows)]
    {
        use readstat::{ReadStatCError, ReadStatError};

        // win-iconv cannot open an EUC-TW converter: either metadata or data
        // reading must surface UNSUPPORTED_CHARSET, and no string data may be
        // silently mis-decoded.
        let assert_unsupported_charset = |e: &ReadStatError| {
            assert!(
                matches!(
                    e,
                    ReadStatError::CLibrary(ReadStatCError::READSTAT_ERROR_UNSUPPORTED_CHARSET)
                ),
                "expected UNSUPPORTED_CHARSET, got: {e:?}"
            );
        };
        let failed = match md_result {
            Err(e) => {
                assert_unsupported_charset(&e);
                true
            }
            Ok(()) => {
                let mut d = ReadStatData::new().init(md.clone(), 0, md.row_count as u32);
                match d.read_data(&rsp) {
                    Err(e) => {
                        assert_unsupported_charset(&e);
                        true
                    }
                    Ok(()) => false,
                }
            }
        };
        assert!(
            failed,
            "expected READSTAT_ERROR_UNSUPPORTED_CHARSET for EUC-TW on Windows (win-iconv)"
        );
    }

    #[cfg(not(windows))]
    {
        // glibc/macOS iconv supports EUC-TW; the ASCII-only data converts
        // losslessly.
        md_result.unwrap();
        assert_eq!(md.file_encoding, String::from("EUC-TW"));

        let mut d = ReadStatData::new().init(md.clone(), 0, md.row_count as u32);
        d.read_data(&rsp).unwrap();

        let batch = d.batch.unwrap();
        assert_eq!(batch.num_rows(), 80);
        let how_arrived = batch
            .column(11)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(how_arrived.value(0), "Bus");
    }
}
