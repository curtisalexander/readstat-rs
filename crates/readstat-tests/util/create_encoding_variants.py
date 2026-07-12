#!/usr/bin/env python3
"""Derive encoding-variant test datasets from messydata.sas7bdat.

The sas7bdat header stores the file encoding as a single byte at offset 70
(the `encoding` field of `sas_header_start_t` in ReadStat's readstat_sas.h:
32-byte magic + 7 mystery/flag bytes + 30-byte pad). ReadStat maps that byte
through its charset table in readstat_sas.c — 62 = WINDOWS-1252,
61 = WINDOWS-1251, 119 = EUC-TW — and feeds the resulting name to
iconv_open(), so patching the byte exercises the real character-conversion
path end to end. messydata.sas7bdat is uncompressed, so string cells can
also be patched in place as long as the replacement has the same length.

Outputs (deterministic; safe to re-run):

* messydata_1251.sas7bdat — encoding byte 62 -> 61 (WINDOWS-1251) and every
  "Bus" cell in the How_Arrived column replaced with the WINDOWS-1251 bytes
  C1 F3 F1, which decode as the Cyrillic "Бус". Readers must convert those
  cells through iconv; asserting the UTF-8 value proves the conversion,
  which matters on Windows where iconv is the vendored win-iconv.

* messydata_euctw.sas7bdat — encoding byte 62 -> 119 (EUC-TW), data bytes
  untouched (pure ASCII, which is valid EUC-TW). GNU/glibc/macOS iconv
  supports EUC-TW in software, so the file parses fine off-Windows; win-iconv
  has no Win32 codepage for it, so on Windows opening the file must fail
  cleanly with READSTAT_ERROR_UNSUPPORTED_CHARSET. The platform split is
  asserted in parse_encoding_test.rs.

Usage:  python3 create_encoding_variants.py
        (from anywhere; paths are resolved relative to this script)
"""

from pathlib import Path

ENCODING_BYTE_OFFSET = 70
WINDOWS_1252 = 0x3E  # 62
WINDOWS_1251 = 0x3D  # 61
EUC_TW = 0x77  # 119

# "Bus" in the How_Arrived column; C1 F3 F1 is "Бус" in WINDOWS-1251.
# The raw file has 10 occurrences: 7 exact "Bus" cells and one "Bus, Walk"
# cell among the 80 parsed rows, plus 2 ghost row-images in unreferenced
# page space that parsers never return. All 10 are patched; tests assert
# against the 8 visible ones.
BUS_ASCII = b"Bus"
BUS_CYRILLIC_1251 = bytes([0xC1, 0xF3, 0xF1])
EXPECTED_BUS_CELLS = 10

DATA_DIR = Path(__file__).resolve().parent.parent / "tests" / "data"
SOURCE = DATA_DIR / "messydata.sas7bdat"


def read_source() -> bytearray:
    data = bytearray(SOURCE.read_bytes())
    actual = data[ENCODING_BYTE_OFFSET]
    assert actual == WINDOWS_1252, (
        f"{SOURCE.name}: expected encoding byte 0x{WINDOWS_1252:02X} (WINDOWS-1252) "
        f"at offset {ENCODING_BYTE_OFFSET}, found 0x{actual:02X} — "
        "has the source dataset changed?"
    )
    return data


def make_1251() -> None:
    data = read_source()
    data[ENCODING_BYTE_OFFSET] = WINDOWS_1251

    count = 0
    start = 0
    while (i := bytes(data).find(BUS_ASCII, start)) != -1:
        data[i : i + len(BUS_ASCII)] = BUS_CYRILLIC_1251
        start = i + len(BUS_ASCII)
        count += 1
    assert count == EXPECTED_BUS_CELLS, (
        f"expected {EXPECTED_BUS_CELLS} 'Bus' cells, patched {count} — "
        "has the source dataset changed?"
    )

    out = DATA_DIR / "messydata_1251.sas7bdat"
    out.write_bytes(bytes(data))
    print(f"wrote {out} (encoding WINDOWS-1251, {count} cells -> Бус)")


def make_euctw() -> None:
    data = read_source()
    data[ENCODING_BYTE_OFFSET] = EUC_TW

    out = DATA_DIR / "messydata_euctw.sas7bdat"
    out.write_bytes(bytes(data))
    print(f"wrote {out} (encoding EUC-TW, data unchanged)")


if __name__ == "__main__":
    make_1251()
    make_euctw()
