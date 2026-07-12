# readstat-iconv-sys

Windows-only FFI bindings to [win-iconv](https://github.com/win-iconv/win-iconv) for character encoding conversion.

win-iconv is an iconv implementation backed by the Win32 conversion APIs
(`MultiByteToWideChar` / `WideCharToMultiByte`). It is **placed in the public
domain** (see `vendor/win-iconv/readme.txt` and the header of
`vendor/win-iconv/win_iconv.c`), so statically linking it imposes no copyleft
obligations — unlike GNU libiconv (LGPL-2.1), which this crate previously
vendored. It is the same iconv implementation that R for Windows uses
internally, so ReadStat-over-win-iconv is well proven in production.

The `build.rs` script compiles win-iconv from the vendored `vendor/win-iconv/`
git submodule using the `cc` crate when the *target* OS is Windows. On
non-Windows targets the build script is a no-op.

The `links = "iconv"` key in `Cargo.toml` allows `readstat-sys` to discover the
include path via the `DEP_ICONV_INCLUDE` environment variable.

## Encoding coverage

win-iconv maps encoding names to Windows codepages. All mainstream `sas7bdat`
encodings are covered: WINDOWS-1250..1258, ISO-8859-1..15, UTF-8/16/32,
US-ASCII, CP437/850/852/857/858/862/864/865/866/869/874, CP932 (Shift-JIS),
CP936 (GBK), CP949, CP950, GB18030, BIG5, EUC-JP, ISO-2022-JP, KOI8-R/U and the
common Mac codepages. A handful of tail encodings that GNU libiconv implements
in software are not available through Win32 codepages (e.g. EUC-TW,
ISO-2022-KR/CN, SHIFT_JISX0213, CP921/922/1129/1381, CP720/737/775); files in
those encodings fail cleanly with `READSTAT_ERROR_UNSUPPORTED_CHARSET` rather
than being read incorrectly.
