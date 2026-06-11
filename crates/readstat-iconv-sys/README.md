# readstat-iconv-sys

Windows-only FFI bindings to [libiconv](https://www.gnu.org/software/libiconv/) for character encoding conversion.

The `build.rs` script compiles libiconv from the vendored `vendor/libiconv-win-build/` git submodule using the `cc` crate. On non-Windows platforms the build script is a no-op.

The `links = "iconv"` key in `Cargo.toml` allows `readstat-sys` to discover the include path via the `DEP_ICONV_INCLUDE` environment variable.

## License

This crate is `LGPL-2.1-or-later AND MIT`: it statically links the vendored
libiconv, which is `LGPL-2.1-or-later`, into Windows builds. Distributors of
Windows binaries built with this crate are subject to the LGPL §6 relinking
obligation. On non-Windows platforms the build script is a no-op and the crate
links nothing.
