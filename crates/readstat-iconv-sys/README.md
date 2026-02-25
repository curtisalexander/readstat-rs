# iconv-sys

Windows-only FFI bindings to [libiconv](https://www.gnu.org/software/libiconv/) for character encoding conversion.

The `build.rs` script compiles libiconv from the vendored `vendor/libiconv-win-build/` git submodule using the `cc` crate. On non-Windows platforms the build script is a no-op.

The `links = "iconv"` key in `Cargo.toml` allows `readstat-sys` to discover the include path via the `DEP_ICONV_INCLUDE` environment variable.
