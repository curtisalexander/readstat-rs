//! Raw FFI bindings to [libiconv](https://www.gnu.org/software/libiconv/) for Windows.
//!
//! This crate compiles libiconv from a vendored git submodule on Windows and is a
//! no-op on other platforms. It exists primarily to support [`readstat-sys`](https://docs.rs/readstat-sys),
//! which needs iconv for character encoding conversion in the `ReadStat` C library.

// Auto-generated bindgen code — suppress lints from generated bindings
#![allow(clippy::pedantic)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::ptr_offset_with_cast)]
#![allow(clippy::useless_transmute)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(dead_code)]
#![allow(deref_nullptr)]
#![allow(improper_ctypes)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

// Only needed for Windows
#[cfg(windows)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
