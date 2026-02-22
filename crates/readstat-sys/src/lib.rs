//! Raw FFI bindings to the [ReadStat](https://github.com/WizardMac/ReadStat) C library.
//!
//! This crate provides auto-generated bindings via [`bindgen`](https://docs.rs/bindgen).
//! It compiles the `ReadStat` C source files from a vendored git submodule and links
//! against platform-specific iconv and zlib libraries.
//!
//! These bindings expose the **full** `ReadStat` API, including support for SAS (`.sas7bdat`,
//! `.xpt`), SPSS (`.sav`, `.zsav`, `.por`), and Stata (`.dta`) file formats. However,
//! the higher-level [`readstat`](https://docs.rs/readstat) crate currently only implements
//! parsing and conversion for **SAS `.sas7bdat` files**.
//!
//! Most users should depend on the higher-level [`readstat`](https://docs.rs/readstat)
//! crate instead of using these bindings directly.

#![allow(clippy::missing_safety_doc)]
#![allow(clippy::ptr_offset_with_cast)]
#![allow(clippy::useless_transmute)]
#![allow(dead_code)]
#![allow(deref_nullptr)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
