#![allow(clippy::redundant_static_lifetimes)]
#![allow(dead_code)]
#![allow(deref_nullptr)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

// Only needed for Windows
#[cfg(windows)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
