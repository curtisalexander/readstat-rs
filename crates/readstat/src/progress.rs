//! Progress reporting trait for parsing feedback.
//!
//! The [`ProgressCallback`] trait allows callers to receive progress updates
//! during data parsing without coupling the library to any specific progress
//! bar implementation. The CLI crate provides an `indicatif`-based implementation.

/// Trait for receiving progress updates during data parsing.
///
/// Implement this trait to display a progress bar, log progress, or perform
/// any other action when the parser makes forward progress.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use readstat::ProgressCallback;
///
/// struct LogProgress;
///
/// impl ProgressCallback for LogProgress {
///     fn inc(&self, n: u64) {
///         println!("Processed {n} more rows");
///     }
///     fn parsing_started(&self, path: &str) {
///         println!("Parsing file: {path}");
///     }
/// }
/// ```
pub trait ProgressCallback: Send + Sync {
    /// Called to advance progress by `n` rows. Invoked once per chunk, just
    /// before that chunk is parsed, so `n` is the number of rows about to be
    /// processed (the chunk size), not a count of rows already completed.
    fn inc(&self, n: u64);

    /// Called when parsing begins for the file at `path`.
    fn parsing_started(&self, path: &str);
}
