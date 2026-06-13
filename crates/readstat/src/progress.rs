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
    /// after that chunk finishes parsing, so `n` counts rows already completed
    /// (the chunk size) — the displayed position stays in step with work done.
    fn inc(&self, n: u64);

    /// Called once when parsing begins for the file at `path`.
    ///
    /// Implementations should be idempotent: the contract is a single
    /// "parsing started" notification per parse, but callers may invoke it
    /// more than once (e.g. the CLI guards against this internally).
    fn parsing_started(&self, path: &str);
}
