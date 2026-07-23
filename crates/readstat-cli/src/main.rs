#![allow(clippy::module_name_repetitions)]

use clap::Parser;

mod cli;
mod run;

fn main() {
    let args = cli::ReadStatCli::parse();
    if let Err(e) = run::run(args) {
        if is_broken_pipe(&e) {
            return;
        }
        eprintln!("Stopping with error: {e}");
        // Exit 1 for runtime failures. clap reserves exit code 2 for
        // usage/argument errors, so keep those distinct.
        std::process::exit(1);
    }
    std::process::exit(0);
}

fn is_broken_pipe(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(err) = current {
        // arrow-csv currently flattens writer I/O errors into CsvError text
        // rather than preserving std::io::Error as a source.
        if err
            .downcast_ref::<readstat::arrow::error::ArrowError>()
            .is_some_and(|error| {
                matches!(error, readstat::arrow::error::ArrowError::CsvError(message) if message.contains("Broken pipe"))
            })
        {
            return true;
        }
        if err
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
        {
            return true;
        }
        current = err.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::is_broken_pipe;

    #[test]
    fn recognizes_io_and_arrow_csv_broken_pipes() {
        let io = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "closed");
        assert!(is_broken_pipe(&io));

        let arrow = readstat::arrow::error::ArrowError::CsvError(
            "Csv error: Broken pipe (os error 32)".into(),
        );
        assert!(is_broken_pipe(&arrow));
    }
}
