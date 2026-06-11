#![allow(clippy::module_name_repetitions)]

use clap::Parser;

mod cli;
mod run;

fn main() {
    let args = cli::ReadStatCli::parse();
    if let Err(e) = run::run(args) {
        eprintln!("Stopping with error: {e}");
        // Exit 1 for runtime failures. clap reserves exit code 2 for
        // usage/argument errors, so keep those distinct.
        std::process::exit(1);
    }
    std::process::exit(0);
}
