use clap::Parser;

mod cli;
mod run;

fn main() {
    let args = cli::ReadStatCli::parse();
    if let Err(e) = run::run(args) {
        eprintln!("Stopping with error: {e}");
        std::process::exit(2);
    }
    std::process::exit(0);
}
