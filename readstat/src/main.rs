use clap::Parser;
use readstat::ReadStatCli;

fn main() {
    let args = ReadStatCli::parse();
    if let Err(e) = readstat::run(args) {
        println!("Stopping with error: {}", e);
        std::process::exit(1);
    }
    std::process::exit(0);
}
