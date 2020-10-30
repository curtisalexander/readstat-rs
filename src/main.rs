use structopt::StructOpt;

use readstat_rs::Args;
use std::process;

fn main() {
    let args = Args::from_args();
    if let Err(e) = readstat_rs::run(args) {
        println!("Stopping with error: {}", e);
        process::exit(1);
    }
    process::exit(0);
}
