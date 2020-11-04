use structopt::StructOpt;

use readstat::Args;
use std::process;

fn main() {
    let args = Args::from_args();
    if let Err(e) = readstat::run(args) {
        println!("Stopping with error: {}", e);
        process::exit(1);
    }
    process::exit(0);
}
