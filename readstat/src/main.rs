use structopt::StructOpt;

fn main() {
    let args = readstat::ReadStat::from_args();
    if let Err(e) = readstat::run(args) {
        println!("Stopping with error: {}", e);
        std::process::exit(1);
    }
    std::process::exit(0);
}
