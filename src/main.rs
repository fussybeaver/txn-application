use std::path::PathBuf;

use clap::{Parser, command};
use txn_assignment::run;

#[derive(Debug, Parser)]
#[command(version, about, long_about=None)]
struct Args {
    filename: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(args.filename).await {
        Ok(_) => (),
        Err(e) => eprintln!("{}", e),
    }
}
