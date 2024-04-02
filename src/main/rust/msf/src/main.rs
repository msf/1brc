#![feature(test)]
extern crate test; // for benchmarking

mod aggregator;
mod parallel;

use std::env;
use std::io;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut chunks = 10_usize;
    if args.len() < 3 {
        eprintln!("Usage: {} <filename> ?chunks", args[0]);
        return;
    } else if args.len() == 4 {
        chunks = args[3].parse().expect("Invalid number of chunks");
    }

    if let Err(err) = parallel::process_file(&args[1], &mut io::stdout(), chunks) {
        eprintln!("Error: {}", err);
    }
}
