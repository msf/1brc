#![feature(test)]
extern crate test; // for benchmarking

mod aggregator;

use std::env;
use std::io;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <filename>", args[0]);
        return;
    }

    if let Err(err) = aggregator::process_file(&args[1], &mut io::stdout()) {
        eprintln!("Error: {}", err);
    }
}
