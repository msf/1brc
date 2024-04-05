#![feature(test)]
extern crate test; // for benchmarking

mod aggregator;
mod parallel;

use std::env;
use std::io;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut workers = 10_usize;
    if args.len() < 2 {
        eprintln!("Usage: {} <filename> ?workers", args[0]);
        return;
    } else if args.len() == 3 {
        workers = args[2].parse().expect("Invalid number of workers");
    }

    if let Err(err) = parallel::process_file(&args[1], &mut io::stdout(), workers) {
        eprintln!("Error: {}", err);
    }
}
