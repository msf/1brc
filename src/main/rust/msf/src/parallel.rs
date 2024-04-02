use std::io::{self, Write};

use std::sync::mpsc;
use std::thread;

use crate::aggregator;

pub fn process_file(
    filename: &str,
    output: &mut dyn std::io::Write,
    chunks: usize,
) -> io::Result<()> {
    let agg = ParallelMeasurementAggregator::new(chunks);
    agg.process_file(filename, output)?;
    Ok(())
}

struct ParallelMeasurementAggregator {
    chunks: usize,
}

impl ParallelMeasurementAggregator {
    fn new(chunks: usize) -> Self {
        ParallelMeasurementAggregator { chunks }
    }
    fn process_file(&self, filename: &str, output: &mut dyn Write) -> io::Result<()> {
        let metadata = std::fs::metadata(&filename).unwrap();
        let file_size = metadata.len();
        let chunk_size = file_size / self.chunks as u64;

        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::with_capacity(self.chunks);
        for i in 0..self.chunks {
            let start = i as u64 * chunk_size;
            let stop = if i == self.chunks - 1 {
                0
            } else {
                start + chunk_size
            };

            let txi = tx.clone();
            let fname = filename.to_string();
            let handle = thread::spawn(move || {
                let mut agg = aggregator::MeasurementAggregator::new();
                agg.run(&fname, start, stop).unwrap();
                txi.send(agg).unwrap();
            });
            handles.push(handle);
        }
        let mut result = aggregator::MeasurementAggregator::new();
        for _ in 0..self.chunks {
            let part = rx.recv().unwrap();
            result.merge(&part);
        }
        result.write(output)?;

        for handle in handles {
            handle.join().unwrap();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io::Read};
    //    use test::Bencher;

    #[test]
    fn test_parallel_basic() {
        // Create a new instance of ParallelAggregator
        let mut aggregator = ParallelMeasurementAggregator::new(16);

        // Run the aggregator
        let input_file = "../../../test/resources/samples/measurements-3.txt";
        let mut output = Vec::new();
        aggregator.process_file(input_file, &mut output).unwrap();

        let expected_output_file = "../../../test/resources/samples/measurements-3.out";
        let mut expected_output = String::new();
        fs::File::open(&expected_output_file)
            .unwrap()
            .read_to_string(&mut expected_output)
            .unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert_eq!(
            output_str, expected_output,
            "Failed for file: {}",
            input_file
        );
    }

    use test::Bencher;

    #[test]
    fn test_process_file() {
        let test_dir = "../../../test/resources/samples/";
        let files = fs::read_dir(test_dir).unwrap();

        let aggregator = ParallelMeasurementAggregator::new(16);
        for file in files {
            let file = file.unwrap();
            let file_path = file.path();
            let fname = file.file_name();
            let file_name = fname.to_str().unwrap();
            if !file_name.ends_with(".txt") {
                continue;
            }
            let expected_output_file = format!("{}.out", file_name.strip_suffix(".txt").unwrap());
            let expected_output_path = file_path.with_file_name(expected_output_file);

            let mut expected_output = String::new();
            fs::File::open(&expected_output_path)
                .unwrap()
                .read_to_string(&mut expected_output)
                .unwrap();

            let mut output = Vec::new();
            aggregator
                .process_file(&file_path.to_string_lossy(), &mut output)
                .unwrap();

            let output_str = String::from_utf8(output).unwrap();
            assert_eq!(
                output_str, expected_output,
                "Failed for file: {}",
                file_name
            );
        }
    }

    #[bench]
    fn bench_process_file(b: &mut Bencher) {
        let test_file = "../../../test/resources/samples/measurements.bench";
        b.iter(|| {
            let agg = ParallelMeasurementAggregator::new(16);
            let mut output = Vec::new();
            agg.process_file(&test_file.to_string(), &mut output)
                .unwrap();
        });
    }
}
