use std::io::{self, Write};

use std::sync::mpsc;
use std::thread;

use log::{debug};

use crossbeam::channel::{self, Receiver};

use crate::aggregator;

pub fn process_file(
    filename: &str,
    output: &mut dyn std::io::Write,
    chunks: usize,
) -> io::Result<()> {
    let agg = ParallelMeasurementAggregator::new(chunks);
    agg.process(filename, output)?;
    Ok(())
}

struct Task {
    start: u64,
    end: u64,
    filename: String,
    response: mpsc::Sender<aggregator::MeasurementAggregator>,
}

struct ParallelMeasurementAggregator {
    workers: usize,
    task_tx: channel::Sender<Option<Task>>,
    worker_handles: Vec<thread::JoinHandle<()>>,
}

impl Drop for ParallelMeasurementAggregator {
    fn drop(&mut self) {
        for _ in &self.worker_handles {
            self.task_tx.send(None).unwrap();
        }

        for handle in self.worker_handles.drain(..) {
            handle.join().unwrap();
        }
    }
}

impl ParallelMeasurementAggregator {
    fn new(workers: usize) -> Self {
        let (tx, rx) = channel::bounded(workers);

        let handles = (0..workers)
            .map(|_| {
                let rx: Receiver<Option<Task>> = rx.clone();
                thread::spawn(move || {
                    loop {
                        match rx.recv() {
                            Ok(Some(part)) => {
                                let mut agg = aggregator::MeasurementAggregator::new();
                                agg.process_chunk(part.filename.as_str(), part.start, part.end)
                                    .unwrap();

                                part.response.send(agg).unwrap();
                            }
                            Ok(None) => break, // Shutdown the worker thread on None value
                            Err(_) => break,   // Shutdown the worker thread on error
                        }
                    }
                })
            })
            .collect();

        ParallelMeasurementAggregator {
            workers,
            task_tx: tx,
            worker_handles: handles,
        }
    }

    fn process(&self, filename: &str, output: &mut dyn Write) -> io::Result<()> {
        let metadata = std::fs::metadata(&filename).unwrap();
        let file_size = metadata.len();
        let min_chunk_size = 1024_u64;
        let chunk_size = file_size.min(min_chunk_size.max(file_size / self.workers as u64));
        let chunks = file_size / chunk_size + 1;

        let (tx, rx) = mpsc::channel();
        for i in 0..chunks {
            let start = i * chunk_size;
            let mut end = (i + 1) * chunk_size;
            if i == chunks - 1 {
                end = 0;
            }

            debug!("size:{}, Task: {} - ]{}-{}]", file_size, i, start, end);
            let t = Task {
                start: start,
                end: end,
                filename: filename.to_string(),
                response: tx.clone(),
            };
            self.task_tx.send(Some(t)).unwrap();
        }
        let mut result = aggregator::MeasurementAggregator::new();
        for _ in 0..chunks {
            let part = rx.recv().unwrap();
            result.merge(&part);
        }
        result.write(output)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io::Read};
    use test::Bencher;

    #[test]
    fn test_rounding() {
        let _ = env_logger::try_init();

        // Create a new instance of ParallelAggregator
        let aggregator = ParallelMeasurementAggregator::new(16);

        // Run the aggregator
        let input_file = "../../../test/resources/samples/measurements-rounding.txt";
        let expected_output_file = "../../../test/resources/samples/measurements-rounding.out";
        let mut output = Vec::new();

        let mut expected_output = String::new();
        fs::File::open(&expected_output_file)
            .unwrap()
            .read_to_string(&mut expected_output)
            .unwrap();

        // run the aggregator
        aggregator.process(input_file, &mut output).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert_eq!(
            output_str, expected_output,
            "Failed for file: {}",
            input_file
        );
    }

    #[test]
    fn test_parallel_basic() {
        let _ = env_logger::try_init();

        // Create a new instance of ParallelAggregator
        let aggregator = ParallelMeasurementAggregator::new(16);

        // Run the aggregator
        let input_file = "../../../test/resources/samples/measurements-3.txt";
        let mut output = Vec::new();
        aggregator.process(input_file, &mut output).unwrap();

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
                .process(&file_path.to_string_lossy(), &mut output)
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
        let agg = ParallelMeasurementAggregator::new(16);
        b.iter(|| {
            let mut output = Vec::new();
            agg.process(&test_file.to_string(), &mut output)
                .unwrap();
        });
    }
}
