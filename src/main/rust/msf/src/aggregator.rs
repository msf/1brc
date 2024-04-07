use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Seek, Write};

use log::debug;

pub struct MeasurementAggregator {
    data: HashMap<String, Aggregate>,
}

impl MeasurementAggregator {
    pub fn new() -> Self {
        MeasurementAggregator {
            data: HashMap::with_capacity(500),
        }
    }

    fn process(&mut self, filename: &str, output: &mut dyn Write) -> io::Result<()> {
        self.process_chunk(filename, 0, 0)?.write(output)?;
        Ok(())
    }

    pub fn process_chunk(&mut self, filename: &str, start: u64, end: u64) -> io::Result<&Self> {
        let mut file = File::open(filename)?;
        let mut curr = file.seek(io::SeekFrom::Start(start))?;
        let mut reader = io::BufReader::new(file);
        let mut first = true;

        let mut buffer = Vec::with_capacity(256);
        loop {
            buffer.clear();
            let bytes_read = reader.read_until(b'\n', &mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            if end != 0 && curr > end {
                break;
            }
            curr += bytes_read as u64;
            if first && start != 0 {
                // skip the first line if we're starting in non zero offset, incomplete
                first = false;
                continue;
            }
            // Remove the newline character from the end of the buffer
            let line = &buffer[..buffer.len() - 1];

            self.add(line);
        }

        for line in reader.lines() {
            if end != 0 && curr > end {
                break;
            }
            let line = line?;
            curr += line.len() as u64 + 1;
        }
        debug!(
            "process bytes:{} curr:{} - ]{start}-{end}]",
            curr - start,
            curr
        );

        return Ok(self);
    }

    fn add(&mut self, line: &[u8]) {
        let (location, temp) = parse(line);
        self.data
            .entry(location)
            .and_modify(|agg| {
                agg.add(temp);
            })
            .or_insert(Aggregate {
                min: temp,
                max: temp,
                sum: temp,
                count: 1,
            });
    }

    pub fn merge(&mut self, other: &MeasurementAggregator) {
        for (location, stats) in &other.data {
            self.data
                .entry(location.clone())
                .and_modify(|agg| {
                    agg.merge(&stats);
                })
                .or_insert(stats.clone());
        }
    }

    pub fn write(&self, output: &mut dyn Write) -> io::Result<()> {
        let mut keys: Vec<_> = self.data.keys().collect();
        keys.sort_unstable();
        output.write_all(b"{")?;
        let mut first = true;
        for location in keys {
            let aggregate = &self.data[location];
            if !first {
                output.write_all(b", ")?;
            } else {
                first = false
            }
            output.write_all(location.as_bytes())?;
            output.write_all(b"=")?;
            output.write_all(aggregate.to_string().as_bytes())?;
            debug!(
                "{}: sum:{} count:{} min:{} max:{}",
                location, aggregate.sum, aggregate.count, aggregate.min, aggregate.max
            )
        }
        output.write_all(b"}\n")?;
        Ok(())
    }
}

#[derive(PartialEq, Debug, Clone)]
struct Aggregate {
    min: Temperature,
    max: Temperature,
    sum: Temperature,
    count: usize,
}

impl Aggregate {
    fn to_string(&self) -> String {
        let min = self.min as f32 / FLOAT2INT;
        let max = self.max as f32 / FLOAT2INT;
        String::from(format!("{:.1}/{:.1}/{:.1}", min, self.avg(), max))
    }

    fn add(&mut self, measurement: Temperature) {
        self.min = self.min.min(measurement);
        self.max = self.max.max(measurement);
        self.sum += measurement;
        self.count += 1;
    }

    fn merge(&mut self, other: &Aggregate) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn avg(&self) -> f32 {
        let t = self.sum as f32 / ((self.count as f32) * FLOAT2INT);
        return round(t);
    }
}

type Temperature = i32;
const FLOAT2INT: f32 = 10.0;

fn parse(line: &[u8]) -> (String, Temperature) {
    let parts: Vec<_> = line.split(|&x| x == b';').collect();
    let location = String::from_utf8(parts[0].to_vec()).unwrap();
    let temp = parsei32(parts[1]);
    return (location, temp);
}

fn parsei32(val: &[u8]) -> i32 {
    let mut num: i32 = 0;
    let mut sign: i32 = 1;
    let ascii_zero = '0' as i32;
    for (i, c) in val.iter().enumerate() {
        if i == 0 && *c == b'-' {
            sign = -1;
            continue;
        }
        if *c < b'0' || *c > b'9' {
            continue;
        }
        num = num * 10 + ((*c as i32) - ascii_zero);
    }
    return num * sign;
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
fn round(x: f32) -> f32 {
    ((x + 0.05) * 10.0).floor() / 10.0
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
        let mut aggregator = MeasurementAggregator::new();

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
    fn test_process_file() {
        let test_dir = "../../../test/resources/samples/";
        let files = fs::read_dir(test_dir).unwrap();

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
            MeasurementAggregator::new()
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

    #[test]
    fn test_measurement_aggregator() {
        let mut aggregator = MeasurementAggregator::new();

        let line1 = b"Location1;25.0";
        aggregator.add(line1);

        let line2 = b"Location2;30.0";
        aggregator.add(line2);

        let line3 = b"Location1;20.0";
        aggregator.add(line3);

        let line4 = b"Location2;35.0";
        aggregator.add(line4);

        let line5 = b"Location3;15.0";
        aggregator.add(line5);

        let mut expected_data = HashMap::new();
        expected_data.insert(
            "Location1".to_string(),
            Aggregate {
                min: 200,
                max: 250,
                sum: 450,
                count: 2,
            },
        );
        expected_data.insert(
            "Location2".to_string(),
            Aggregate {
                min: 300,
                max: 350,
                sum: 650,
                count: 2,
            },
        );
        expected_data.insert(
            "Location3".to_string(),
            Aggregate {
                min: 150,
                max: 150,
                sum: 150,
                count: 1,
            },
        );

        assert_eq!(aggregator.data, expected_data);
    }

    #[bench]
    fn bench_process_file(b: &mut Bencher) {
        let test_file = "../../../test/resources/samples/measurements.bench";
        b.iter(|| {
            let mut output = Vec::new();
            MeasurementAggregator::new()
                .process(&test_file.to_string(), &mut output)
                .unwrap();
        });
    }
}
