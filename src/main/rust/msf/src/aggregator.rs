use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};

pub fn process_file(filename: &str, output: &mut dyn Write) -> io::Result<()> {
    let file = File::open(filename)?;
    let reader = io::BufReader::new(file);

    let mut aggregator = MeasurementAggregator::new();

    for line in reader.split(b'\n') {
        let line = line?;
        aggregator.add(line);
    }

    aggregator.write(output)?;

    Ok(())
}

struct MeasurementAggregator {
    data: HashMap<String, Aggregate>,
}

impl MeasurementAggregator {
    fn new() -> Self {
        MeasurementAggregator {
            data: HashMap::new(),
        }
    }

    fn add(&mut self, line: Vec<u8>) {
        let parts: Vec<&[u8]> = line.split(|x| *x == b';').collect();
        let value_str = std::str::from_utf8(&parts[1]).expect("Invalid UTF-8 sequence");
        let value: f64 = value_str.parse().expect(format!("Invalid float value: {}", value_str).as_str());
        let temp = round(value);
        let loc = std::str::from_utf8(parts[0]).expect("Invalid UTF-8 sequence").to_owned();
        self.data
            .entry(loc)
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

    fn write(&self, output: &mut dyn Write) -> io::Result<()> {
        let mut keys: Vec<_> = self.data.keys().collect();
        keys.sort_unstable();
        output.write_all(b"{")?;
        let mut first = true;
        for location in keys {
            let stats = &self.data[location];
            if !first {
                output.write_all(b", ")?;
            }
            output.write_all(location.as_bytes())?;
            output.write_all(b"=")?;
            output.write_all(stats.to_string().as_bytes())?;
            first = false;
        }
        output.write_all(b"}\n")?;
        Ok(())
    }
}

#[derive(PartialEq, Debug)]
struct Aggregate {
    min: Temperature,
    max: Temperature,
    sum: Temperature,
    count: usize,
}

impl Aggregate {
    fn to_string(&self) -> String {
        let avg = round(self.sum) / self.count as f64;
        String::from(format!("{:.1}/{:.1}/{:.1}", self.min, round(avg), self.max))
    }

    fn add(&mut self, measurement: Temperature) {
        self.min = self.min.min(measurement);
        self.max = self.max.max(measurement);
        self.sum += measurement;
        self.count += 1;
    }
}

type Temperature = f64;

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
fn round(x: Temperature) -> Temperature {
    ((x + 0.05) * 10.0).floor() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io::Read};
    use test::Bencher;

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
            process_file(&file_path.to_string_lossy(), &mut output).unwrap();

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

        let line1 = b"Location1;25.0".to_vec();
        aggregator.add(line1);

        let line2 = b"Location2;30.0".to_vec();
        aggregator.add(line2);

        let line3 = b"Location1;20.0".to_vec();
        aggregator.add(line3);

        let line4 = b"Location2;35.0".to_vec();
        aggregator.add(line4);

        let line5 = b"Location3;15.0".to_vec();
        aggregator.add(line5);

        let mut expected_data = HashMap::new();
        expected_data.insert(
            "Location1".to_string(),
            Aggregate {
                min: 20.0,
                max: 25.0,
                sum: 45.0,
                count: 2,
            },
        );
        expected_data.insert(
            "Location2".to_string(),
            Aggregate {
                min: 30.0,
                max: 35.0,
                sum: 65.0,
                count: 2,
            },
        );
        expected_data.insert(
            "Location3".to_string(),
            Aggregate {
                min: 15.0,
                max: 15.0,
                sum: 15.0,
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
            process_file(&test_file.to_string(), &mut output).unwrap();
        });
    }
}
