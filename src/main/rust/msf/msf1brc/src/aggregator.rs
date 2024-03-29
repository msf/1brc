use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};

struct MeasurementAggregator {
    data: HashMap<String, Aggregate>,
}

#[derive(PartialEq, Debug)]
struct Aggregate {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
}

impl Aggregate {
    fn to_string(&self) -> String {
        let avg = round(self.sum) / self.count as f64;
        String::from(format!("{:.1}/{:.1}/{:.1}", self.min, round(avg), self.max))
    }

    fn add(&mut self, measurement: &Measurement) {
        self.min = self.min.min(measurement.temperature);
        self.max = self.max.max(measurement.temperature);
        self.sum += measurement.temperature;
        self.count += 1;
    }
}

impl MeasurementAggregator {
    fn new() -> Self {
        MeasurementAggregator {
            data: HashMap::new(),
        }
    }

    fn add(&mut self, measurement: &Measurement) {
        self.data
            .entry(measurement.location.clone())
            .and_modify(|agg| {
                agg.add(measurement);
            })
            .or_insert(Aggregate {
                min: measurement.temperature,
                max: measurement.temperature,
                sum: measurement.temperature,
                count: 1,
            });
    }

    fn write(&self, output: &mut dyn Write) -> io::Result<()> {
        let mut keys: Vec<_> = self.data.keys().collect();
        keys.sort();
        write!(output, "{{")?;
        let mut first = true;
        for location in keys {
            let stats = &self.data[location];
            if !first {
                write!(output, ", ")?;
            }
            write!(output, "{}={}", location, stats.to_string())?;
            first = false;
        }
        write!(output, "}}\n")?;
        Ok(())
    }
}

struct Measurement {
    location: String,
    temperature: f64,
}

// rounding floats to 1 decimal place with 0.05 rounding up to 0.1
fn round(x: f64) -> f64 {
    ((x + 0.05) * 10.0).floor() / 10.0
}

pub fn process_file(filename: &str, output: &mut dyn Write) -> io::Result<()> {
    let file = File::open(filename)?;
    let reader = io::BufReader::new(file);

    let mut aggregator = MeasurementAggregator::new();

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split(';').collect();
        let value: f64 = parts[1].parse().unwrap();
        let measurement = Measurement {
            location: parts[0].to_string(),
            temperature: round(value),
        };
        aggregator.add(&measurement);
    }

    aggregator.write(output)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Read};

    use super::*;
    #[test]
    fn test_process_file() {
        let test_dir = "../../../../test/resources/samples/";
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

        let measurement1 = Measurement {
            location: "Location1".to_string(),
            temperature: 25.0,
        };
        aggregator.add(&measurement1);

        let measurement2 = Measurement {
            location: "Location2".to_string(),
            temperature: 30.0,
        };
        aggregator.add(&measurement2);

        let measurement3 = Measurement {
            location: "Location1".to_string(),
            temperature: 20.0,
        };
        aggregator.add(&measurement3);

        let measurement4 = Measurement {
            location: "Location2".to_string(),
            temperature: 35.0,
        };
        aggregator.add(&measurement4);

        let measurement5 = Measurement {
            location: "Location3".to_string(),
            temperature: 15.0,
        };
        aggregator.add(&measurement5);

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
}
