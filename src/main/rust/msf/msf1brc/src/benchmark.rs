mod aggregator;

use criterion::{criterion_group, criterion_main, Criterion};

fn process_file_benchmark(c: &mut Criterion) {
    let test_file = "../../../../test/resources/samples/measurements.bench";
    c.bench_function("process_file", |b| {
        b.iter(|| {
            let mut output = Vec::new();
            aggregator::process_file(&test_file.to_string(), &mut output).unwrap();
        })
    });
}

criterion_group!(benches, process_file_benchmark);
criterion_main!(benches);
