//! Atomic metrics observation overhead microbenchmarks.

use std::{hint::black_box, time::Duration};

use criterion::Criterion;
use router_core::{LatencyStage, Metrics};

fn observability_overhead(criterion: &mut Criterion) {
    let metrics = Metrics::default();
    let duration = Duration::from_micros(250);
    let mut group = criterion.benchmark_group("observability");

    group.bench_function("baseline", |bencher| {
        bencher.iter(|| black_box(duration));
    });
    group.bench_function("counter", |bencher| {
        bencher.iter(|| metrics.record_kafka_message(black_box(1024)));
    });
    group.bench_function("histogram", |bencher| {
        bencher.iter(|| {
            metrics.record_latency(LatencyStage::EndToEnd, black_box(duration));
        });
    });
    group.finish();
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    observability_overhead(&mut criterion);
    criterion.final_summary();
}
