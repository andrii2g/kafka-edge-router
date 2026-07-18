//! Candidate generation and bounded dispatch microbenchmarks.

use std::{hint::black_box, sync::Arc};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput};
use router_core::{
    DeliveryProtocol, RouteFilter, RouteKey, RoutedMessage, Router, RouterConfig, RoutingMetadata,
    SubscriptionId,
};

fn metadata() -> RoutingMetadata {
    RoutingMetadata {
        message_id: Arc::from("benchmark-message"),
        tenant_id: Arc::from("tenant-benchmark"),
        kind: Some(Arc::from("content")),
        message_type: Some(Arc::from("broadcast")),
        channel: Some(Arc::from("news")),
        actor_id: Some(Arc::from("actor-1")),
        audience_type: Some(Arc::from("team")),
        audience_id: Some(Arc::from("team-7")),
        content_type: Arc::from("application/json"),
        timestamp_ms: None,
        source: None,
    }
}

fn message() -> Arc<RoutedMessage> {
    Arc::new(
        RoutedMessage::new(metadata(), Bytes::from_static(br#"{"ok":true}"#))
            .expect("benchmark message is valid"),
    )
}

fn wildcard_filter() -> RouteFilter {
    RouteFilter {
        tenant_id: Arc::from("tenant-benchmark"),
        kind: None,
        message_type: None,
        channel: None,
        actor_id: None,
        audience_type: None,
        audience_id: None,
    }
}

fn candidate_generation(criterion: &mut Criterion) {
    let metadata = metadata();
    criterion.bench_function("candidate_generation/all_dimensions", |bencher| {
        bencher.iter(|| black_box(RouteKey::candidates(black_box(&metadata))));
    });
}

fn unmatched_dispatch(criterion: &mut Criterion) {
    let router = Router::new(RouterConfig::default());
    let message = message();
    criterion.bench_function("dispatch/unmatched", |bencher| {
        bencher.iter(|| black_box(router.dispatch(Arc::clone(&message))));
    });
}

fn fan_out(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("dispatch/fan_out");
    for connection_count in [1_usize, 32, 256] {
        let router = Router::new(RouterConfig {
            default_queue_capacity: 1,
            max_queue_capacity: 1,
            max_subscriptions_per_connection: 1,
            slow_consumer_strikes: 3,
            ..RouterConfig::default()
        });
        let mut registrations = Vec::with_capacity(connection_count);
        for index in 0..connection_count {
            let registration = router
                .register_connection("tenant-benchmark", DeliveryProtocol::WebSocket, Some(1))
                .expect("benchmark registration");
            router
                .subscribe(
                    registration.connection_id,
                    SubscriptionId::new(format!("subscription-{index}"))
                        .expect("benchmark subscription id"),
                    wildcard_filter(),
                )
                .expect("benchmark subscription");
            registrations.push(registration);
        }
        let message = message();
        group.throughput(Throughput::Elements(connection_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(connection_count),
            &connection_count,
            |bencher, _| {
                bencher.iter(|| {
                    let report = router.dispatch(Arc::clone(&message));
                    black_box(report);
                    for registration in &mut registrations {
                        black_box(
                            registration
                                .receiver
                                .try_recv()
                                .expect("benchmark delivery must be queued"),
                        );
                    }
                });
            },
        );
    }
    group.finish();
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    candidate_generation(&mut criterion);
    unmatched_dispatch(&mut criterion);
    fan_out(&mut criterion);
    criterion.final_summary();
}
