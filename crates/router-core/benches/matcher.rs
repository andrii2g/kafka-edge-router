//! Candidate generation, bounded dispatch, queue state, and mutation benchmarks.

use std::{hint::black_box, sync::Arc};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput};
use router_core::{
    DeliveryProtocol, RouteFilter, RouteKey, RoutedMessage, Router, RouterConfig, RoutingMetadata,
    SubscriptionId,
};

fn metadata(dimensions: usize) -> RoutingMetadata {
    let mut metadata = RoutingMetadata {
        message_id: Arc::from("benchmark-message"),
        tenant_id: Arc::from("tenant-benchmark"),
        kind: None,
        message_type: None,
        channel: None,
        actor_id: None,
        audience_type: None,
        audience_id: None,
        content_type: Arc::from("application/octet-stream"),
        timestamp_ms: None,
        source: None,
    };
    if dimensions >= 2 {
        metadata.kind = Some(Arc::from("content"));
        metadata.channel = Some(Arc::from("news"));
    }
    if dimensions >= 4 {
        metadata.message_type = Some(Arc::from("broadcast"));
        metadata.actor_id = Some(Arc::from("actor-1"));
    }
    if dimensions >= 6 {
        metadata.audience_type = Some(Arc::from("team"));
        metadata.audience_id = Some(Arc::from("team-7"));
    }
    metadata
}

fn message(payload_size: usize) -> Arc<RoutedMessage> {
    Arc::new(
        RoutedMessage::new(metadata(6), Bytes::from(vec![0x5a; payload_size]))
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

fn benchmark_config(queue_capacity: usize, subscriptions: usize) -> RouterConfig {
    RouterConfig {
        default_queue_capacity: queue_capacity,
        max_queue_capacity: queue_capacity,
        max_connections: 2_000,
        max_connections_per_tenant: 2_000,
        max_subscriptions: subscriptions,
        max_subscriptions_per_tenant: subscriptions,
        max_subscriptions_per_connection: subscriptions,
        slow_consumer_strikes: u32::MAX,
    }
}

fn candidate_generation(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("candidate_generation");
    for dimensions in [0_usize, 2, 4, 6] {
        let metadata = metadata(dimensions);
        group.bench_with_input(
            BenchmarkId::from_parameter(dimensions),
            &metadata,
            |bencher, metadata| {
                bencher.iter(|| black_box(RouteKey::candidates(black_box(metadata))));
            },
        );
    }
    group.finish();
}

fn unmatched_dispatch(criterion: &mut Criterion) {
    let router = Router::new(RouterConfig::default());
    let mut group = criterion.benchmark_group("dispatch/unmatched_payload_bytes");
    for payload_size in [128_usize, 1_024, 16_384, 262_144, 1_048_576] {
        let message = message(payload_size);
        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_size),
            &message,
            |bencher, message| {
                bencher.iter(|| black_box(router.dispatch(Arc::clone(message))));
            },
        );
    }
    group.finish();
}

fn fan_out(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("dispatch/accepted_fan_out");
    for connection_count in [1_usize, 32, 256, 1_024] {
        let router = Router::new(benchmark_config(1, connection_count));
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
        let message = message(1_024);
        group.throughput(Throughput::Elements(connection_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(connection_count),
            &connection_count,
            |bencher, _| {
                bencher.iter(|| {
                    black_box(router.dispatch(Arc::clone(&message)));
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

fn full_queue_dispatch(criterion: &mut Criterion) {
    let router = Router::new(benchmark_config(1, 1));
    let registration = router
        .register_connection("tenant-benchmark", DeliveryProtocol::WebSocket, Some(1))
        .expect("benchmark registration");
    router
        .subscribe(
            registration.connection_id,
            SubscriptionId::new("full-queue").expect("benchmark subscription id"),
            wildcard_filter(),
        )
        .expect("benchmark subscription");
    let message = message(1_024);
    let _ = router.dispatch(Arc::clone(&message));
    criterion.bench_function("dispatch/full_queue", |bencher| {
        bencher.iter(|| black_box(router.dispatch(Arc::clone(&message))));
    });
    black_box(registration);
}

fn subscription_mutation(criterion: &mut Criterion) {
    let router = Router::new(benchmark_config(1, 1));
    let registration = router
        .register_connection("tenant-benchmark", DeliveryProtocol::WebSocket, Some(1))
        .expect("benchmark registration");
    let mut sequence = 0_u64;
    criterion.bench_function("subscription/subscribe_unsubscribe", |bencher| {
        bencher.iter(|| {
            sequence += 1;
            let id = SubscriptionId::new(format!("churn-{sequence}"))
                .expect("benchmark subscription id");
            router
                .subscribe(registration.connection_id, id.clone(), wildcard_filter())
                .expect("benchmark subscribe");
            router
                .unsubscribe(registration.connection_id, &id)
                .expect("benchmark unsubscribe");
        });
    });
    black_box(registration);
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    candidate_generation(&mut criterion);
    unmatched_dispatch(&mut criterion);
    fan_out(&mut criterion);
    full_queue_dispatch(&mut criterion);
    subscription_mutation(&mut criterion);
    criterion.final_summary();
}
