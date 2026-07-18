//! Lock-free counters and Prometheus text rendering.

use std::{
    fmt::Write as _,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const LATENCY_BUCKETS_SECONDS: [f64; 14] = [
    0.000_01, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1,
    0.5, 1.0,
];

/// Bounded operation names used by latency histograms.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyStage {
    /// Kafka header decoding and metadata validation.
    Decode,
    /// Route candidate lookup and match coalescing.
    Match,
    /// Bounded delivery queue fan-out.
    Enqueue,
    /// Protocol adapter encoding and output handoff.
    ProtocolWrite,
    /// One outbound webhook HTTP attempt.
    WebhookAttempt,
    /// Public Kafka publish validation and broker acknowledgement.
    Publish,
    /// Complete valid Kafka record processing through source commit request.
    EndToEnd,
}

impl LatencyStage {
    const ALL: [Self; 7] = [
        Self::Decode,
        Self::Match,
        Self::Enqueue,
        Self::ProtocolWrite,
        Self::WebhookAttempt,
        Self::Publish,
        Self::EndToEnd,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Match => "match",
            Self::Enqueue => "enqueue",
            Self::ProtocolWrite => "protocol_write",
            Self::WebhookAttempt => "webhook_attempt",
            Self::Publish => "publish",
            Self::EndToEnd => "end_to_end",
        }
    }
}

#[derive(Debug)]
struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_SECONDS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    fn observe(&self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        if let Some(bucket) = LATENCY_BUCKETS_SECONDS
            .iter()
            .position(|upper| seconds <= *upper)
        {
            self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            count: self.count.load(Ordering::Relaxed),
            sum_micros: self.sum_micros.load(Ordering::Relaxed),
        }
    }
}

/// Fixed-bucket latency histogram snapshot.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct HistogramSnapshot {
    /// Non-cumulative bucket counts in ascending boundary order.
    pub buckets: [u64; LATENCY_BUCKETS_SECONDS.len()],
    /// Total observations, including values above the largest finite bucket.
    pub count: u64,
    /// Sum of observed latency in microseconds.
    pub sum_micros: u64,
}

/// Public protocol that initiated a Kafka publish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishProtocol {
    /// JSON or base64 HTTP endpoint.
    Http,
    /// Raw-byte gRPC method.
    Grpc,
}

/// Process-wide router counters.
#[derive(Debug, Default)]
pub struct Metrics {
    kafka_messages: AtomicU64,
    kafka_bytes: AtomicU64,
    kafka_commit_errors: AtomicU64,
    kafka_rebalance_assignments: AtomicU64,
    kafka_rebalance_revocations: AtomicU64,
    kafka_rebalance_errors: AtomicU64,
    valid_messages: AtomicU64,
    invalid_messages: AtomicU64,
    matched_subscriptions: AtomicU64,
    unmatched_messages: AtomicU64,
    delivered_connections: AtomicU64,
    full_connections: AtomicU64,
    closed_connections: AtomicU64,
    slow_consumer_disconnects: AtomicU64,
    websocket_opened: AtomicU64,
    sse_opened: AtomicU64,
    grpc_opened: AtomicU64,
    webhook_opened: AtomicU64,
    webhook_attempts: AtomicU64,
    webhook_successes: AtomicU64,
    webhook_failures: AtomicU64,
    webhook_durable_commands: AtomicU64,
    webhook_retries_scheduled: AtomicU64,
    webhook_recovery_replays: AtomicU64,
    webhook_dead_letters: AtomicU64,
    http_publish_attempts: AtomicU64,
    grpc_publish_attempts: AtomicU64,
    http_publish_acknowledged: AtomicU64,
    grpc_publish_acknowledged: AtomicU64,
    http_publish_failures: AtomicU64,
    grpc_publish_failures: AtomicU64,
    active_connections: [AtomicU64; 4],
    active_subscriptions: [AtomicU64; 4],
    kafka_assigned_partitions: AtomicU64,
    kafka_lag_messages: AtomicU64,
    latency: [Histogram; 7],
}

impl Metrics {
    /// Records one Kafka record and its payload size.
    pub fn record_kafka_message(&self, bytes: usize) {
        self.kafka_messages.fetch_add(1, Ordering::Relaxed);
        self.kafka_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records a failed Kafka offset commit request or callback.
    pub fn record_kafka_commit_error(&self) {
        self.kafka_commit_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a Kafka consumer partition assignment callback.
    pub fn record_kafka_rebalance_assignment(&self) {
        self.kafka_rebalance_assignments
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a Kafka consumer partition revocation callback.
    pub fn record_kafka_rebalance_revocation(&self) {
        self.kafka_rebalance_revocations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a Kafka consumer rebalance error callback.
    pub fn record_kafka_rebalance_error(&self) {
        self.kafka_rebalance_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Replaces the current assigned-partition gauge after a rebalance callback.
    pub fn set_kafka_assigned_partitions(&self, partitions: usize) {
        self.kafka_assigned_partitions
            .store(partitions as u64, Ordering::Relaxed);
    }

    /// Replaces the current aggregate consumer-lag gauge.
    pub fn set_kafka_lag_messages(&self, lag: u64) {
        self.kafka_lag_messages.store(lag, Ordering::Relaxed);
    }

    /// Records one bounded latency observation.
    pub fn record_latency(&self, stage: LatencyStage, duration: Duration) {
        self.latency[stage.index()].observe(duration);
    }

    /// Records a decoded and validated message.
    pub fn record_valid_message(&self) {
        self.valid_messages.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a message rejected during decoding or validation.
    pub fn record_invalid_message(&self) {
        self.invalid_messages.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dispatch(
        &self,
        matched_subscriptions: usize,
        delivered_connections: usize,
        full_connections: usize,
        closed_connections: usize,
    ) {
        self.matched_subscriptions
            .fetch_add(matched_subscriptions as u64, Ordering::Relaxed);
        self.delivered_connections
            .fetch_add(delivered_connections as u64, Ordering::Relaxed);
        self.full_connections
            .fetch_add(full_connections as u64, Ordering::Relaxed);
        self.closed_connections
            .fetch_add(closed_connections as u64, Ordering::Relaxed);
        if matched_subscriptions == 0 {
            self.unmatched_messages.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_protocol_opened(&self, protocol: crate::DeliveryProtocol) {
        let counter = match protocol {
            crate::DeliveryProtocol::WebSocket => &self.websocket_opened,
            crate::DeliveryProtocol::Sse => &self.sse_opened,
            crate::DeliveryProtocol::Grpc => &self.grpc_opened,
            crate::DeliveryProtocol::HttpWebhook => &self.webhook_opened,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.active_connections[protocol_index(protocol)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_protocol_closed(
        &self,
        protocol: crate::DeliveryProtocol,
        subscriptions: usize,
    ) {
        decrement(&self.active_connections[protocol_index(protocol)], 1);
        decrement(
            &self.active_subscriptions[protocol_index(protocol)],
            subscriptions as u64,
        );
    }

    pub(crate) fn record_subscription_added(&self, protocol: crate::DeliveryProtocol) {
        self.active_subscriptions[protocol_index(protocol)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_subscription_removed(&self, protocol: crate::DeliveryProtocol) {
        decrement(&self.active_subscriptions[protocol_index(protocol)], 1);
    }

    pub(crate) fn record_slow_disconnect(&self) {
        self.slow_consumer_disconnects
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records one outbound webhook attempt.
    pub fn record_webhook_attempt(&self) {
        self.webhook_attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful outbound webhook delivery.
    pub fn record_webhook_success(&self) {
        self.webhook_successes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a terminal webhook delivery failure.
    pub fn record_webhook_failure(&self) {
        self.webhook_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a broker-acknowledged initial durable command.
    pub fn record_webhook_durable_command(&self) {
        self.webhook_durable_commands
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a broker-acknowledged persisted retry.
    pub fn record_webhook_retry_scheduled(&self) {
        self.webhook_retries_scheduled
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a retry command processed during startup recovery.
    pub fn record_webhook_recovery_replay(&self) {
        self.webhook_recovery_replays
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a broker-acknowledged dead-letter command.
    pub fn record_webhook_dead_letter(&self) {
        self.webhook_dead_letters.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one authenticated or rejected public publish attempt.
    pub fn record_publish_attempt(&self, protocol: PublishProtocol) {
        let counter = match protocol {
            PublishProtocol::Http => &self.http_publish_attempts,
            PublishProtocol::Grpc => &self.grpc_publish_attempts,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one broker-acknowledged public publish.
    pub fn record_publish_acknowledged(&self, protocol: PublishProtocol) {
        let counter = match protocol {
            PublishProtocol::Http => &self.http_publish_acknowledged,
            PublishProtocol::Grpc => &self.grpc_publish_acknowledged,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one public publish rejected or failed before acknowledgement.
    pub fn record_publish_failure(&self, protocol: PublishProtocol) {
        let counter = match protocol {
            PublishProtocol::Http => &self.http_publish_failures,
            PublishProtocol::Grpc => &self.grpc_publish_failures,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Captures a consistent-enough relaxed snapshot for status and metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            kafka_messages: self.kafka_messages.load(Ordering::Relaxed),
            kafka_bytes: self.kafka_bytes.load(Ordering::Relaxed),
            kafka_commit_errors: self.kafka_commit_errors.load(Ordering::Relaxed),
            kafka_rebalance_assignments: self.kafka_rebalance_assignments.load(Ordering::Relaxed),
            kafka_rebalance_revocations: self.kafka_rebalance_revocations.load(Ordering::Relaxed),
            kafka_rebalance_errors: self.kafka_rebalance_errors.load(Ordering::Relaxed),
            valid_messages: self.valid_messages.load(Ordering::Relaxed),
            invalid_messages: self.invalid_messages.load(Ordering::Relaxed),
            matched_subscriptions: self.matched_subscriptions.load(Ordering::Relaxed),
            unmatched_messages: self.unmatched_messages.load(Ordering::Relaxed),
            delivered_connections: self.delivered_connections.load(Ordering::Relaxed),
            full_connections: self.full_connections.load(Ordering::Relaxed),
            closed_connections: self.closed_connections.load(Ordering::Relaxed),
            slow_consumer_disconnects: self.slow_consumer_disconnects.load(Ordering::Relaxed),
            websocket_opened: self.websocket_opened.load(Ordering::Relaxed),
            sse_opened: self.sse_opened.load(Ordering::Relaxed),
            grpc_opened: self.grpc_opened.load(Ordering::Relaxed),
            webhook_opened: self.webhook_opened.load(Ordering::Relaxed),
            webhook_attempts: self.webhook_attempts.load(Ordering::Relaxed),
            webhook_successes: self.webhook_successes.load(Ordering::Relaxed),
            webhook_failures: self.webhook_failures.load(Ordering::Relaxed),
            http_publish_attempts: self.http_publish_attempts.load(Ordering::Relaxed),
            grpc_publish_attempts: self.grpc_publish_attempts.load(Ordering::Relaxed),
            webhook_durable_commands: self.webhook_durable_commands.load(Ordering::Relaxed),
            webhook_retries_scheduled: self.webhook_retries_scheduled.load(Ordering::Relaxed),
            webhook_recovery_replays: self.webhook_recovery_replays.load(Ordering::Relaxed),
            webhook_dead_letters: self.webhook_dead_letters.load(Ordering::Relaxed),
            http_publish_acknowledged: self.http_publish_acknowledged.load(Ordering::Relaxed),
            grpc_publish_acknowledged: self.grpc_publish_acknowledged.load(Ordering::Relaxed),
            http_publish_failures: self.http_publish_failures.load(Ordering::Relaxed),
            grpc_publish_failures: self.grpc_publish_failures.load(Ordering::Relaxed),
            active_connections_by_protocol: std::array::from_fn(|index| {
                self.active_connections[index].load(Ordering::Relaxed)
            }),
            active_subscriptions_by_protocol: std::array::from_fn(|index| {
                self.active_subscriptions[index].load(Ordering::Relaxed)
            }),
            kafka_assigned_partitions: self.kafka_assigned_partitions.load(Ordering::Relaxed),
            kafka_lag_messages: self.kafka_lag_messages.load(Ordering::Relaxed),
            latency: std::array::from_fn(|index| self.latency[index].snapshot()),
        }
    }
}

/// Serializable metrics snapshot.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    /// Kafka records observed.
    pub kafka_messages: u64,
    /// Kafka payload bytes observed.
    pub kafka_bytes: u64,
    /// Failed Kafka offset commit requests and callbacks.
    pub kafka_commit_errors: u64,
    /// Kafka consumer partition assignment callbacks.
    pub kafka_rebalance_assignments: u64,
    /// Kafka consumer partition revocation callbacks.
    pub kafka_rebalance_revocations: u64,
    /// Kafka consumer rebalance error callbacks.
    pub kafka_rebalance_errors: u64,
    /// Valid decoded messages.
    pub valid_messages: u64,
    /// Invalid decoded messages.
    pub invalid_messages: u64,
    /// Total matching subscriptions.
    pub matched_subscriptions: u64,
    /// Messages with no match.
    pub unmatched_messages: u64,
    /// Successful queue insertions by connection.
    pub delivered_connections: u64,
    /// Queue-full outcomes.
    pub full_connections: u64,
    /// Queue-closed outcomes.
    pub closed_connections: u64,
    /// Connections removed after repeated queue saturation.
    pub slow_consumer_disconnects: u64,
    /// WebSocket connections opened since process start.
    pub websocket_opened: u64,
    /// SSE connections opened since process start.
    pub sse_opened: u64,
    /// gRPC streams opened since process start.
    pub grpc_opened: u64,
    /// Webhook workers registered since process start.
    pub webhook_opened: u64,
    /// Webhook HTTP attempts.
    pub webhook_attempts: u64,
    /// Successful webhook deliveries.
    pub webhook_successes: u64,
    /// Terminal webhook failures.
    pub webhook_failures: u64,
    /// HTTP publish attempts.
    pub http_publish_attempts: u64,
    /// gRPC publish attempts.
    pub grpc_publish_attempts: u64,
    /// Broker-acknowledged HTTP publishes.
    pub http_publish_acknowledged: u64,
    /// Broker-acknowledged gRPC publishes.
    /// Broker-acknowledged initial durable commands.
    pub webhook_durable_commands: u64,
    /// Broker-acknowledged persisted retries.
    pub webhook_retries_scheduled: u64,
    /// Retry commands replayed during startup recovery.
    pub webhook_recovery_replays: u64,
    /// Broker-acknowledged dead-letter commands.
    pub webhook_dead_letters: u64,
    /// Broker-acknowledged gRPC publishes.
    pub grpc_publish_acknowledged: u64,
    /// Rejected or failed HTTP publishes.
    pub http_publish_failures: u64,
    /// Rejected or failed gRPC publishes.
    pub grpc_publish_failures: u64,
    /// Active connections indexed as WebSocket, SSE, gRPC, and HTTP webhook.
    pub active_connections_by_protocol: [u64; 4],
    /// Active subscriptions indexed as WebSocket, SSE, gRPC, and HTTP webhook.
    pub active_subscriptions_by_protocol: [u64; 4],
    /// Current partitions assigned to the source consumer.
    pub kafka_assigned_partitions: u64,
    /// Current aggregate source-consumer lag in messages.
    pub kafka_lag_messages: u64,
    /// Histograms indexed by the fixed `LatencyStage` order.
    pub latency: [HistogramSnapshot; 7],
}

fn protocol_index(protocol: crate::DeliveryProtocol) -> usize {
    match protocol {
        crate::DeliveryProtocol::WebSocket => 0,
        crate::DeliveryProtocol::Sse => 1,
        crate::DeliveryProtocol::Grpc => 2,
        crate::DeliveryProtocol::HttpWebhook => 3,
    }
}

fn decrement(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

const PROTOCOLS: [&str; 4] = ["websocket", "sse", "grpc", "http_webhook"];

/// Renders metrics in Prometheus/OpenMetrics-compatible text format.
#[allow(clippy::too_many_lines)]
pub fn render_prometheus(
    metrics: &MetricsSnapshot,
    active_connections: usize,
    subscriptions: usize,
) -> String {
    let mut output = format!(
        concat!(
            "# TYPE router_kafka_messages_total counter\n",
            "router_kafka_messages_total {}\n",
            "# TYPE router_kafka_bytes_total counter\n",
            "router_kafka_bytes_total {}\n",
            "# TYPE router_kafka_commit_errors_total counter\n",
            "router_kafka_commit_errors_total {}\n",
            "# TYPE router_kafka_rebalances_total counter\n",
            "router_kafka_rebalances_total{{event=\"assignment\"}} {}\n",
            "router_kafka_rebalances_total{{event=\"revocation\"}} {}\n",
            "router_kafka_rebalances_total{{event=\"error\"}} {}\n",
            "# TYPE router_messages_valid_total counter\n",
            "router_messages_valid_total {}\n",
            "# TYPE router_messages_invalid_total counter\n",
            "router_messages_invalid_total {}\n",
            "# TYPE router_messages_unmatched_total counter\n",
            "router_messages_unmatched_total {}\n",
            "# TYPE router_matched_subscriptions_total counter\n",
            "router_matched_subscriptions_total {}\n",
            "# TYPE router_delivered_connections_total counter\n",
            "router_delivered_connections_total {}\n",
            "# TYPE router_full_connections_total counter\n",
            "router_full_connections_total {}\n",
            "# TYPE router_closed_connections_total counter\n",
            "router_closed_connections_total {}\n",
            "# TYPE router_slow_consumer_disconnects_total counter\n",
            "router_slow_consumer_disconnects_total {}\n",
            "# TYPE router_connections gauge\n",
            "router_connections {}\n",
            "# TYPE router_subscriptions gauge\n",
            "router_subscriptions {}\n",
            "# TYPE router_protocol_connections_opened_total counter\n",
            "router_protocol_connections_opened_total{{protocol=\"websocket\"}} {}\n",
            "router_protocol_connections_opened_total{{protocol=\"sse\"}} {}\n",
            "router_protocol_connections_opened_total{{protocol=\"grpc\"}} {}\n",
            "router_protocol_connections_opened_total{{protocol=\"http_webhook\"}} {}\n",
            "# TYPE router_webhook_attempts_total counter\n",
            "router_webhook_attempts_total {}\n",
            "# TYPE router_webhook_successes_total counter\n",
            "router_webhook_successes_total {}\n",
            "# TYPE router_webhook_failures_total counter\n",
            "router_webhook_failures_total {}\n",
            "# TYPE router_webhook_durable_commands_total counter\n",
            "router_webhook_durable_commands_total {}\n",
            "# TYPE router_webhook_retries_scheduled_total counter\n",
            "router_webhook_retries_scheduled_total {}\n",
            "# TYPE router_webhook_recovery_replays_total counter\n",
            "router_webhook_recovery_replays_total {}\n",
            "# TYPE router_webhook_dead_letters_total counter\n",
            "router_webhook_dead_letters_total {}\n",
            "# TYPE router_publish_attempts_total counter\n",
            "router_publish_attempts_total{{protocol=\"http\"}} {}\n",
            "router_publish_attempts_total{{protocol=\"grpc\"}} {}\n",
            "# TYPE router_publish_acknowledged_total counter\n",
            "router_publish_acknowledged_total{{protocol=\"http\"}} {}\n",
            "router_publish_acknowledged_total{{protocol=\"grpc\"}} {}\n",
            "# TYPE router_publish_failures_total counter\n",
            "router_publish_failures_total{{protocol=\"http\"}} {}\n",
            "router_publish_failures_total{{protocol=\"grpc\"}} {}\n"
        ),
        metrics.kafka_messages,
        metrics.kafka_bytes,
        metrics.kafka_commit_errors,
        metrics.kafka_rebalance_assignments,
        metrics.kafka_rebalance_revocations,
        metrics.kafka_rebalance_errors,
        metrics.valid_messages,
        metrics.invalid_messages,
        metrics.unmatched_messages,
        metrics.matched_subscriptions,
        metrics.delivered_connections,
        metrics.full_connections,
        metrics.closed_connections,
        metrics.slow_consumer_disconnects,
        active_connections,
        subscriptions,
        metrics.websocket_opened,
        metrics.sse_opened,
        metrics.grpc_opened,
        metrics.webhook_opened,
        metrics.webhook_attempts,
        metrics.webhook_successes,
        metrics.webhook_failures,
        metrics.webhook_durable_commands,
        metrics.webhook_retries_scheduled,
        metrics.webhook_recovery_replays,
        metrics.webhook_dead_letters,
        metrics.http_publish_attempts,
        metrics.grpc_publish_attempts,
        metrics.http_publish_acknowledged,
        metrics.grpc_publish_acknowledged,
        metrics.http_publish_failures,
        metrics.grpc_publish_failures,
    );

    writeln!(
        output,
        "# TYPE router_kafka_assigned_partitions gauge
router_kafka_assigned_partitions {}",
        metrics.kafka_assigned_partitions
    )
    .expect("writing to String cannot fail");
    writeln!(
        output,
        "# TYPE router_kafka_lag_messages gauge
router_kafka_lag_messages {}",
        metrics.kafka_lag_messages
    )
    .expect("writing to String cannot fail");

    output.push_str(
        "# TYPE router_active_connections gauge
",
    );
    output.push_str(
        "# TYPE router_active_subscriptions gauge
",
    );
    for (index, protocol) in PROTOCOLS.iter().enumerate() {
        writeln!(
            output,
            r#"router_active_connections{{protocol="{protocol}"}} {}"#,
            metrics.active_connections_by_protocol[index]
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            r#"router_active_subscriptions{{protocol="{protocol}"}} {}"#,
            metrics.active_subscriptions_by_protocol[index]
        )
        .expect("writing to String cannot fail");
    }

    output.push_str(
        "# TYPE router_operation_latency_seconds histogram
",
    );
    for stage in LatencyStage::ALL {
        let histogram = metrics.latency[stage.index()];
        let mut cumulative = 0u64;
        for (index, upper) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
            cumulative = cumulative.saturating_add(histogram.buckets[index]);
            writeln!(
                output,
                r#"router_operation_latency_seconds_bucket{{stage="{}",le="{}"}} {}"#,
                stage.label(),
                upper,
                cumulative
            )
            .expect("writing to String cannot fail");
        }
        writeln!(
            output,
            r#"router_operation_latency_seconds_bucket{{stage="{}",le="+Inf"}} {}"#,
            stage.label(),
            histogram.count
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            r#"router_operation_latency_seconds_sum{{stage="{}"}} {}.{:06}"#,
            stage.label(),
            histogram.sum_micros / 1_000_000,
            histogram.sum_micros % 1_000_000
        )
        .expect("writing to String cannot fail");
        writeln!(
            output,
            r#"router_operation_latency_seconds_count{{stage="{}"}} {}"#,
            stage.label(),
            histogram.count
        )
        .expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::DeliveryProtocol;

    use super::{render_prometheus, LatencyStage, Metrics, PublishProtocol};

    #[test]
    fn renders_kafka_commit_and_rebalance_counters() {
        let metrics = Metrics::default();
        metrics.record_kafka_commit_error();
        metrics.record_kafka_rebalance_assignment();
        metrics.record_kafka_rebalance_revocation();
        metrics.record_kafka_rebalance_error();
        metrics.record_publish_attempt(PublishProtocol::Http);
        metrics.record_publish_acknowledged(PublishProtocol::Http);
        metrics.record_publish_attempt(PublishProtocol::Grpc);
        metrics.record_publish_failure(PublishProtocol::Grpc);

        let rendered = render_prometheus(&metrics.snapshot(), 0, 0);
        assert!(rendered.contains("router_kafka_commit_errors_total 1\n"));
        assert!(rendered.contains("router_kafka_rebalances_total{event=\"assignment\"} 1\n"));
        assert!(rendered.contains("router_kafka_rebalances_total{event=\"revocation\"} 1\n"));
        assert!(rendered.contains("router_kafka_rebalances_total{event=\"error\"} 1\n"));
        assert!(rendered.contains("router_publish_attempts_total{protocol=\"http\"} 1\n"));
        assert!(rendered.contains("router_publish_attempts_total{protocol=\"grpc\"} 1\n"));
        assert!(rendered.contains("router_publish_acknowledged_total{protocol=\"http\"} 1\n"));
        assert!(rendered.contains("router_publish_failures_total{protocol=\"grpc\"} 1\n"));
    }
    #[test]
    fn latency_histogram_names_and_labels_are_fixed() {
        let metrics = Metrics::default();
        for stage in LatencyStage::ALL {
            metrics.record_latency(stage, Duration::from_micros(250));
        }

        let rendered = render_prometheus(&metrics.snapshot(), 0, 0);
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with("router_operation_latency_seconds_count"))
                .count(),
            LatencyStage::ALL.len()
        );
        for label in [
            "decode",
            "match",
            "enqueue",
            "protocol_write",
            "webhook_attempt",
            "publish",
            "end_to_end",
        ] {
            assert!(rendered.contains(&format!(r#"stage="{label}""#)));
        }
    }

    #[test]
    fn protocol_gauges_have_only_the_four_bounded_labels() {
        let metrics = Metrics::default();
        for protocol in [
            DeliveryProtocol::WebSocket,
            DeliveryProtocol::Sse,
            DeliveryProtocol::Grpc,
            DeliveryProtocol::HttpWebhook,
        ] {
            metrics.record_protocol_opened(protocol);
            metrics.record_subscription_added(protocol);
        }

        let rendered = render_prometheus(&metrics.snapshot(), 4, 4);
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with("router_active_connections{"))
                .count(),
            4
        );
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with("router_active_subscriptions{"))
                .count(),
            4
        );
        for forbidden in ["tenant", "message_id", "authorization", "payload", "url"] {
            assert!(!rendered.contains(forbidden));
        }
    }
}
