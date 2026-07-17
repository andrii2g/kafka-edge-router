//! Lock-free counters and Prometheus text rendering.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide router counters.
#[derive(Debug, Default)]
pub struct Metrics {
    kafka_messages: AtomicU64,
    kafka_bytes: AtomicU64,
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
}

impl Metrics {
    /// Records one Kafka record and its payload size.
    pub fn record_kafka_message(&self, bytes: usize) {
        self.kafka_messages.fetch_add(1, Ordering::Relaxed);
        self.kafka_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
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

    /// Captures a consistent-enough relaxed snapshot for status and metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            kafka_messages: self.kafka_messages.load(Ordering::Relaxed),
            kafka_bytes: self.kafka_bytes.load(Ordering::Relaxed),
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
}

/// Renders metrics in Prometheus/OpenMetrics-compatible text format.
pub fn render_prometheus(
    metrics: MetricsSnapshot,
    active_connections: usize,
    subscriptions: usize,
) -> String {
    format!(
        concat!(
            "# TYPE router_kafka_messages_total counter\n",
            "router_kafka_messages_total {}\n",
            "# TYPE router_kafka_bytes_total counter\n",
            "router_kafka_bytes_total {}\n",
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
            "router_webhook_failures_total {}\n"
        ),
        metrics.kafka_messages,
        metrics.kafka_bytes,
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
    )
}
