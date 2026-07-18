//! Long-running Kafka consumer loop with explicit offset commits.

use std::sync::Arc;

use async_trait::async_trait;
use rdkafka::{
    client::ClientContext,
    consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    topic_partition_list::TopicPartitionList,
    ClientConfig, Message,
};
use router_core::{Metrics, RoutedMessage, Router};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::{decode_message, KafkaConsumerConfig};
/// A durable side effect that must be acknowledged before a source offset is committed.
#[async_trait]
pub trait PreCommitSink: Send + Sync {
    /// Persists all side effects for one validated source message.
    async fn persist(&self, message: Arc<RoutedMessage>) -> Result<(), String>;
}

/// Kafka ingestion startup failure.
#[derive(Debug, Error)]
pub enum KafkaIngestError {
    /// Consumer creation failed.
    #[error("failed to create Kafka consumer: {0}")]
    Create(#[source] rdkafka::error::KafkaError),
    /// Topic subscription failed.
    #[error("failed to subscribe Kafka consumer: {0}")]
    Subscribe(#[source] rdkafka::error::KafkaError),
}

#[derive(Clone)]
struct RouterConsumerContext {
    metrics: Arc<Metrics>,
}

impl ClientContext for RouterConsumerContext {}

impl ConsumerContext for RouterConsumerContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(_) => self.metrics.record_kafka_rebalance_assignment(),
            Rebalance::Revoke(_) => self.metrics.record_kafka_rebalance_revocation(),
            Rebalance::Error(_) => self.metrics.record_kafka_rebalance_error(),
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        if let Err(commit_error) = result {
            self.metrics.record_kafka_commit_error();
            error!(error = %commit_error, "Kafka offset commit failed");
        }
    }
}

/// Kafka record consumer that routes messages into bounded local queues.
pub struct KafkaIngestor {
    consumer: StreamConsumer<RouterConsumerContext>,
    router: Arc<Router>,
    max_payload_bytes: usize,
    commit_invalid_messages: bool,
    pre_commit_sinks: Vec<Arc<dyn PreCommitSink>>,
}

impl KafkaIngestor {
    /// Creates and subscribes a stream consumer.
    pub fn new(
        config: &KafkaConsumerConfig,
        router: Arc<Router>,
    ) -> Result<Self, KafkaIngestError> {
        Self::with_pre_commit_sinks(config, router, Vec::new())
    }

    /// Creates a consumer with durable sinks that complete before source commit.
    pub fn with_pre_commit_sinks(
        config: &KafkaConsumerConfig,
        router: Arc<Router>,
        pre_commit_sinks: Vec<Arc<dyn PreCommitSink>>,
    ) -> Result<Self, KafkaIngestError> {
        let mut client = ClientConfig::new();
        for (key, value) in &config.properties {
            client.set(key, value);
        }
        // Apply semantic invariants last so free-form librdkafka properties cannot
        // silently re-enable auto commit or replace the configured identity.
        client
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("client.id", &config.client_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", &config.auto_offset_reset);

        let context = RouterConsumerContext {
            metrics: Arc::clone(router.metrics()),
        };
        let consumer: StreamConsumer<RouterConsumerContext> = client
            .create_with_context(context)
            .map_err(KafkaIngestError::Create)?;
        let topics: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topics)
            .map_err(KafkaIngestError::Subscribe)?;

        Ok(Self {
            consumer,
            router,
            max_payload_bytes: config.max_payload_bytes,
            commit_invalid_messages: config.commit_invalid_messages,
            pre_commit_sinks,
        })
    }

    /// Consumes until shutdown. Offset commit means the message was accepted or
    /// intentionally dropped according to local queue policy, not network delivery.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        info!("Kafka consumer started");
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        info!("Kafka consumer stopping");
                        break;
                    }
                }
                result = self.consumer.recv() => {
                    match result {
                        Ok(record) => {
                            let bytes = record.payload().map_or(0, <[u8]>::len);
                            self.router.metrics().record_kafka_message(bytes);
                            match decode_message(&record, self.max_payload_bytes) {
                                Ok(message) => {
                                    let message_id = message.metadata.message_id.to_string();
                                    let message = Arc::new(message);
                                    let report = self.router.dispatch(Arc::clone(&message));
                                    let mut persistence_failed = false;
                                    for sink in &self.pre_commit_sinks {
                                        if let Err(reason) = sink.persist(Arc::clone(&message)).await {
                                            error!(
                                                %message_id,
                                                %reason,
                                                "pre-commit sink failed; stopping before source offset commit"
                                            );
                                            persistence_failed = true;
                                            break;
                                        }
                                    }
                                    if persistence_failed {
                                        break;
                                    }
                                    debug!(
                                        %message_id,
                                        matched_subscriptions = report.matched_subscriptions,
                                        delivered_connections = report.delivered_connections,
                                        full_connections = report.full_connections,
                                        "Kafka message routed"
                                    );
                                    if let Err(commit_error) = self.consumer.commit_message(&record, CommitMode::Async) {
                                        self.router.metrics().record_kafka_commit_error();
                                        error!(error = %commit_error, %message_id, "failed to enqueue Kafka offset commit");
                                    }
                                }
                                Err(decode_error) => {
                                    self.router.metrics().record_invalid_message();
                                    warn!(
                                        error = %decode_error,
                                        topic = record.topic(),
                                        partition = record.partition(),
                                        offset = record.offset(),
                                        "invalid Kafka message"
                                    );
                                    if self.commit_invalid_messages {
                                        if let Err(commit_error) = self.consumer.commit_message(&record, CommitMode::Async) {
                                            self.router.metrics().record_kafka_commit_error();
                                            error!(error = %commit_error, "failed to commit invalid Kafka message");
                                        }
                                    } else {
                                        warn!(
                                            topic = record.topic(),
                                            partition = record.partition(),
                                            offset = record.offset(),
                                            "stopping Kafka consumer so a later commit cannot skip an invalid record"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                        Err(kafka_error) => {
                            error!(error = %kafka_error, "Kafka receive error");
                        }
                    }
                }
            }
        }
    }
}
