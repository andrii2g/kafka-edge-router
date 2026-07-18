//! Kafka-backed webhook commands, retry scheduling, and dead letters.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use http::{HeaderName, HeaderValue, StatusCode};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    ClientConfig, Message,
};
use reqwest::Url;
use router_core::{
    encode_delivery_json, ConnectionId, Delivery, DeliveryProtocol, LatencyStage, Metrics,
    RouteKey, RoutedMessage, Router, SubscriptionId, TraceContext,
};
use router_kafka::PreCommitSink;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{watch, Mutex},
    time::sleep,
};
use tracing::{debug, error, info, info_span, warn, Instrument as _};
use uuid::Uuid;

use crate::{
    manager::{parse_headers, retryable_status, signature},
    pinned_client, validate_destination_port, validate_destination_url, DurableWebhookConfig,
    WebhookConfig, WebhookDestinationConfig, WebhookError,
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecordState {
    Delivery,
    Retry,
    DeadLetter,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum FailureClass {
    Timeout,
    Connect,
    Request,
    Http429,
    Http5xx,
    PermanentHttp,
    Client,
    InvalidRecord,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableRecord {
    schema_version: u16,
    delivery_id: String,
    destination_id: String,
    original_message_id: String,
    body_base64: String,
    attempt: u32,
    next_attempt_at_ms: u64,
    last_error_class: Option<FailureClass>,
    source_topic: Option<String>,
    source_partition: Option<i32>,
    source_offset: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace_context: Option<TraceContext>,
    state: RecordState,
}

#[derive(Clone)]
struct RouteDestination {
    id: String,
    receiver: Arc<Mutex<tokio::sync::mpsc::Receiver<Delivery>>>,
}

pub(crate) struct DurableWebhookSink {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
    max_record_bytes: usize,
    routes: HashMap<RouteKey, Vec<RouteDestination>>,
    metrics: Arc<Metrics>,
    router: Arc<Router>,
    connection_ids: Vec<ConnectionId>,
}

pub(crate) struct DurableWebhookRuntime {
    pub(crate) sink: Arc<DurableWebhookSink>,
    pub(crate) workers: Vec<DurableWorker>,
}

pub(crate) fn build_durable_runtime(
    config: &WebhookConfig,
    router: Arc<Router>,
) -> Result<DurableWebhookRuntime, WebhookError> {
    let metrics = Arc::clone(router.metrics());
    let producer = create_producer(&config.durable)?;
    let mut route_table: HashMap<RouteKey, Vec<RouteDestination>> = HashMap::new();
    let mut workers = Vec::with_capacity(config.destinations.len());
    let mut registrations = RegistrationGuard::new(Arc::clone(&router));
    for destination in &config.destinations {
        let subscription_id = SubscriptionId::new(format!("webhook:{}", destination.id))
            .map_err(|error| WebhookError::Durable(error.to_string()))?;
        let registration = router
            .register_connection(
                &destination.filter.tenant_id,
                DeliveryProtocol::HttpWebhook,
                Some(destination.queue_capacity),
            )
            .map_err(|error| WebhookError::Registration {
                id: destination.id.clone(),
                reason: error.to_string(),
            })?;
        registrations.ids.push(registration.connection_id);
        router
            .subscribe(
                registration.connection_id,
                subscription_id,
                destination.filter.clone(),
            )
            .map_err(|error| WebhookError::Registration {
                id: destination.id.clone(),
                reason: error.to_string(),
            })?;
        route_table
            .entry(RouteKey::from(&destination.filter))
            .or_default()
            .push(RouteDestination {
                id: destination.id.clone(),
                receiver: Arc::new(Mutex::new(registration.receiver)),
            });
        workers.push(DurableWorker::new(
            destination.clone(),
            &config.durable,
            producer.clone(),
            Arc::clone(&metrics),
        )?);
    }
    let connection_ids = std::mem::take(&mut registrations.ids);
    registrations.armed = false;
    Ok(DurableWebhookRuntime {
        sink: Arc::new(DurableWebhookSink {
            producer,
            topic: config.durable.delivery_topic.clone(),
            timeout: Duration::from_millis(config.durable.delivery_timeout_ms),
            max_record_bytes: config.durable.max_record_bytes,
            routes: route_table,
            metrics,
            router,
            connection_ids,
        }),
        workers,
    })
}

#[async_trait]
impl PreCommitSink for DurableWebhookSink {
    async fn persist(&self, message: Arc<RoutedMessage>) -> Result<(), String> {
        let mut destinations = Vec::new();
        for candidate in RouteKey::candidates(&message.metadata) {
            if let Some(matches) = self.routes.get(&candidate) {
                destinations.extend(matches.iter().cloned());
            }
        }
        for destination in destinations {
            let delivery = destination
                .receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| format!("durable route queue closed for {}", destination.id))?;
            if delivery.message.metadata.message_id != message.metadata.message_id {
                return Err(format!(
                    "durable route queue returned message {} while waiting for {}",
                    delivery.message.metadata.message_id, message.metadata.message_id
                ));
            }
            let body = encode_delivery_json(&delivery);
            let source = delivery.message.metadata.source.as_ref();
            let record = DurableRecord {
                schema_version: SCHEMA_VERSION,
                delivery_id: Uuid::new_v4().to_string(),
                destination_id: destination.id,
                original_message_id: message.metadata.message_id.to_string(),
                body_base64: STANDARD.encode(body),
                attempt: 1,
                next_attempt_at_ms: now_ms(),
                last_error_class: None,
                source_topic: source.map(|value| value.topic.to_string()),
                source_partition: source.map(|value| value.partition),
                source_offset: source.map(|value| value.offset),
                trace_context: delivery.message.trace_context().cloned(),
                state: RecordState::Delivery,
            };
            publish_record(
                &self.producer,
                &self.topic,
                &record,
                self.timeout,
                self.max_record_bytes,
            )
            .await?;
            self.metrics.record_webhook_durable_command();
        }
        Ok(())
    }
}

impl Drop for DurableWebhookSink {
    fn drop(&mut self) {
        for connection_id in &self.connection_ids {
            self.router.unregister_connection(*connection_id);
        }
    }
}

struct RegistrationGuard {
    router: Arc<Router>,
    ids: Vec<ConnectionId>,
    armed: bool,
}

impl RegistrationGuard {
    fn new(router: Arc<Router>) -> Self {
        Self {
            router,
            ids: Vec::new(),
            armed: true,
        }
    }
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            for connection_id in &self.ids {
                self.router.unregister_connection(*connection_id);
            }
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestCrashPoint {
    BeforeRequest,
    AfterRemoteSuccess,
    AfterRetryPublish,
    AfterDeadLetterPublish,
}

pub(crate) struct DurableWorker {
    endpoint: DurableEndpoint,
    producer: FutureProducer,
    consumer: StreamConsumer,
    delivery_topic: String,
    retry_topic: String,
    dead_letter_topic: String,
    timeout: Duration,
    max_record_bytes: usize,
    max_recovery_records: usize,
    metrics: Arc<Metrics>,
    #[cfg(test)]
    test_crash_point: Option<TestCrashPoint>,
}

impl DurableWorker {
    fn new(
        destination: WebhookDestinationConfig,
        durable: &DurableWebhookConfig,
        producer: FutureProducer,
        metrics: Arc<Metrics>,
    ) -> Result<Self, WebhookError> {
        let endpoint = DurableEndpoint::new(destination)?;
        let mut client = ClientConfig::new();
        for (key, value) in &durable.properties {
            client.set(key, value);
        }
        client
            .set("bootstrap.servers", &durable.brokers)
            .set("group.id", format!("{}.{}", durable.group_id, endpoint.id))
            .set(
                "client.id",
                format!("{}-{}", durable.client_id, endpoint.id),
            )
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "true")
            .set("partition.assignment.strategy", "range");
        let consumer = client
            .create()
            .map_err(|error| WebhookError::Durable(format!("consumer creation failed: {error}")))?;
        Ok(Self {
            endpoint,
            producer,
            consumer,
            delivery_topic: durable.delivery_topic.clone(),
            retry_topic: durable.retry_topic.clone(),
            dead_letter_topic: durable.dead_letter_topic.clone(),
            timeout: Duration::from_millis(durable.delivery_timeout_ms),
            max_record_bytes: durable.max_record_bytes,
            max_recovery_records: durable.max_recovery_records,
            metrics,
            #[cfg(test)]
            test_crash_point: None,
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.endpoint.id
    }

    #[cfg(test)]
    fn crash_at(mut self, point: TestCrashPoint) -> Self {
        self.test_crash_point = Some(point);
        self
    }

    #[cfg(test)]
    fn inject_crash(&self, point: TestCrashPoint) -> Result<(), String> {
        if self.test_crash_point == Some(point) {
            Err(format!("injected crash at {point:?}"))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), String> {
        info!(webhook_id = %self.endpoint.id, "durable webhook worker started");
        let mut completed = HashSet::new();
        self.recover_retries(&mut completed, &mut shutdown).await?;
        self.consumer
            .subscribe(&[&self.delivery_topic, &self.retry_topic])
            .map_err(|error| format!("durable topic subscription failed: {error}"))?;
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                result = self.consumer.recv() => match result {
                    Ok(message) => {
                        let input = InputRecord::from_message(&message, self.max_record_bytes)?;
                        if input.record.destination_id != self.endpoint.id
                            || completed.contains(&input.record.delivery_id)
                        {
                            commit_cursor(&self.consumer, &input.cursor)?;
                        } else {
                            self.process(input, &mut completed, &mut shutdown).await?;
                        }
                    }
                    Err(KafkaError::PartitionEOF(_)) => {}
                    Err(error) => warn!(
                        webhook_id = %self.endpoint.id,
                        %error,
                        "durable webhook consumer receive error"
                    ),
                }
            }
        }
        info!(webhook_id = %self.endpoint.id, "durable webhook worker stopped");
        Ok(())
    }

    async fn recover_retries(
        &self,
        completed: &mut HashSet<String>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), String> {
        self.consumer
            .subscribe(&[&self.retry_topic])
            .map_err(|error| format!("retry recovery subscription failed: {error}"))?;
        let mut eof_partitions = HashSet::new();
        let mut recovered = 0usize;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                result = self.consumer.recv() => match result {
                    Ok(message) => {
                        eof_partitions.remove(&message.partition());
                        let input = InputRecord::from_message(&message, self.max_record_bytes)?;
                        if input.record.destination_id != self.endpoint.id {
                            commit_cursor(&self.consumer, &input.cursor)?;
                            continue;
                        }
                        recovered = recovered.saturating_add(1);
                        self.metrics.record_webhook_recovery_replay();
                        if recovered > self.max_recovery_records {
                            return Err(format!(
                                "retry recovery exceeded configured limit {}",
                                self.max_recovery_records
                            ));
                        }
                        if completed.contains(&input.record.delivery_id) {
                            commit_cursor(&self.consumer, &input.cursor)?;
                        } else {
                            self.process(input, completed, shutdown).await?;
                        }
                    }
                    Err(KafkaError::PartitionEOF(partition)) => {
                        eof_partitions.insert(partition);
                        let assigned = self.consumer.assignment()
                            .map_err(|error| format!("failed to inspect retry assignment: {error}"))?
                            .elements_for_topic(&self.retry_topic)
                            .len();
                        if assigned > 0 && eof_partitions.len() >= assigned {
                            debug!(
                                webhook_id = %self.endpoint.id,
                                recovered,
                                "durable retry recovery reached high watermark"
                            );
                            return Ok(());
                        }
                    }
                    Err(error) => return Err(format!("retry recovery receive failed: {error}")),
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn process(
        &self,
        input: InputRecord,
        completed: &mut HashSet<String>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), String> {
        let mut record = input.record;
        let body = STANDARD
            .decode(&record.body_base64)
            .map_err(|error| format!("invalid durable webhook body: {error}"))?;
        let mut input_committed = false;
        loop {
            #[cfg(test)]
            self.inject_crash(TestCrashPoint::BeforeRequest)?;
            sleep_until(record.next_attempt_at_ms, shutdown).await?;
            self.metrics.record_webhook_attempt();
            let started = Instant::now();
            let span = info_span!(
                "webhook.attempt",
                message_id = %record.original_message_id,
                attempt = record.attempt,
            );
            if let Some(trace_context) = &record.trace_context {
                trace_context.set_span_parent(&span);
            }
            let outcome = self
                .endpoint
                .send_attempt(&body, &record.original_message_id, record.attempt)
                .instrument(span)
                .await;
            self.metrics
                .record_latency(LatencyStage::WebhookAttempt, started.elapsed());
            match outcome {
                AttemptOutcome::Delivered(status) => {
                    self.metrics.record_webhook_success();
                    debug!(
                        webhook_id = %self.endpoint.id,
                        message_id = %record.original_message_id,
                        %status,
                        attempt = record.attempt,
                        "durable webhook delivered"
                    );
                    #[cfg(test)]
                    self.inject_crash(TestCrashPoint::AfterRemoteSuccess)?;
                    if !input_committed {
                        commit_cursor(&self.consumer, &input.cursor)?;
                    }
                    remember_completed(completed, record.delivery_id, self.max_recovery_records)?;
                    return Ok(());
                }
                AttemptOutcome::Failed(class)
                    if should_retry(class) && record.attempt < self.endpoint.max_attempts =>
                {
                    record.attempt = record.attempt.saturating_add(1);
                    record.next_attempt_at_ms =
                        now_ms().saturating_add(self.endpoint.retry_delay_ms(record.attempt));
                    record.last_error_class = Some(class);
                    record.state = RecordState::Retry;
                    publish_record(
                        &self.producer,
                        &self.retry_topic,
                        &record,
                        self.timeout,
                        self.max_record_bytes,
                    )
                    .await?;
                    self.metrics.record_webhook_retry_scheduled();
                    #[cfg(test)]
                    self.inject_crash(TestCrashPoint::AfterRetryPublish)?;
                    if !input_committed {
                        commit_cursor(&self.consumer, &input.cursor)?;
                        input_committed = true;
                    }
                    warn!(
                        webhook_id = %self.endpoint.id,
                        message_id = %record.original_message_id,
                        attempt = record.attempt,
                        next_attempt_at_ms = record.next_attempt_at_ms,
                        error_class = ?class,
                        "durable webhook retry persisted"
                    );
                }
                AttemptOutcome::Failed(class) => {
                    record.last_error_class = Some(class);
                    record.state = RecordState::DeadLetter;
                    publish_record(
                        &self.producer,
                        &self.dead_letter_topic,
                        &record,
                        self.timeout,
                        self.max_record_bytes,
                    )
                    .await?;
                    self.metrics.record_webhook_dead_letter();
                    self.metrics.record_webhook_failure();
                    #[cfg(test)]
                    self.inject_crash(TestCrashPoint::AfterDeadLetterPublish)?;
                    if !input_committed {
                        commit_cursor(&self.consumer, &input.cursor)?;
                    }
                    remember_completed(completed, record.delivery_id, self.max_recovery_records)?;
                    error!(
                        webhook_id = %self.endpoint.id,
                        message_id = %record.original_message_id,
                        attempt = record.attempt,
                        error_class = ?class,
                        "durable webhook moved to dead letter"
                    );
                    return Ok(());
                }
            }
        }
    }
}

struct DurableEndpoint {
    id: String,
    url: Url,
    allow_private_ips: bool,
    timeout: Duration,
    headers: Vec<(HeaderName, HeaderValue)>,
    signing_secret: Option<Vec<u8>>,
    max_attempts: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl DurableEndpoint {
    fn new(config: WebhookDestinationConfig) -> Result<Self, WebhookError> {
        let url = validate_destination_url(
            &config.url,
            &config.allowed_hosts,
            config.allow_private_ips,
            config.allow_http,
        )?;
        validate_destination_port(&url, &config.allowed_ports)?;
        Ok(Self {
            id: config.id,
            url,
            allow_private_ips: config.allow_private_ips,
            timeout: Duration::from_millis(config.timeout_ms.max(1)),
            headers: parse_headers(&config.headers)?,
            signing_secret: config.signing_secret.map(String::into_bytes),
            max_attempts: config.max_attempts,
            initial_backoff_ms: config.initial_backoff_ms,
            max_backoff_ms: config.max_backoff_ms,
        })
    }

    async fn send_attempt(&self, body: &[u8], message_id: &str, attempt: u32) -> AttemptOutcome {
        let Ok(client) = pinned_client(&self.url, self.allow_private_ips, self.timeout).await
        else {
            return AttemptOutcome::Failed(FailureClass::Connect);
        };
        let mut request = client
            .post(self.url.clone())
            .header("content-type", "application/json")
            .header(
                "user-agent",
                concat!("kafka-edge-router/", env!("CARGO_PKG_VERSION")),
            )
            .header("x-router-message-id", message_id)
            .header("idempotency-key", message_id)
            .header("x-router-attempt", attempt.to_string())
            .body(Bytes::copy_from_slice(body));
        for (name, value) in &self.headers {
            request = request.header(name.clone(), value.clone());
        }
        if let Some(secret) = &self.signing_secret {
            request = request.header("x-router-signature", signature(secret, body));
        }
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                AttemptOutcome::Delivered(response.status())
            }
            Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
                AttemptOutcome::Failed(FailureClass::Http429)
            }
            Ok(response) if retryable_status(response.status()) => {
                AttemptOutcome::Failed(FailureClass::Http5xx)
            }
            Ok(_) => AttemptOutcome::Failed(FailureClass::PermanentHttp),
            Err(error) if error.is_timeout() => AttemptOutcome::Failed(FailureClass::Timeout),
            Err(error) if error.is_connect() => AttemptOutcome::Failed(FailureClass::Connect),
            Err(error) if error.is_request() => AttemptOutcome::Failed(FailureClass::Request),
            Err(_) => AttemptOutcome::Failed(FailureClass::Client),
        }
    }

    fn retry_delay_ms(&self, next_attempt: u32) -> u64 {
        let mut delay = self.initial_backoff_ms;
        for _ in 0..next_attempt.saturating_sub(2) {
            delay = delay.saturating_mul(2).min(self.max_backoff_ms);
        }
        delay.min(self.max_backoff_ms)
    }
}

enum AttemptOutcome {
    Delivered(StatusCode),
    Failed(FailureClass),
}

struct InputRecord {
    record: DurableRecord,
    cursor: RecordCursor,
}

impl InputRecord {
    fn from_message(
        message: &rdkafka::message::BorrowedMessage<'_>,
        max_record_bytes: usize,
    ) -> Result<Self, String> {
        let payload = message
            .payload()
            .ok_or_else(|| "durable webhook record has no payload".to_owned())?;
        if payload.len() > max_record_bytes {
            return Err(format!(
                "durable webhook record is {} bytes, limit is {}",
                payload.len(),
                max_record_bytes
            ));
        }
        let record: DurableRecord = serde_json::from_slice(payload)
            .map_err(|error| format!("invalid durable webhook record: {error}"))?;
        if record.schema_version != SCHEMA_VERSION {
            return Err(format!(
                "unsupported durable webhook schema version {}",
                record.schema_version
            ));
        }
        Ok(Self {
            record,
            cursor: RecordCursor {
                topic: message.topic().to_owned(),
                partition: message.partition(),
                offset: message.offset(),
            },
        })
    }
}

struct RecordCursor {
    topic: String,
    partition: i32,
    offset: i64,
}

fn create_producer(config: &DurableWebhookConfig) -> Result<FutureProducer, WebhookError> {
    let mut client = ClientConfig::new();
    for (key, value) in &config.properties {
        client.set(key, value);
    }
    client
        .set("bootstrap.servers", &config.brokers)
        .set("client.id", &config.client_id)
        .set("message.timeout.ms", config.delivery_timeout_ms.to_string())
        .set("enable.idempotence", "true")
        .set("acks", "all");
    client
        .create()
        .map_err(|error| WebhookError::Durable(format!("producer creation failed: {error}")))
}

async fn publish_record(
    producer: &FutureProducer,
    topic: &str,
    record: &DurableRecord,
    timeout: Duration,
    max_record_bytes: usize,
) -> Result<(), String> {
    let payload = serde_json::to_vec(record)
        .map_err(|error| format!("record serialization failed: {error}"))?;
    if payload.len() > max_record_bytes {
        return Err(format!(
            "serialized durable webhook record is {} bytes, limit is {}",
            payload.len(),
            max_record_bytes
        ));
    }
    producer
        .send(
            FutureRecord::to(topic)
                .key(&record.destination_id)
                .payload(&payload),
            rdkafka::util::Timeout::After(timeout),
        )
        .await
        .map_err(|(error, _)| format!("Kafka publication to {topic} failed: {error}"))?;
    Ok(())
}

fn commit_cursor(consumer: &StreamConsumer, cursor: &RecordCursor) -> Result<(), String> {
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset(
            &cursor.topic,
            cursor.partition,
            Offset::Offset(cursor.offset.saturating_add(1)),
        )
        .map_err(|error| format!("failed to build webhook commit offset: {error}"))?;
    consumer
        .commit(&offsets, CommitMode::Sync)
        .map_err(|error| format!("durable webhook offset commit failed: {error}"))
}

async fn sleep_until(
    next_attempt_at_ms: u64,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let delay = Duration::from_millis(next_attempt_at_ms.saturating_sub(now_ms()));
    if delay.is_zero() {
        return Ok(());
    }
    tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                Err("shutdown interrupted durable retry wait".to_owned())
            } else {
                Ok(())
            }
        }
        () = sleep(delay) => Ok(()),
    }
}

fn should_retry(class: FailureClass) -> bool {
    matches!(
        class,
        FailureClass::Timeout
            | FailureClass::Connect
            | FailureClass::Request
            | FailureClass::Http429
            | FailureClass::Http5xx
    )
}

fn remember_completed(
    completed: &mut HashSet<String>,
    delivery_id: String,
    limit: usize,
) -> Result<(), String> {
    if completed.len() >= limit && !completed.contains(&delivery_id) {
        return Err(format!(
            "completed retry set exceeded configured limit {limit}"
        ));
    }
    completed.insert(delivery_id);
    Ok(())
}

fn now_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{should_retry, DurableEndpoint, FailureClass};

    #[test]
    fn retry_classes_are_explicit() {
        assert!(should_retry(FailureClass::Timeout));
        assert!(should_retry(FailureClass::Http429));
        assert!(!should_retry(FailureClass::PermanentHttp));
        assert!(!should_retry(FailureClass::InvalidRecord));
    }

    #[test]
    fn exponential_backoff_is_capped() {
        let endpoint = DurableEndpoint {
            id: "test".to_owned(),
            url: "https://example.com".parse().expect("URL"),
            allow_private_ips: false,
            timeout: Duration::from_secs(1),
            headers: Vec::new(),
            signing_secret: None,
            max_attempts: 8,
            initial_backoff_ms: 100,
            max_backoff_ms: 350,
        };
        assert_eq!(endpoint.retry_delay_ms(2), 100);
        assert_eq!(endpoint.retry_delay_ms(3), 200);
        assert_eq!(endpoint.retry_delay_ms(4), 350);
        assert_eq!(endpoint.retry_delay_ms(8), 350);
    }
}
#[cfg(test)]
mod kafka_tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use bytes::Bytes;
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        client::DefaultClientContext,
    };
    use router_core::{KafkaPosition, RouteFilter, RoutedMessage, Router, RoutingMetadata};
    use router_kafka::PreCommitSink;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{mpsc, watch},
        time::{sleep, timeout},
    };
    use uuid::Uuid;

    use super::{build_durable_runtime, TestCrashPoint};
    use crate::{
        DurableWebhookConfig, WebhookConfig, WebhookDeliveryMode, WebhookDestinationConfig,
    };

    #[tokio::test]
    async fn persisted_retry_survives_worker_restart() {
        let Some(brokers) = required_brokers() else {
            return;
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let delivery = format!("router.webhook.delivery.{suffix}");
        let retry = format!("router.webhook.retry.{suffix}");
        let dead = format!("router.webhook.dead-letter.{suffix}");
        create_topics(&brokers, [&delivery, &retry, &dead]).await;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("receiver bind");
        let address = listener.local_addr().expect("receiver address");
        let (requests_tx, mut requests_rx) = mpsc::channel(2);
        let receiver = tokio::spawn(async move {
            for status in ["503 Service Unavailable", "200 OK"] {
                let (mut socket, _) = listener.accept().await.expect("receiver accept");
                let mut bytes = vec![0; 8192];
                let read = socket.read(&mut bytes).await.expect("receiver read");
                let request = String::from_utf8_lossy(&bytes[..read]).into_owned();
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("receiver response");
                requests_tx
                    .send(request)
                    .await
                    .expect("request observation");
            }
        });

        let config = webhook_config(
            &brokers,
            &suffix,
            &delivery,
            &retry,
            &dead,
            &format!("http://{address}/hook"),
        );
        let router = Arc::new(Router::new(router_core::RouterConfig::default()));
        let metrics = Arc::clone(router.metrics());
        let mut first = build_durable_runtime(&config, Arc::clone(&router)).expect("runtime");
        let routed_message = message();
        router.dispatch(Arc::clone(&routed_message));
        first
            .sink
            .persist(routed_message)
            .await
            .expect("initial command persisted");
        let first_worker = first
            .workers
            .remove(0)
            .crash_at(TestCrashPoint::AfterRetryPublish);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let first_task = tokio::spawn(first_worker.run(shutdown_rx));

        let first_request = timeout(Duration::from_secs(30), requests_rx.recv())
            .await
            .expect("first request timeout")
            .expect("first request");
        wait_for_counter(|| metrics.snapshot().webhook_retries_scheduled).await;
        let first_result = timeout(Duration::from_secs(10), first_task)
            .await
            .expect("first worker stop")
            .expect("first worker join");
        assert!(first_result
            .expect_err("worker must stop after persisted retry")
            .contains("AfterRetryPublish"));
        drop(first);

        let mut second = build_durable_runtime(&config, Arc::clone(&router)).expect("restart");
        let second_worker = second.workers.remove(0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let second_task = tokio::spawn(second_worker.run(shutdown_rx));
        let second_request = timeout(Duration::from_secs(30), requests_rx.recv())
            .await
            .expect("recovery request timeout")
            .expect("recovery request");
        wait_for_counter(|| metrics.snapshot().webhook_successes).await;
        shutdown_tx.send(true).expect("second shutdown");
        let _ = timeout(Duration::from_secs(10), second_task)
            .await
            .expect("second worker stop");
        receiver.await.expect("receiver task");

        assert!(first_request.contains("idempotency-key: durable-message-1"));
        assert!(second_request.contains("idempotency-key: durable-message-1"));
        assert_eq!(metrics.snapshot().webhook_dead_letters, 0);
        assert!(metrics.snapshot().webhook_recovery_replays >= 1);
    }

    #[tokio::test]
    async fn crash_boundaries_redeliver_without_losing_acknowledged_state() {
        let Some(brokers) = required_brokers() else {
            return;
        };

        exercise_crash_boundary(
            &brokers,
            TestCrashPoint::BeforeRequest,
            &["200 OK"],
            3,
            1,
            0,
        )
        .await;
        exercise_crash_boundary(
            &brokers,
            TestCrashPoint::AfterRemoteSuccess,
            &["200 OK", "200 OK"],
            3,
            2,
            0,
        )
        .await;
        exercise_crash_boundary(
            &brokers,
            TestCrashPoint::AfterDeadLetterPublish,
            &["400 Bad Request", "400 Bad Request"],
            1,
            0,
            2,
        )
        .await;
    }

    async fn exercise_crash_boundary(
        brokers: &str,
        crash_point: TestCrashPoint,
        statuses: &[&'static str],
        max_attempts: u32,
        expected_successes: u64,
        expected_dead_letters: u64,
    ) {
        let suffix = Uuid::new_v4().simple().to_string();
        let delivery = format!("router.webhook.delivery.{suffix}");
        let retry = format!("router.webhook.retry.{suffix}");
        let dead = format!("router.webhook.dead-letter.{suffix}");
        create_topics(brokers, [&delivery, &retry, &dead]).await;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("receiver bind");
        let address = listener.local_addr().expect("receiver address");
        let expected_request_count = statuses.len();
        let statuses = statuses.to_vec();
        let receiver = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(statuses.len());
            for status in statuses {
                let (mut socket, _) = listener.accept().await.expect("receiver accept");
                let mut bytes = vec![0; 8192];
                let read = socket.read(&mut bytes).await.expect("receiver read");
                requests.push(String::from_utf8_lossy(&bytes[..read]).into_owned());
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("receiver response");
            }
            requests
        });

        let mut config = webhook_config(
            brokers,
            &suffix,
            &delivery,
            &retry,
            &dead,
            &format!("http://{address}/hook"),
        );
        config.destinations[0].max_attempts = max_attempts;
        let router = Arc::new(Router::new(router_core::RouterConfig::default()));
        let metrics = Arc::clone(router.metrics());
        let mut first = build_durable_runtime(&config, Arc::clone(&router)).expect("runtime");
        let routed_message = message();
        router.dispatch(Arc::clone(&routed_message));
        first
            .sink
            .persist(routed_message)
            .await
            .expect("initial command persisted");
        let first_worker = first.workers.remove(0).crash_at(crash_point);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let first_result = timeout(
            Duration::from_secs(30),
            tokio::spawn(first_worker.run(shutdown_rx)),
        )
        .await
        .expect("crash timeout")
        .expect("crash task join");
        assert!(first_result
            .expect_err("worker must stop at injected crash")
            .contains("injected crash"));
        drop(first);

        let mut second = build_durable_runtime(&config, Arc::clone(&router)).expect("restart");
        let second_worker = second.workers.remove(0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let second_task = tokio::spawn(second_worker.run(shutdown_rx));
        let requests = timeout(Duration::from_secs(30), receiver)
            .await
            .expect("receiver timeout")
            .expect("receiver task");
        wait_for_value(|| metrics.snapshot().webhook_successes, expected_successes).await;
        wait_for_value(
            || metrics.snapshot().webhook_dead_letters,
            expected_dead_letters,
        )
        .await;
        shutdown_tx.send(true).expect("second shutdown");
        timeout(Duration::from_secs(10), second_task)
            .await
            .expect("second worker stop")
            .expect("second worker join")
            .expect("second worker result");

        assert_eq!(requests.len(), expected_request_count);
        assert!(requests
            .iter()
            .all(|request| request.contains("idempotency-key: durable-message-1")));
    }

    fn required_brokers() -> Option<String> {
        let brokers = std::env::var("KAFKA_TEST_BROKERS").ok()?;
        if std::env::var("KAFKA_INTEGRATION_REQUIRED").as_deref() == Ok("1") {
            Some(brokers)
        } else {
            None
        }
    }

    async fn create_topics(brokers: &str, topics: [&str; 3]) {
        let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("admin client");
        let definitions: Vec<_> = topics
            .iter()
            .map(|topic| NewTopic::new(topic, 1, TopicReplication::Fixed(1)))
            .collect();
        let results = admin
            .create_topics(&definitions, &AdminOptions::new())
            .await
            .expect("topic request");
        assert!(results.iter().all(Result::is_ok), "{results:?}");
    }

    fn webhook_config(
        brokers: &str,
        suffix: &str,
        delivery: &str,
        retry: &str,
        dead: &str,
        url: &str,
    ) -> WebhookConfig {
        WebhookConfig {
            enabled: true,
            mode: WebhookDeliveryMode::Durable,
            durable: DurableWebhookConfig {
                brokers: brokers.to_owned(),
                client_id: format!("webhook-test-{suffix}"),
                group_id: format!("webhook-test-{suffix}"),
                delivery_topic: delivery.to_owned(),
                retry_topic: retry.to_owned(),
                dead_letter_topic: dead.to_owned(),
                delivery_timeout_ms: 10_000,
                max_record_bytes: 1_048_576,
                max_recovery_records: 100,
                properties: BTreeMap::new(),
            },
            destinations: vec![WebhookDestinationConfig {
                id: "destination-a".to_owned(),
                url: url.to_owned(),
                filter: RouteFilter {
                    tenant_id: Arc::from("tenant-a"),
                    kind: None,
                    message_type: None,
                    channel: None,
                    actor_id: None,
                    audience_type: None,
                    audience_id: None,
                },
                queue_capacity: 8,
                timeout_ms: 2_000,
                max_attempts: 3,
                initial_backoff_ms: 5_000,
                max_backoff_ms: 5_000,
                signing_secret: Some("test-secret".to_owned()),
                headers: BTreeMap::new(),
                allowed_hosts: vec!["127.0.0.1".to_owned()],
                allowed_ports: vec![url
                    .parse::<reqwest::Url>()
                    .expect("webhook URL")
                    .port_or_known_default()
                    .expect("webhook port")],
                allow_private_ips: true,
                allow_http: true,
            }],
        }
    }

    fn message() -> Arc<RoutedMessage> {
        Arc::new(
            RoutedMessage::new(
                RoutingMetadata {
                    message_id: Arc::from("durable-message-1"),
                    tenant_id: Arc::from("tenant-a"),
                    kind: Some(Arc::from("content")),
                    message_type: None,
                    channel: None,
                    actor_id: None,
                    audience_type: None,
                    audience_id: None,
                    content_type: Arc::from("application/json"),
                    timestamp_ms: None,
                    source: Some(KafkaPosition {
                        topic: Arc::from("router.input"),
                        partition: 0,
                        offset: 42,
                    }),
                },
                Bytes::from_static(br#"{"ok":true}"#),
            )
            .expect("message"),
        )
    }

    async fn wait_for_counter(read: impl Fn() -> u64) {
        wait_for_value(read, 1).await;
    }

    async fn wait_for_value(read: impl Fn() -> u64, expected: u64) {
        timeout(Duration::from_secs(10), async {
            while read() < expected {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("metric timeout");
    }
}
