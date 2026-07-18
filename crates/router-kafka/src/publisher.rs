//! Idempotent Kafka producer used by public publish endpoints.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::{
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use router_core::{MessagePublisher, PublishCommand, PublishError, PublishReceipt};
use thiserror::Error;

use crate::KafkaProducerConfig;

/// Kafka producer construction failure.
#[derive(Debug, Error)]
pub enum KafkaPublisherError {
    /// Producer creation failed.
    #[error("failed to create Kafka producer: {0}")]
    Create(#[source] rdkafka::error::KafkaError),
}

/// `MessagePublisher` backed by an idempotent `FutureProducer`.
pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
    delivery_timeout: Duration,
}

impl KafkaPublisher {
    /// Creates a producer from configuration.
    pub fn new(config: &KafkaProducerConfig) -> Result<Self, KafkaPublisherError> {
        let producer = producer_client_config(config)
            .create()
            .map_err(KafkaPublisherError::Create)?;
        Ok(Self {
            producer,
            topic: config.topic.clone(),
            delivery_timeout: Duration::from_millis(config.delivery_timeout_ms),
        })
    }
}

fn producer_client_config(config: &KafkaProducerConfig) -> ClientConfig {
    let mut client = ClientConfig::new();
    for (key, value) in &config.properties {
        client.set(key, value);
    }
    // Apply delivery guarantees last so free-form properties cannot weaken them.
    client
        .set("bootstrap.servers", &config.brokers)
        .set("client.id", &config.client_id)
        .set("message.timeout.ms", config.delivery_timeout_ms.to_string())
        .set("enable.idempotence", "true")
        .set("acks", "all");
    client
}

#[async_trait]
impl MessagePublisher for KafkaPublisher {
    async fn publish(&self, command: PublishCommand) -> Result<PublishReceipt, PublishError> {
        command.validate()?;
        let message_id = command.message_id.to_string();

        let mut headers = OwnedHeaders::new()
            .insert(Header {
                key: "x-message-id",
                value: Some(message_id.as_bytes()),
            })
            .insert(Header {
                key: "x-tenant-id",
                value: Some(command.tenant_id.as_bytes()),
            })
            .insert(Header {
                key: "x-content-type",
                value: Some(command.content_type.as_bytes()),
            });
        headers = insert_optional(headers, "x-kind", command.kind.as_deref());
        headers = insert_optional(headers, "x-type", command.message_type.as_deref());
        headers = insert_optional(headers, "x-channel", command.channel.as_deref());
        headers = insert_optional(headers, "x-actor-id", command.actor_id.as_deref());
        headers = insert_optional(headers, "x-audience-type", command.audience_type.as_deref());
        headers = insert_optional(headers, "x-audience-id", command.audience_id.as_deref());

        let key = kafka_key(&command);
        let record = FutureRecord::to(&self.topic)
            .key(key.as_bytes())
            .payload(command.payload.as_ref())
            .headers(headers);
        let delivery = self
            .producer
            .send(record, Timeout::After(self.delivery_timeout))
            .await
            .map_err(|(error, _)| classify_delivery_error(&error))?;

        Ok(PublishReceipt {
            message_id,
            topic: self.topic.clone(),
            partition: delivery.partition,
            offset: delivery.offset,
        })
    }
}

fn insert_optional(headers: OwnedHeaders, key: &'static str, value: Option<&str>) -> OwnedHeaders {
    match value {
        Some(value) => headers.insert(Header {
            key,
            value: Some(value.as_bytes()),
        }),
        None => headers,
    }
}

fn classify_delivery_error(error: &KafkaError) -> PublishError {
    match error.rdkafka_error_code() {
        Some(RDKafkaErrorCode::QueueFull) => PublishError::queue_full(error.to_string()),
        Some(
            RDKafkaErrorCode::MessageTimedOut
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::TimedOutQueue,
        ) => PublishError::timeout(error.to_string()),
        _ => PublishError::backend(error.to_string()),
    }
}

fn kafka_key(command: &PublishCommand) -> String {
    if let Some(ordering_key) = &command.ordering_key {
        return format!("{}:explicit:{ordering_key}", command.tenant_id);
    }
    match (&command.audience_type, &command.audience_id) {
        (Some(audience_type), Some(audience_id)) => {
            format!("{}:{audience_type}:{audience_id}", command.tenant_id)
        }
        _ => command.channel.as_ref().map_or_else(
            || command.tenant_id.to_string(),
            |channel| format!("{}:channel:{channel}", command.tenant_id),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use bytes::Bytes;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use router_core::{PublishCommand, PublishErrorKind};

    use super::{classify_delivery_error, kafka_key, producer_client_config};
    use crate::KafkaProducerConfig;

    fn command() -> PublishCommand {
        PublishCommand {
            message_id: Arc::from("message-1"),
            tenant_id: Arc::from("tenant-a"),
            kind: None,
            message_type: None,
            channel: Some(Arc::from("news")),
            actor_id: None,
            audience_type: Some(Arc::from("team")),
            audience_id: Some(Arc::from("team-7")),
            ordering_key: None,
            content_type: Arc::from("application/json"),
            payload: Bytes::new(),
        }
    }

    #[test]
    fn audience_and_explicit_keys_preserve_tenant_ordering() {
        let mut command = command();
        assert_eq!(kafka_key(&command), "tenant-a:team:team-7");

        command.ordering_key = Some(Arc::from("invoice-42"));
        assert_eq!(
            kafka_key(&command),
            "tenant-a:explicit:invoice-42",
            "explicit keys must remain tenant namespaced"
        );
    }

    #[test]
    fn producer_invariants_override_free_form_properties() {
        let config = KafkaProducerConfig {
            brokers: "broker:9092".to_owned(),
            client_id: "publisher".to_owned(),
            delivery_timeout_ms: 3210,
            properties: BTreeMap::from([
                ("enable.idempotence".to_owned(), "false".to_owned()),
                ("acks".to_owned(), "1".to_owned()),
                ("message.timeout.ms".to_owned(), "999".to_owned()),
            ]),
            ..KafkaProducerConfig::default()
        };
        let client = producer_client_config(&config);

        assert_eq!(client.get("enable.idempotence"), Some("true"));
        assert_eq!(client.get("acks"), Some("all"));
        assert_eq!(client.get("message.timeout.ms"), Some("3210"));
    }

    #[test]
    fn delivery_errors_keep_timeout_and_queue_full_classifications() {
        let queue_full =
            classify_delivery_error(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull));
        assert_eq!(queue_full.kind(), PublishErrorKind::QueueFull);

        let timeout = classify_delivery_error(&KafkaError::MessageProduction(
            RDKafkaErrorCode::MessageTimedOut,
        ));
        assert_eq!(timeout.kind(), PublishErrorKind::Timeout);

        let backend =
            classify_delivery_error(&KafkaError::MessageProduction(RDKafkaErrorCode::Unknown));
        assert_eq!(backend.kind(), PublishErrorKind::Backend);
    }
}
