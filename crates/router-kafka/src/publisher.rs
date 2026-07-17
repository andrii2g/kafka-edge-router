//! Idempotent Kafka producer used by public publish endpoints.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use router_core::{
    MessagePublisher, PublishCommand, PublishError, PublishReceipt, RoutingMetadata,
};
use thiserror::Error;
use uuid::Uuid;

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
        let producer = client.create().map_err(KafkaPublisherError::Create)?;
        Ok(Self {
            producer,
            topic: config.topic.clone(),
            delivery_timeout: Duration::from_millis(config.delivery_timeout_ms),
        })
    }
}

#[async_trait]
impl MessagePublisher for KafkaPublisher {
    async fn publish(&self, command: PublishCommand) -> Result<PublishReceipt, PublishError> {
        let message_id = command
            .message_id
            .as_deref()
            .map_or_else(|| Uuid::new_v4().to_string(), str::to_owned);

        RoutingMetadata {
            message_id: Arc::from(message_id.as_str()),
            tenant_id: Arc::clone(&command.tenant_id),
            kind: command.kind.clone(),
            message_type: command.message_type.clone(),
            channel: command.channel.clone(),
            actor_id: command.actor_id.clone(),
            audience_type: command.audience_type.clone(),
            audience_id: command.audience_id.clone(),
            content_type: Arc::clone(&command.content_type),
            timestamp_ms: None,
            source: None,
        }
        .validate()
        .map_err(|error| PublishError::invalid_input(error.to_string()))?;

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
            .map_err(|(error, _)| PublishError::backend(error.to_string()))?;

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

fn kafka_key(command: &PublishCommand) -> String {
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
    use std::sync::Arc;

    use bytes::Bytes;
    use router_core::PublishCommand;

    use super::kafka_key;

    #[test]
    fn audience_key_preserves_entity_ordering() {
        let command = PublishCommand {
            message_id: None,
            tenant_id: Arc::from("tenant-a"),
            kind: None,
            message_type: None,
            channel: Some(Arc::from("news")),
            actor_id: None,
            audience_type: Some(Arc::from("team")),
            audience_id: Some(Arc::from("team-7")),
            content_type: Arc::from("application/json"),
            payload: Bytes::new(),
        };
        assert_eq!(kafka_key(&command), "tenant-a:team:team-7");
    }
}
