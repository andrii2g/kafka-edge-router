//! Kafka consumer, decoder, and idempotent producer adapters.

mod config;
mod decoder;
mod ingestor;
mod publisher;

pub use config::{KafkaConsumerConfig, KafkaProducerConfig};
pub use decoder::{decode_message, DecodeError};
pub use ingestor::{KafkaIngestError, KafkaIngestor};
pub use publisher::{KafkaPublisher, KafkaPublisherError};
