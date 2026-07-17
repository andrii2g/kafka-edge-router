//! Zero-payload-parse routing metadata decoder.

use std::{str, sync::Arc};

use bytes::Bytes;
use rdkafka::{
    message::{BorrowedMessage, Headers},
    Message,
};
use router_core::{KafkaPosition, RoutedMessage, RoutingMetadata};
use thiserror::Error;

/// Kafka record contract violation.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// A mandatory header is missing.
    #[error("missing Kafka header {0}")]
    MissingHeader(&'static str),
    /// A routing header is not valid UTF-8.
    #[error("Kafka header {0} is not valid UTF-8")]
    InvalidUtf8(&'static str),
    /// Payload exceeds configured policy.
    #[error("payload is {actual} bytes; limit is {limit} bytes")]
    PayloadTooLarge {
        /// Actual bytes.
        actual: usize,
        /// Configured maximum.
        limit: usize,
    },
    /// Decoded routing metadata violates core invariants.
    #[error("invalid routing metadata: {0}")]
    InvalidMetadata(String),
}

/// Decodes routing fields exclusively from Kafka headers and retains the payload as bytes.
pub fn decode_message(
    record: &BorrowedMessage<'_>,
    max_payload_bytes: usize,
) -> Result<RoutedMessage, DecodeError> {
    let payload = record.payload().unwrap_or_default();
    if payload.len() > max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge {
            actual: payload.len(),
            limit: max_payload_bytes,
        });
    }

    let tenant_id = required_header(record, "x-tenant-id")?;
    let message_id = optional_header(record, "x-message-id")?.map_or_else(
        || {
            format!(
                "{}:{}:{}",
                record.topic(),
                record.partition(),
                record.offset()
            )
        },
        str::to_owned,
    );
    let content_type = optional_header(record, "x-content-type")?
        .unwrap_or("application/octet-stream")
        .to_owned();

    let metadata = RoutingMetadata {
        message_id: Arc::from(message_id),
        tenant_id: Arc::from(tenant_id),
        kind: optional_header(record, "x-kind")?.map(Arc::from),
        message_type: optional_header(record, "x-type")?.map(Arc::from),
        channel: optional_header(record, "x-channel")?.map(Arc::from),
        actor_id: optional_header(record, "x-actor-id")?.map(Arc::from),
        audience_type: optional_header(record, "x-audience-type")?.map(Arc::from),
        audience_id: optional_header(record, "x-audience-id")?.map(Arc::from),
        content_type: Arc::from(content_type),
        timestamp_ms: record.timestamp().to_millis(),
        source: Some(KafkaPosition {
            topic: Arc::from(record.topic()),
            partition: record.partition(),
            offset: record.offset(),
        }),
    };

    RoutedMessage::new(metadata, Bytes::copy_from_slice(payload))
        .map_err(|error| DecodeError::InvalidMetadata(error.to_string()))
}

fn required_header<'a>(
    record: &'a BorrowedMessage<'_>,
    name: &'static str,
) -> Result<&'a str, DecodeError> {
    optional_header(record, name)?.ok_or(DecodeError::MissingHeader(name))
}

fn optional_header<'a>(
    record: &'a BorrowedMessage<'_>,
    name: &'static str,
) -> Result<Option<&'a str>, DecodeError> {
    let Some(headers) = record.headers() else {
        return Ok(None);
    };
    for index in 0..headers.count() {
        let header = headers.get(index);
        if header.key.eq_ignore_ascii_case(name) {
            return header
                .value
                .map(|value| str::from_utf8(value).map_err(|_| DecodeError::InvalidUtf8(name)))
                .transpose();
        }
    }
    Ok(None)
}
