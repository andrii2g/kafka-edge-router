//! Zero-payload-parse routing metadata decoder.

use std::{str, sync::Arc};

use bytes::Bytes;
use rdkafka::{message::Headers, Message};
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
    /// A routing header appears more than once.
    #[error("duplicate Kafka header {0}")]
    DuplicateHeader(&'static str),
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
pub fn decode_message<M: Message>(
    record: &M,
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
    record: &'a impl Message,
    name: &'static str,
) -> Result<&'a str, DecodeError> {
    optional_header(record, name)?.ok_or(DecodeError::MissingHeader(name))
}

fn optional_header<'a>(
    record: &'a impl Message,
    name: &'static str,
) -> Result<Option<&'a str>, DecodeError> {
    let Some(headers) = record.headers() else {
        return Ok(None);
    };
    let mut value = None;
    for index in 0..headers.count() {
        let header = headers.get(index);
        if header.key.eq_ignore_ascii_case(name) {
            if value.is_some() {
                return Err(DecodeError::DuplicateHeader(name));
            }
            value = Some(
                header
                    .value
                    .map(|value| str::from_utf8(value).map_err(|_| DecodeError::InvalidUtf8(name)))
                    .transpose()?,
            );
        }
    }
    Ok(value.flatten())
}

#[cfg(test)]
mod tests {
    use rdkafka::{
        message::{Header, OwnedHeaders, OwnedMessage},
        Timestamp,
    };

    use super::{decode_message, DecodeError};

    fn record(headers: &[(&'static str, Option<&[u8]>)], payload: &[u8]) -> OwnedMessage {
        let headers = headers
            .iter()
            .fold(OwnedHeaders::new(), |headers, (key, value)| {
                headers.insert(Header { key, value: *value })
            });
        OwnedMessage::new(
            Some(payload.to_vec()),
            None,
            "events".to_owned(),
            Timestamp::NotAvailable,
            2,
            41,
            Some(headers),
        )
    }

    #[test]
    fn requires_tenant_header() {
        let error = decode_message(&record(&[], b"payload"), 1024).expect_err("missing tenant");
        assert!(matches!(error, DecodeError::MissingHeader("x-tenant-id")));
    }

    #[test]
    fn rejects_invalid_utf8() {
        let error = decode_message(&record(&[("x-tenant-id", Some(&[0xff]))], b"payload"), 1024)
            .expect_err("invalid UTF-8");
        assert!(matches!(error, DecodeError::InvalidUtf8("x-tenant-id")));
    }

    #[test]
    fn rejects_duplicate_headers_case_insensitively() {
        let error = decode_message(
            &record(
                &[
                    ("x-tenant-id", Some(b"tenant-a")),
                    ("X-Tenant-Id", Some(b"tenant-b")),
                ],
                b"payload",
            ),
            1024,
        )
        .expect_err("duplicate tenant");
        assert!(matches!(error, DecodeError::DuplicateHeader("x-tenant-id")));
    }

    #[test]
    fn requires_audience_headers_as_a_pair() {
        let error = decode_message(
            &record(
                &[
                    ("x-tenant-id", Some(b"tenant-a")),
                    ("x-audience-type", Some(b"team")),
                ],
                b"payload",
            ),
            1024,
        )
        .expect_err("incomplete audience");
        assert!(matches!(error, DecodeError::InvalidMetadata(_)));
    }

    #[test]
    fn enforces_identifier_and_payload_limits() {
        let long_tenant = vec![b'a'; 257];
        let identifier_error = decode_message(
            &record(&[("x-tenant-id", Some(&long_tenant))], b"payload"),
            1024,
        )
        .expect_err("long tenant");
        assert!(matches!(identifier_error, DecodeError::InvalidMetadata(_)));

        let payload_error =
            decode_message(&record(&[("x-tenant-id", Some(b"tenant-a"))], b"12345"), 4)
                .expect_err("large payload");
        assert!(matches!(
            payload_error,
            DecodeError::PayloadTooLarge {
                actual: 5,
                limit: 4
            }
        ));
    }

    #[test]
    fn supplies_default_message_id_and_content_type() {
        let message = decode_message(
            &record(&[("x-tenant-id", Some(b"tenant-a"))], b"payload"),
            1024,
        )
        .expect("valid message");
        assert_eq!(&*message.metadata.message_id, "events:2:41");
        assert_eq!(&*message.metadata.content_type, "application/octet-stream");
    }
}
