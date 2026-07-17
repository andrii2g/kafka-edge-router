//! Shared JSON wire encoding for WebSocket, SSE, and webhooks.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};

use crate::{Delivery, RoutedMessage};

/// Returns a cached JSON value for JSON content or a base64 wrapper otherwise.
pub fn payload_as_json(message: &RoutedMessage) -> Value {
    message
        .cached_payload_json
        .get_or_init(|| {
            if message
                .metadata
                .content_type
                .split(';')
                .next()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
            {
                serde_json::from_slice(&message.payload).unwrap_or_else(|_| {
                    json!({
                        "encoding": "base64",
                        "data": STANDARD.encode(&message.payload),
                        "invalid_json": true
                    })
                })
            } else {
                json!({
                    "encoding": "base64",
                    "data": STANDARD.encode(&message.payload)
                })
            }
        })
        .clone()
}

/// Encodes a complete delivery envelope once for a protocol writer.
pub fn encode_delivery_json(delivery: &Delivery) -> bytes::Bytes {
    let subscription_ids: Vec<&str> = delivery
        .subscription_ids
        .iter()
        .map(crate::SubscriptionId::as_str)
        .collect();
    let value = json!({
        "operation": "message",
        "subscription_ids": subscription_ids,
        "message": {
            "metadata": &delivery.message.metadata,
            "payload": payload_as_json(&delivery.message),
        }
    });
    bytes::Bytes::from(
        serde_json::to_vec(&value).expect("serializing validated router envelope cannot fail"),
    )
}
