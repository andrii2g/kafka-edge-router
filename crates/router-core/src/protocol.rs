//! Delivery protocol identifiers used by metrics and policy.

use serde::{Deserialize, Serialize};

/// Transport attached to a router connection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryProtocol {
    /// Bidirectional WebSocket connection.
    WebSocket,
    /// Server-Sent Events stream.
    Sse,
    /// gRPC server or bidirectional stream.
    Grpc,
    /// Outbound HTTP webhook worker.
    HttpWebhook,
}

impl DeliveryProtocol {
    /// Stable Prometheus label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::Sse => "sse",
            Self::Grpc => "grpc",
            Self::HttpWebhook => "http_webhook",
        }
    }
}
