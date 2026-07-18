//! Transport-independent routing primitives and the bounded fan-out engine.

mod error;
mod ids;
mod message;
mod metrics;
mod protocol;
mod publisher;
mod route;
mod router;
mod wire;

pub use error::CoreError;
pub use ids::{ConnectionId, SubscriptionId};
pub use message::{KafkaPosition, RoutedMessage, RoutingMetadata, TraceContext};
pub use metrics::{
    render_prometheus, HistogramSnapshot, LatencyStage, Metrics, MetricsSnapshot, PublishProtocol,
};
pub use protocol::DeliveryProtocol;
pub use publisher::{
    MessagePublisher, PublishCommand, PublishError, PublishErrorKind, PublishReceipt,
};
pub use route::{RouteFilter, RouteKey};
pub use router::{
    ConnectionRegistration, Delivery, DispatchReport, Router, RouterConfig, RouterStatus,
};
pub use wire::{encode_delivery_json, payload_as_json};
