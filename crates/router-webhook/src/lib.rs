//! Static, tenant-scoped webhook destinations with bounded queues and retries.

mod config;
mod manager;
mod security;

pub use config::{WebhookConfig, WebhookDestinationConfig};
pub use manager::{WebhookError, WebhookManager};
pub use security::validate_destination_url;
