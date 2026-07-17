//! Webhook configuration contracts.

use std::collections::BTreeMap;

use router_core::RouteFilter;
use serde::Deserialize;

fn default_queue_capacity() -> usize {
    256
}

fn default_timeout_ms() -> u64 {
    5_000
}

fn default_max_attempts() -> u32 {
    5
}

fn default_initial_backoff_ms() -> u64 {
    250
}

fn default_max_backoff_ms() -> u64 {
    30_000
}

/// Outbound webhook module configuration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct WebhookConfig {
    /// Enables static destinations.
    pub enabled: bool,
    /// Independently ordered destinations.
    pub destinations: Vec<WebhookDestinationConfig>,
}

/// One statically configured outbound destination.
#[derive(Clone, Debug, Deserialize)]
pub struct WebhookDestinationConfig {
    /// Stable operator-defined destination id.
    pub id: String,
    /// HTTPS endpoint, or HTTP only when explicitly enabled.
    pub url: String,
    /// Route filter registered in the core matcher.
    pub filter: RouteFilter,
    /// Bounded destination queue capacity.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    /// Connect and total request timeout.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Total attempts including the first request.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Initial exponential retry delay.
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Maximum exponential retry delay.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Optional HMAC-SHA256 signing secret.
    #[serde(default)]
    pub signing_secret: Option<String>,
    /// Additional non-reserved request headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Explicit hostname allowlist. Empty means only the configured hostname.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// Allows literal private/local IP destinations. Disabled by default.
    #[serde(default)]
    pub allow_private_ips: bool,
    /// Allows plain HTTP. Disabled by default.
    #[serde(default)]
    pub allow_http: bool,
}
