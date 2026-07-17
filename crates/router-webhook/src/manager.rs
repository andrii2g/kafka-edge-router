//! One ordered worker and bounded queue per static webhook destination.

use std::{net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use bytes::Bytes;
use hmac::{Hmac, Mac};
use http::{HeaderName, HeaderValue, StatusCode};
use reqwest::{redirect::Policy, Client, Url};
use router_core::{
    encode_delivery_json, ConnectionId, Delivery, DeliveryProtocol, Router, SubscriptionId,
};
use sha2::Sha256;
use thiserror::Error;
use tokio::{sync::watch, task::JoinSet, time::sleep};
use tracing::{debug, error, info, warn};

use crate::{validate_destination_url, WebhookConfig, WebhookDestinationConfig};

type HmacSha256 = Hmac<Sha256>;

/// Webhook configuration or worker construction error.
#[derive(Debug, Error)]
pub enum WebhookError {
    /// Destination URL is malformed or unsafe.
    #[error("invalid webhook URL: {0}")]
    InvalidUrl(String),
    /// URL host is outside the configured allowlist.
    #[error("webhook host {0} is not allowed")]
    HostNotAllowed(String),
    /// Literal private/local destination is disallowed.
    #[error("private webhook address {0} is not allowed")]
    PrivateAddress(IpAddr),
    /// Static header configuration is invalid.
    #[error("invalid webhook header {name}: {reason}")]
    InvalidHeader {
        /// Header name.
        name: String,
        /// Validation error.
        reason: String,
    },
    /// HTTP client construction failed.
    #[error("failed to build webhook client: {0}")]
    Client(#[source] reqwest::Error),
    /// Core registration or subscription failed.
    #[error("failed to register webhook {id}: {reason}")]
    Registration {
        /// Destination id.
        id: String,
        /// Core error.
        reason: String,
    },
}

/// Owns one independent worker per configured webhook destination.
pub struct WebhookManager {
    workers: Vec<WebhookWorker>,
}

impl WebhookManager {
    /// Validates destinations, registers filters, and creates HTTP clients.
    pub fn new(config: &WebhookConfig, router: Arc<Router>) -> Result<Self, WebhookError> {
        if !config.enabled {
            return Ok(Self { workers: Vec::new() });
        }
        let mut workers = Vec::with_capacity(config.destinations.len());
        for destination in &config.destinations {
            workers.push(WebhookWorker::new(
                destination.clone(),
                Arc::clone(&router),
            )?);
        }
        Ok(Self { workers })
    }

    /// Runs all destination workers until shutdown. Disabled mode remains alive
    /// so the daemon does not interpret an empty manager as a component failure.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        if self.workers.is_empty() {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            return;
        }

        let mut tasks = JoinSet::new();
        for worker in self.workers {
            let _abort_handle = tasks.spawn(worker.run(shutdown.clone()));
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(join_error) = result {
                error!(error = %join_error, "webhook worker task failed");
            }
        }
    }
}

struct WebhookWorker {
    id: String,
    url: Url,
    client: Client,
    headers: Vec<(HeaderName, HeaderValue)>,
    signing_secret: Option<Vec<u8>>,
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    router: Arc<Router>,
    connection_id: ConnectionId,
    receiver: tokio::sync::mpsc::Receiver<Delivery>,
}

impl WebhookWorker {
    fn new(config: WebhookDestinationConfig, router: Arc<Router>) -> Result<Self, WebhookError> {
        let url = validate_destination_url(
            &config.url,
            &config.allowed_hosts,
            config.allow_private_ips,
            config.allow_http,
        )?;
        let client = Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_millis(config.timeout_ms.max(1)))
            .timeout(Duration::from_millis(config.timeout_ms.max(1)))
            .build()
            .map_err(WebhookError::Client)?;
        let headers = parse_headers(&config.headers)?;

        let registration = router
            .register_connection(
                &config.filter.tenant_id,
                DeliveryProtocol::HttpWebhook,
                Some(config.queue_capacity),
            )
            .map_err(|error| WebhookError::Registration {
                id: config.id.clone(),
                reason: error.to_string(),
            })?;
        let connection_id = registration.connection_id;
        let subscription_id = match SubscriptionId::new(format!("webhook:{}", config.id)) {
            Ok(subscription_id) => subscription_id,
            Err(error) => {
                router.unregister_connection(connection_id);
                return Err(WebhookError::Registration {
                    id: config.id.clone(),
                    reason: error.to_string(),
                });
            }
        };
        if let Err(error) =
            router.subscribe(connection_id, subscription_id, config.filter)
        {
            router.unregister_connection(connection_id);
            return Err(WebhookError::Registration {
                id: config.id,
                reason: error.to_string(),
            });
        }

        Ok(Self {
            id: config.id,
            url,
            client,
            headers,
            signing_secret: config.signing_secret.map(String::into_bytes),
            max_attempts: config.max_attempts.max(1),
            initial_backoff: Duration::from_millis(config.initial_backoff_ms.max(1)),
            max_backoff: Duration::from_millis(
                config
                    .max_backoff_ms
                    .max(config.initial_backoff_ms.max(1)),
            ),
            router,
            connection_id,
            receiver: registration.receiver,
        })
    }

    async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        info!(webhook_id = %self.id, url = %self.url, "webhook worker started");
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                delivery = self.receiver.recv() => {
                    let Some(delivery) = delivery else {
                        break;
                    };
                    self.deliver(delivery).await;
                }
            }
        }
        info!(webhook_id = %self.id, "webhook worker stopped");
    }

    async fn deliver(&self, delivery: Delivery) {
        let body = encode_delivery_json(&delivery);
        let message_id = delivery.message.metadata.message_id.to_string();
        let mut delay = self.initial_backoff;

        for attempt in 1..=self.max_attempts {
            self.router.metrics().record_webhook_attempt();
            match self.send_attempt(&body, &message_id, attempt).await {
                AttemptResult::Delivered(status) => {
                    self.router.metrics().record_webhook_success();
                    debug!(
                        webhook_id = %self.id,
                        %message_id,
                        %status,
                        attempt,
                        "webhook delivered"
                    );
                    return;
                }
                AttemptResult::Permanent(reason) => {
                    self.router.metrics().record_webhook_failure();
                    warn!(
                        webhook_id = %self.id,
                        %message_id,
                        attempt,
                        %reason,
                        "webhook permanently rejected"
                    );
                    return;
                }
                AttemptResult::Retryable(reason) if attempt < self.max_attempts => {
                    warn!(
                        webhook_id = %self.id,
                        %message_id,
                        attempt,
                        %reason,
                        retry_after_ms = delay.as_millis(),
                        "webhook attempt will be retried"
                    );
                    sleep(delay).await;
                    delay = delay.saturating_mul(2).min(self.max_backoff);
                }
                AttemptResult::Retryable(reason) => {
                    self.router.metrics().record_webhook_failure();
                    error!(
                        webhook_id = %self.id,
                        %message_id,
                        attempt,
                        %reason,
                        "webhook retries exhausted"
                    );
                    return;
                }
            }
        }
    }

    async fn send_attempt(&self, body: &Bytes, message_id: &str, attempt: u32) -> AttemptResult {
        let mut request = self
            .client
            .post(self.url.clone())
            .header("content-type", "application/json")
            .header(
                "user-agent",
                concat!("rust-kafka-edge-router/", env!("CARGO_PKG_VERSION")),
            )
            .header("x-router-message-id", message_id)
            .header("idempotency-key", message_id)
            .header("x-router-attempt", attempt.to_string())
            .body(body.clone());
        for (name, value) in &self.headers {
            request = request.header(name.clone(), value.clone());
        }
        if let Some(secret) = &self.signing_secret {
            request = request.header("x-router-signature", signature(secret, body));
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => {
                AttemptResult::Delivered(response.status())
            }
            Ok(response) if retryable_status(response.status()) => {
                AttemptResult::Retryable(format!("HTTP {}", response.status()))
            }
            Ok(response) => AttemptResult::Permanent(format!("HTTP {}", response.status())),
            Err(error) if error.is_timeout() || error.is_connect() || error.is_request() => {
                AttemptResult::Retryable(error.to_string())
            }
            Err(error) => AttemptResult::Permanent(error.to_string()),
        }
    }
}

impl Drop for WebhookWorker {
    fn drop(&mut self) {
        self.router.unregister_connection(self.connection_id);
    }
}

fn parse_headers(
    values: &std::collections::BTreeMap<String, String>,
) -> Result<Vec<(HeaderName, HeaderValue)>, WebhookError> {
    let mut headers = Vec::with_capacity(values.len());
    for (name, value) in values {
        let parsed_name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            WebhookError::InvalidHeader {
                name: name.clone(),
                reason: error.to_string(),
            }
        })?;
        if matches!(
            parsed_name.as_str(),
            "host"
                | "content-length"
                | "content-type"
                | "user-agent"
                | "idempotency-key"
                | "x-router-message-id"
                | "x-router-attempt"
                | "x-router-signature"
        ) {
            return Err(WebhookError::InvalidHeader {
                name: name.clone(),
                reason: "reserved transport header".to_owned(),
            });
        }
        let parsed_value = HeaderValue::from_str(value).map_err(|error| {
            WebhookError::InvalidHeader {
                name: name.clone(),
                reason: error.to_string(),
            }
        })?;
        headers.push((parsed_name, parsed_value));
    }
    Ok(headers)
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_EARLY
            | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

fn signature(secret: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts arbitrary key lengths");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

enum AttemptResult {
    Delivered(StatusCode),
    Retryable(String),
    Permanent(String),
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::{retryable_status, signature};

    #[test]
    fn signs_body_deterministically() {
        assert_eq!(
            signature(b"secret", b"body"),
            "sha256=dc46983557fea127b43af721467eb9b3fde2338fe3e14f51952aa8478c13d355"
        );
    }

    #[test]
    fn retries_only_transient_statuses() {
        assert!(retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(retryable_status(StatusCode::BAD_GATEWAY));
        assert!(!retryable_status(StatusCode::BAD_REQUEST));
    }
}
