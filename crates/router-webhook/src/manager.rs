//! One ordered worker and bounded queue per static webhook destination.

use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use http::{HeaderName, HeaderValue, StatusCode};
use reqwest::{redirect::Policy, Client, Url};
use router_core::{
    encode_delivery_json, ConnectionId, Delivery, DeliveryProtocol, LatencyStage, Router,
    SubscriptionId,
};
use router_kafka::PreCommitSink;
use sha2::Sha256;
use thiserror::Error;
use tokio::{sync::watch, task::JoinSet, time::sleep};
use tracing::{debug, error, info, info_span, warn, Instrument as _};

use crate::{
    durable::{build_durable_runtime, DurableWebhookSink, DurableWorker},
    validate_destination_url, WebhookConfig, WebhookDeliveryMode, WebhookDestinationConfig,
};

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
    /// Durable Kafka adapter construction failed.
    #[error("failed to construct durable webhook adapter: {0}")]
    Durable(String),
    /// A destination worker failed after startup.
    #[error("webhook worker {id} failed: {reason}")]
    Worker {
        /// Destination id.
        id: String,
        /// Worker failure.
        reason: String,
    },
}

/// Owns one independent worker per configured webhook destination.
pub struct WebhookManager {
    workers: Vec<WebhookWorker>,
    durable_workers: Vec<DurableWorker>,
    durable_sink: Option<Arc<DurableWebhookSink>>,
}

impl WebhookManager {
    /// Validates destinations and constructs the selected explicit delivery mode.
    pub fn new(config: &WebhookConfig, router: &Arc<Router>) -> Result<Self, WebhookError> {
        if !config.enabled {
            return Ok(Self {
                workers: Vec::new(),
                durable_workers: Vec::new(),
                durable_sink: None,
            });
        }
        match config.mode {
            WebhookDeliveryMode::Volatile => {
                let mut workers = Vec::with_capacity(config.destinations.len());
                for destination in &config.destinations {
                    workers.push(WebhookWorker::new(destination.clone(), Arc::clone(router))?);
                }
                Ok(Self {
                    workers,
                    durable_workers: Vec::new(),
                    durable_sink: None,
                })
            }
            WebhookDeliveryMode::Durable => {
                let runtime = build_durable_runtime(config, Arc::clone(router))?;
                Ok(Self {
                    workers: Vec::new(),
                    durable_workers: runtime.workers,
                    durable_sink: Some(runtime.sink),
                })
            }
        }
    }

    /// Returns the durable source-commit barrier when durable mode is selected.
    pub fn pre_commit_sink(&self) -> Option<Arc<dyn PreCommitSink>> {
        self.durable_sink
            .as_ref()
            .map(|sink| Arc::clone(sink) as Arc<dyn PreCommitSink>)
    }

    /// Runs all destination workers until shutdown. Disabled mode remains alive.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), WebhookError> {
        if self.workers.is_empty() && self.durable_workers.is_empty() {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            return Ok(());
        }

        let mut tasks = JoinSet::new();
        for worker in self.workers {
            let id = worker.id.clone();
            let worker_shutdown = shutdown.clone();
            let _abort_handle = tasks.spawn(async move {
                worker
                    .run(worker_shutdown)
                    .await
                    .map_err(|reason| WebhookError::Worker { id, reason })
            });
        }
        for worker in self.durable_workers {
            let id = worker.id().to_owned();
            let worker_shutdown = shutdown.clone();
            let _abort_handle = tasks.spawn(async move {
                worker
                    .run(worker_shutdown)
                    .await
                    .map_err(|reason| WebhookError::Worker { id, reason })
            });
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) if *shutdown.borrow() => {}
                Ok(Ok(())) => {
                    return Err(WebhookError::Worker {
                        id: "unknown".to_owned(),
                        reason: "worker exited before shutdown".to_owned(),
                    });
                }
                Ok(Err(error)) => return Err(error),
                Err(error) => {
                    return Err(WebhookError::Worker {
                        id: "unknown".to_owned(),
                        reason: error.to_string(),
                    });
                }
            }
        }
        Ok(())
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
        if let Err(error) = router.subscribe(connection_id, subscription_id, config.filter) {
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
                config.max_backoff_ms.max(config.initial_backoff_ms.max(1)),
            ),
            router,
            connection_id,
            receiver: registration.receiver,
        })
    }

    async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), String> {
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
                        if *shutdown.borrow() {
                            break;
                        }
                        return Err("core delivery queue closed".to_owned());
                    };
                    self.deliver(delivery).await;
                }
            }
        }
        info!(webhook_id = %self.id, "webhook worker stopped");
        Ok(())
    }

    async fn deliver(&self, delivery: Delivery) {
        let body = encode_delivery_json(&delivery);
        let message_id = delivery.message.metadata.message_id.to_string();
        let mut delay = self.initial_backoff;

        for attempt in 1..=self.max_attempts {
            self.router.metrics().record_webhook_attempt();
            let started = Instant::now();
            let span = info_span!(
                "webhook.attempt",
                message_id = %message_id,
                attempt,
            );
            delivery.message.set_span_parent(&span);
            let outcome = self
                .send_attempt(&body, &message_id, attempt)
                .instrument(span)
                .await;
            self.router
                .metrics()
                .record_latency(LatencyStage::WebhookAttempt, started.elapsed());
            match outcome {
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
                concat!("kafka-edge-router/", env!("CARGO_PKG_VERSION")),
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

pub(crate) fn parse_headers(
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
        let parsed_value =
            HeaderValue::from_str(value).map_err(|error| WebhookError::InvalidHeader {
                name: name.clone(),
                reason: error.to_string(),
            })?;
        headers.push((parsed_name, parsed_value));
    }
    Ok(headers)
}

pub(crate) fn retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
    ) || status.is_server_error()
}

pub(crate) fn signature(secret: &[u8], body: &[u8]) -> String {
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
