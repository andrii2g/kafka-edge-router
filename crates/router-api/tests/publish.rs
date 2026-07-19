//! HTTP and gRPC publish contract, authorization, idempotency, and error tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{body::Body, Router as AxumRouter};
use bytes::Bytes;
use http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use router_api::{http_router, serve_grpc, ApiConfig, ApiState, AuthConfig, AuthMode, HealthState};
use router_core::{
    MessagePublisher, PublishCommand, PublishError, PublishErrorKind, PublishReceipt, Router,
    RouterConfig,
};
use router_proto::v1::{kafka_router_client::KafkaRouterClient, PublishRequest};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tonic::{transport::Channel, Code};
use tower::ServiceExt;

#[derive(Clone, Copy)]
enum Outcome {
    Success,
    Failure(PublishErrorKind),
}

struct RecordingPublisher {
    commands: Mutex<Vec<PublishCommand>>,
    outcome: Outcome,
}

impl RecordingPublisher {
    fn new(outcome: Outcome) -> Arc<Self> {
        Arc::new(Self {
            commands: Mutex::new(Vec::new()),
            outcome,
        })
    }

    fn commands(&self) -> Vec<PublishCommand> {
        self.commands.lock().expect("commands lock").clone()
    }
}

#[async_trait]
impl MessagePublisher for RecordingPublisher {
    async fn publish(&self, command: PublishCommand) -> Result<PublishReceipt, PublishError> {
        self.commands
            .lock()
            .expect("commands lock")
            .push(command.clone());
        match self.outcome {
            Outcome::Success => Ok(PublishReceipt {
                message_id: command.message_id.to_string(),
                topic: "router.input".to_owned(),
                partition: 2,
                offset: 17,
            }),
            Outcome::Failure(PublishErrorKind::InvalidInput) => {
                Err(PublishError::invalid_input("backend validation"))
            }
            Outcome::Failure(PublishErrorKind::Timeout) => {
                Err(PublishError::timeout("private broker timeout detail"))
            }
            Outcome::Failure(PublishErrorKind::QueueFull) => {
                Err(PublishError::queue_full("private queue detail"))
            }
            Outcome::Failure(PublishErrorKind::Backend) => {
                Err(PublishError::backend("private broker detail"))
            }
        }
    }
}

fn api_config(maximum: usize) -> ApiConfig {
    ApiConfig {
        http_body_limit_bytes: 16_384,
        publish_max_payload_bytes: maximum,
        grpc_health_enabled: false,
        grpc_reflection_enabled: false,
        ..ApiConfig::default()
    }
}

fn auth(publish_allowed: bool) -> AuthConfig {
    AuthConfig {
        default_tenant: Some("tenant-a".to_owned()),
        publish_tenants: if publish_allowed {
            BTreeSet::from(["tenant-a".to_owned()])
        } else {
            BTreeSet::new()
        },
        ..AuthConfig::default()
    }
}

fn test_state(
    maximum: usize,
    auth: AuthConfig,
    publisher: Option<Arc<dyn MessagePublisher>>,
) -> (ApiState, Arc<Router>) {
    let router = Arc::new(Router::new(RouterConfig::default()));
    (
        ApiState::new(
            Arc::clone(&router),
            auth,
            publisher,
            Arc::new(HealthState::default()),
            api_config(maximum),
        ),
        router,
    )
}

async fn post(app: &AxumRouter, body: Value, authorization: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/publish")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(authorization) = authorization {
        request = request.header(header::AUTHORIZATION, authorization);
    }
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(serde_json::to_vec(&body).expect("request JSON")))
                .expect("request"),
        )
        .await
        .expect("HTTP response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, body)
}

#[tokio::test]
async fn http_json_and_base64_round_trip_and_ids_are_explicit() {
    let publisher = RecordingPublisher::new(Outcome::Success);
    let (state, _) = test_state(
        32,
        auth(true),
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    );
    let app = http_router(state);

    let (status, generated) = post(
        &app,
        json!({
            "tenant_id": "tenant-a",
            "kind": "content",
            "payload": null
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let generated_id = generated["message_id"]
        .as_str()
        .expect("generated message id");
    assert!(!generated_id.is_empty());

    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        [0_u8, 255, 10, 13],
    );
    for _ in 0..2 {
        let (status, response) = post(
            &app,
            json!({
                "message_id": "stable-retry-id",
                "tenant_id": "tenant-a",
                "content_type": "application/octet-stream",
                "ordering_key": "invoice-42",
                "payload_base64": encoded
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["message_id"], "stable-retry-id");
        assert_eq!(response["partition"], 2);
        assert_eq!(response["offset"], 17);
    }

    let commands = publisher.commands();
    assert_eq!(commands.len(), 3);
    assert_eq!(commands[0].payload, Bytes::from_static(b"null"));
    assert_eq!(commands[0].content_type.as_ref(), "application/json");
    assert_eq!(commands[0].message_id.as_ref(), generated_id);
    for command in &commands[1..] {
        assert_eq!(command.message_id.as_ref(), "stable-retry-id");
        assert_eq!(command.payload.as_ref(), &[0, 255, 10, 13]);
        assert_eq!(command.ordering_key.as_deref(), Some("invoice-42"));
    }
}

#[tokio::test]
async fn http_rejects_ambiguous_oversized_and_inconsistent_inputs_before_publish() {
    let publisher = RecordingPublisher::new(Outcome::Success);
    let (state, _) = test_state(
        4,
        auth(true),
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    );
    let app = http_router(state);

    for body in [
        json!({"tenant_id":"tenant-a","payload":{},"payload_base64":"e30="}),
        json!({"tenant_id":"tenant-a"}),
        json!({"tenant_id":"tenant-a","content_type":"application/octet-stream","payload_base64":"***"}),
        json!({"tenant_id":"tenant-a","content_type":"application/octet-stream","payload_base64":"MTIzNDU="}),
        json!({"tenant_id":"tenant-a","content_type":"text/plain","payload":{"ok":true}}),
        json!({"tenant_id":"tenant-a","recipient_type":"team","payload":{}}),
        json!({"tenant_id":"tenant-a","recipient_category":"team","recipient_key":"team-7","payload":{}}),
        json!({"tenant_id":"tenant-a","ordering_key":" ","payload":{}}),
    ] {
        let (status, response) = post(&app, body, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    }

    assert!(publisher.commands().is_empty());
}

#[tokio::test]
async fn http_publish_permission_and_tenant_are_independent_from_subscription_auth() {
    let publisher = RecordingPublisher::new(Outcome::Success);
    let (state, _) = test_state(
        32,
        auth(false),
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    );
    let (status, _) = post(
        &http_router(state),
        json!({"tenant_id":"tenant-a","payload":{}}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let mut bearer_tokens = BTreeMap::new();
    bearer_tokens.insert("token-a".to_owned(), "tenant-a".to_owned());
    let static_auth = AuthConfig {
        mode: AuthMode::StaticBearer,
        bearer_tokens,
        publish_tenants: BTreeSet::from(["tenant-a".to_owned()]),
        ..AuthConfig::default()
    };
    let (state, _) = test_state(
        32,
        static_auth,
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    );
    let (status, _) = post(
        &http_router(state),
        json!({"tenant_id":"tenant-b","payload":{}}),
        Some("Bearer token-a"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(publisher.commands().is_empty());
}

#[tokio::test]
async fn http_backend_failures_have_stable_public_statuses_and_safe_metrics() {
    for (kind, expected) in [
        (PublishErrorKind::InvalidInput, StatusCode::BAD_REQUEST),
        (PublishErrorKind::Timeout, StatusCode::GATEWAY_TIMEOUT),
        (PublishErrorKind::QueueFull, StatusCode::SERVICE_UNAVAILABLE),
        (PublishErrorKind::Backend, StatusCode::BAD_GATEWAY),
    ] {
        let publisher = RecordingPublisher::new(Outcome::Failure(kind));
        let (state, _) = test_state(64, auth(true), Some(publisher));
        let (status, body) = post(
            &http_router(state),
            json!({"tenant_id":"tenant-a","payload":"do-not-expose"}),
            None,
        )
        .await;
        assert_eq!(status, expected);
        assert!(!body.to_string().contains("private"));
        assert!(!body.to_string().contains("do-not-expose"));
    }

    let publisher = RecordingPublisher::new(Outcome::Success);
    let (state, router) = test_state(64, auth(true), Some(publisher));
    let app = http_router(state);
    let (status, _) = post(
        &app,
        json!({"tenant_id":"tenant-a","payload":"secret-payload"}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let metrics = router.metrics().snapshot();
    assert_eq!(metrics.http_publish_attempts, 1);
    assert_eq!(metrics.http_publish_acknowledged, 1);
    assert_eq!(metrics.http_publish_failures, 0);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");
    let body = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .expect("metrics body")
            .to_bytes()
            .to_vec(),
    )
    .expect("metrics text");
    assert!(body.contains("router_publish_attempts_total{protocol=\"http\"} 1"));
    assert!(!body.contains("secret-payload"));
}

struct GrpcHarness {
    endpoint: String,
    router: Arc<Router>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl GrpcHarness {
    async fn start(
        maximum: usize,
        auth: AuthConfig,
        publisher: Option<Arc<dyn MessagePublisher>>,
    ) -> Self {
        let (state, router) = test_state(maximum, auth, publisher);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("gRPC bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            serve_grpc(listener, state, receiver)
                .await
                .expect("gRPC server");
        });
        Self {
            endpoint,
            router,
            shutdown,
            task,
        }
    }

    async fn client(&self) -> KafkaRouterClient<Channel> {
        let channel = Channel::from_shared(self.endpoint.clone())
            .expect("endpoint")
            .connect()
            .await
            .expect("channel");
        KafkaRouterClient::new(channel)
    }

    async fn stop(self) {
        self.shutdown.send(true).expect("shutdown");
        self.task.abort();
        let result = self.task.await;
        assert!(result.is_ok() || result.is_err_and(|error| error.is_cancelled()));
    }
}

fn grpc_request(payload: Vec<u8>) -> PublishRequest {
    PublishRequest {
        message_id: Some("grpc-message".to_owned()),
        tenant_id: "tenant-a".to_owned(),
        recipient_type: Some("team".to_owned()),
        recipient_identity: Some("team-7".to_owned()),
        ordering_key: Some("entity-7".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        payload,
        ..PublishRequest::default()
    }
}

#[tokio::test]
async fn grpc_raw_bytes_and_errors_match_the_shared_publish_contract() {
    let publisher = RecordingPublisher::new(Outcome::Success);
    let harness = GrpcHarness::start(
        4,
        auth(true),
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    )
    .await;
    let response = harness
        .client()
        .await
        .publish(grpc_request(vec![0, 255, 10, 13]))
        .await
        .expect("gRPC publish")
        .into_inner();
    assert_eq!(response.message_id, "grpc-message");
    let command = publisher.commands().pop().expect("published command");
    assert_eq!(command.payload.as_ref(), &[0, 255, 10, 13]);
    assert_eq!(command.ordering_key.as_deref(), Some("entity-7"));
    assert_eq!(
        harness
            .router
            .metrics()
            .snapshot()
            .grpc_publish_acknowledged,
        1
    );

    let oversized = harness
        .client()
        .await
        .publish(grpc_request(vec![0; 5]))
        .await
        .expect_err("oversized payload");
    assert_eq!(oversized.code(), Code::InvalidArgument);
    harness.stop().await;

    for (kind, expected) in [
        (PublishErrorKind::InvalidInput, Code::InvalidArgument),
        (PublishErrorKind::Timeout, Code::DeadlineExceeded),
        (PublishErrorKind::QueueFull, Code::ResourceExhausted),
        (PublishErrorKind::Backend, Code::Internal),
    ] {
        let publisher = RecordingPublisher::new(Outcome::Failure(kind));
        let harness = GrpcHarness::start(32, auth(true), Some(publisher)).await;
        let status = harness
            .client()
            .await
            .publish(grpc_request(Vec::new()))
            .await
            .expect_err("publish failure");
        assert_eq!(status.code(), expected);
        assert!(!status.message().contains("private"));
        harness.stop().await;
    }
}

#[tokio::test]
async fn grpc_rejects_publish_permission_and_incomplete_recipient() {
    let publisher = RecordingPublisher::new(Outcome::Success);
    let harness = GrpcHarness::start(
        32,
        auth(false),
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    )
    .await;
    let denied = harness
        .client()
        .await
        .publish(grpc_request(Vec::new()))
        .await
        .expect_err("publish permission");
    assert_eq!(denied.code(), Code::PermissionDenied);
    harness.stop().await;

    let harness = GrpcHarness::start(
        32,
        auth(true),
        Some(Arc::clone(&publisher) as Arc<dyn MessagePublisher>),
    )
    .await;
    let mut invalid = grpc_request(Vec::new());
    invalid.recipient_identity = None;
    let status = harness
        .client()
        .await
        .publish(invalid)
        .await
        .expect_err("incomplete recipient");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(publisher.commands().is_empty());
    harness.stop().await;
}
