//! WebSocket protocol, security, limit, and lifecycle adapter tests.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{header::AUTHORIZATION, HeaderValue};
use router_api::{http_router, ApiConfig, ApiState, AuthConfig, AuthMode, HealthState};
use router_core::{DeliveryProtocol, RoutedMessage, Router, RouterConfig, RoutingMetadata};
use serde_json::{json, Value};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{frame::coding::CloseCode, CloseFrame},
        Error, Message,
    },
    MaybeTlsStream, WebSocketStream,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

type ClientSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct TestServer {
    address: std::net::SocketAddr,
    router: Arc<Router>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(router_config: RouterConfig, api_config: ApiConfig, auth: AuthConfig) -> Self {
        let router = Arc::new(Router::new(router_config));
        let state = ApiState::new(
            Arc::clone(&router),
            auth,
            None,
            Arc::new(HealthState::default()),
            api_config,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("listener address");
        let task = tokio::spawn(async move {
            axum::serve(listener, http_router(state).into_make_service())
                .await
                .expect("HTTP server");
        });
        Self {
            address,
            router,
            task,
        }
    }

    async fn connect(&self, query: &str) -> (ClientSocket, http::Response<Option<Vec<u8>>>) {
        connect_async(format!("ws://{}{query}", self.address))
            .await
            .expect("WebSocket connection")
    }

    async fn wait_for_connections(&self, expected: usize) {
        timeout(TEST_TIMEOUT, async {
            loop {
                if self.router.status().active_connections == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection count timeout");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn router_config(max_subscriptions: usize, slow_consumer_strikes: u32) -> RouterConfig {
    RouterConfig {
        default_queue_capacity: 4,
        max_queue_capacity: 8,
        max_subscriptions_per_connection: max_subscriptions,
        slow_consumer_strikes,
        ..RouterConfig::default()
    }
}

fn api_config(command_limit: u32, max_message_bytes: usize) -> ApiConfig {
    ApiConfig {
        stream_queue_capacity: 4,
        max_stream_queue_capacity: 8,
        ws_max_message_bytes: max_message_bytes,
        ws_max_frame_bytes: max_message_bytes,
        ws_max_commands_per_second: command_limit,
        ..ApiConfig::default()
    }
}

fn disabled_auth() -> AuthConfig {
    AuthConfig {
        default_tenant: Some("tenant-a".to_owned()),
        ..AuthConfig::default()
    }
}

async fn send_json(socket: &mut ClientSocket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("send command");
}

async fn next_message(socket: &mut ClientSocket) -> Message {
    timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("WebSocket receive timeout")
        .expect("WebSocket stream ended")
        .expect("WebSocket receive")
}

async fn next_json(socket: &mut ClientSocket) -> Value {
    match next_message(socket).await {
        Message::Text(text) => serde_json::from_str(text.as_str()).expect("JSON response"),
        message => panic!("expected text response, got {message:?}"),
    }
}

async fn close_normally(socket: &mut ClientSocket) {
    socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "test_complete".into(),
        })))
        .await
        .expect("send close");
    let _ = timeout(TEST_TIMEOUT, socket.next()).await;
}

#[tokio::test]
async fn websocket_command_contract_is_stable_and_connection_remains_healthy() {
    let server =
        TestServer::start(router_config(1, 3), api_config(100, 1024), disabled_auth()).await;
    let (mut socket, response) = server
        .connect("/v1/ws?tenant_id=tenant-a&queue_capacity=2")
        .await;
    assert!(response.headers().get("sec-websocket-extensions").is_none());
    server.wait_for_connections(1).await;

    let subscribe = json!({
        "operation": "subscribe",
        "subscription_id": "news",
        "filter": { "channel": "news" }
    });
    send_json(&mut socket, subscribe.clone()).await;
    assert_eq!(next_json(&mut socket).await["operation"], "subscribed");

    send_json(&mut socket, subscribe).await;
    assert_eq!(next_json(&mut socket).await["code"], "subscription_exists");

    send_json(
        &mut socket,
        json!({
            "operation": "subscribe",
            "subscription_id": "alerts",
            "filter": { "channel": "alerts" }
        }),
    )
    .await;
    assert_eq!(
        next_json(&mut socket).await["code"],
        "subscription_limit_reached"
    );

    send_json(
        &mut socket,
        json!({ "operation": "ping", "opaque": "request-17" }),
    )
    .await;
    let pong = next_json(&mut socket).await;
    assert_eq!(pong["operation"], "pong");
    assert_eq!(pong["opaque"], "request-17");

    socket
        .send(Message::Text("{".into()))
        .await
        .expect("malformed command");
    assert_eq!(next_json(&mut socket).await["code"], "invalid_json");

    socket
        .send(Message::Binary(Bytes::from_static(b"binary")))
        .await
        .expect("binary command");
    assert_eq!(next_json(&mut socket).await["code"], "binary_not_supported");

    send_json(
        &mut socket,
        json!({ "operation": "unsubscribe", "subscription_id": "news" }),
    )
    .await;
    assert_eq!(next_json(&mut socket).await["operation"], "unsubscribed");
    send_json(
        &mut socket,
        json!({ "operation": "unsubscribe", "subscription_id": "news" }),
    )
    .await;
    assert_eq!(
        next_json(&mut socket).await["code"],
        "subscription_not_found"
    );

    close_normally(&mut socket).await;
    server.wait_for_connections(0).await;

    let (reconnected, _) = server.connect("/v1/ws?tenant_id=tenant-a").await;
    server.wait_for_connections(1).await;
    drop(reconnected);
    server.wait_for_connections(0).await;
}

#[tokio::test]
async fn websocket_validation_error_codes_are_stable() {
    let server =
        TestServer::start(router_config(4, 3), api_config(20, 1024), disabled_auth()).await;
    let (mut socket, _) = server.connect("/v1/ws?tenant_id=tenant-a").await;

    send_json(&mut socket, json!({ "operation": "unknown" })).await;
    assert_eq!(next_json(&mut socket).await["code"], "invalid_command");

    for (subscription_id, filter, expected) in [
        ("", json!({}), "invalid_subscription_id"),
        (
            "invalid-filter",
            json!({ "audience_type": "team" }),
            "invalid_filter",
        ),
        (
            "cross-tenant",
            json!({ "tenant_id": "tenant-b" }),
            "tenant_mismatch",
        ),
    ] {
        send_json(
            &mut socket,
            json!({
                "operation": "subscribe",
                "subscription_id": subscription_id,
                "filter": filter
            }),
        )
        .await;
        assert_eq!(next_json(&mut socket).await["code"], expected);
    }

    close_normally(&mut socket).await;
    server.wait_for_connections(0).await;
}
#[tokio::test]
async fn websocket_untrusted_queue_capacity_is_rejected_before_registration() {
    let server =
        TestServer::start(router_config(4, 3), api_config(10, 1024), disabled_auth()).await;
    let error = connect_async(format!(
        "ws://{}/v1/ws?tenant_id=tenant-a&queue_capacity=999999",
        server.address
    ))
    .await
    .expect_err("oversized queue request");
    let Error::Http(response) = error else {
        panic!("expected HTTP rejection, got {error:?}");
    };
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(server.router.status().active_connections, 0);
}
#[tokio::test]
async fn websocket_authenticated_query_tenant_mismatch_is_rejected_before_registration() {
    let mut bearer_tokens = BTreeMap::new();
    bearer_tokens.insert("secret-token".to_owned(), "tenant-a".to_owned());
    let auth = AuthConfig {
        mode: AuthMode::StaticBearer,
        bearer_tokens,
        ..AuthConfig::default()
    };
    let server = TestServer::start(router_config(4, 3), api_config(10, 1024), auth).await;
    let mut request = format!("ws://{}/v1/ws?tenant_id=tenant-b", server.address)
        .into_client_request()
        .expect("request");
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer secret-token"),
    );

    let error = connect_async(request).await.expect_err("tenant mismatch");
    let Error::Http(response) = error else {
        panic!("expected HTTP rejection, got {error:?}");
    };
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
    assert_eq!(server.router.status().active_connections, 0);
}

#[tokio::test]
async fn websocket_command_rate_limit_returns_error_without_closing_connection() {
    let server = TestServer::start(router_config(4, 3), api_config(2, 1024), disabled_auth()).await;
    let (mut socket, _) = server.connect("/v1/ws?tenant_id=tenant-a").await;

    for opaque in ["one", "two"] {
        send_json(
            &mut socket,
            json!({ "operation": "ping", "opaque": opaque }),
        )
        .await;
        assert_eq!(next_json(&mut socket).await["operation"], "pong");
    }
    send_json(
        &mut socket,
        json!({ "operation": "ping", "opaque": "three" }),
    )
    .await;
    assert_eq!(next_json(&mut socket).await["code"], "rate_limited");
    assert_eq!(server.router.status().active_connections, 1);

    close_normally(&mut socket).await;
    server.wait_for_connections(0).await;
}

#[tokio::test]
async fn websocket_oversized_command_closes_with_size_code_and_cleans_registration() {
    let server = TestServer::start(router_config(4, 3), api_config(10, 64), disabled_auth()).await;
    let (mut socket, _) = server.connect("/v1/ws?tenant_id=tenant-a").await;
    server.wait_for_connections(1).await;
    socket
        .send(Message::Text("x".repeat(256).into()))
        .await
        .expect("oversized command");

    let Message::Close(Some(frame)) = next_message(&mut socket).await else {
        panic!("expected close frame");
    };
    assert_eq!(frame.code, CloseCode::Size);
    assert_eq!(frame.reason, "message_too_large");
    server.wait_for_connections(0).await;
}

#[tokio::test]
async fn websocket_queue_saturation_closes_slow_consumer_without_blocking_dispatch() {
    let server =
        TestServer::start(router_config(4, 1), api_config(10, 1024), disabled_auth()).await;
    let (mut socket, _) = server
        .connect("/v1/ws?tenant_id=tenant-a&queue_capacity=1")
        .await;
    send_json(
        &mut socket,
        json!({
            "operation": "subscribe",
            "subscription_id": "all",
            "filter": {}
        }),
    )
    .await;
    assert_eq!(next_json(&mut socket).await["operation"], "subscribed");

    let message = Arc::new(
        RoutedMessage::new(
            RoutingMetadata {
                message_id: Arc::from("slow-1"),
                tenant_id: Arc::from("tenant-a"),
                kind: None,
                message_type: None,
                channel: None,
                actor_id: None,
                audience_type: None,
                audience_id: None,
                content_type: Arc::from("application/octet-stream"),
                timestamp_ms: None,
                source: None,
            },
            Bytes::from_static(b"payload"),
        )
        .expect("routed message"),
    );
    let first = server.router.dispatch(Arc::clone(&message));
    let second = server.router.dispatch(message);
    assert_eq!(first.delivered_connections, 1);
    assert_eq!(second.full_connections, 1);
    assert_eq!(server.router.status().active_connections, 0);

    assert_eq!(next_json(&mut socket).await["operation"], "message");
    let Message::Close(Some(frame)) = next_message(&mut socket).await else {
        panic!("expected slow-consumer close frame");
    };
    assert_eq!(frame.code, CloseCode::Again);
    assert_eq!(frame.reason, "slow_consumer");
}

#[tokio::test]
async fn websocket_abrupt_client_cancellation_removes_registration_and_subscriptions() {
    let server =
        TestServer::start(router_config(4, 3), api_config(10, 1024), disabled_auth()).await;
    let (mut socket, _) = server.connect("/v1/ws?tenant_id=tenant-a").await;
    send_json(
        &mut socket,
        json!({
            "operation": "subscribe",
            "subscription_id": "cancelled",
            "filter": {}
        }),
    )
    .await;
    assert_eq!(next_json(&mut socket).await["operation"], "subscribed");
    assert_eq!(server.router.status().subscriptions, 1);

    drop(socket);
    server.wait_for_connections(0).await;
    assert_eq!(server.router.status().subscriptions, 0);
    assert_eq!(
        server
            .router
            .connections_by_protocol(DeliveryProtocol::WebSocket),
        0
    );
}
