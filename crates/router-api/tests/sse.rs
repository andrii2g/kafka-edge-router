//! SSE framing, security, backpressure, and lifecycle adapter tests.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{header, Request, Response, StatusCode},
    Router as AxumRouter,
};
use bytes::Bytes;
use http_body_util::BodyExt;
use router_api::{http_router, ApiConfig, ApiState, AuthConfig, AuthMode, HealthState};
use router_core::{RoutedMessage, Router, RouterConfig, RoutingMetadata};
use serde_json::Value;
use tower::ServiceExt;

struct TestApp {
    app: AxumRouter,
    router: Arc<Router>,
}

impl TestApp {
    fn new(router_config: RouterConfig, api_config: ApiConfig, auth: AuthConfig) -> Self {
        let router = Arc::new(Router::new(router_config));
        let state = ApiState::new(
            Arc::clone(&router),
            auth,
            None,
            Arc::new(HealthState::default()),
            api_config,
        );
        Self {
            app: http_router(state),
            router,
        }
    }

    async fn get(&self, uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
        let mut request = Request::builder().uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.app
            .clone()
            .oneshot(request.body(Body::empty()).expect("request"))
            .await
            .expect("HTTP response")
    }
}

fn router_config(slow_consumer_strikes: u32) -> RouterConfig {
    RouterConfig {
        default_queue_capacity: 4,
        max_queue_capacity: 8,
        max_subscriptions_per_connection: 4,
        slow_consumer_strikes,
        ..RouterConfig::default()
    }
}

fn api_config() -> ApiConfig {
    ApiConfig {
        stream_queue_capacity: 4,
        max_stream_queue_capacity: 8,
        sse_keep_alive_secs: 2,
        ..ApiConfig::default()
    }
}

fn disabled_auth() -> AuthConfig {
    AuthConfig {
        default_tenant: Some("tenant-a".to_owned()),
        ..AuthConfig::default()
    }
}

fn message(id: &str, tenant: &str, payload: Bytes) -> Arc<RoutedMessage> {
    Arc::new(
        RoutedMessage::new(
            RoutingMetadata {
                message_id: Arc::from(id),
                tenant_id: Arc::from(tenant),
                kind: Some(Arc::from("content")),
                message_type: Some(Arc::from("broadcast")),
                channel: Some(Arc::from("news")),
                actor_id: None,
                audience_type: None,
                audience_id: None,
                content_type: Arc::from("application/json"),
                timestamp_ms: None,
                source: None,
            },
            payload,
        )
        .expect("routed message"),
    )
}

async fn next_data(body: &mut Body) -> Bytes {
    loop {
        let frame = body
            .frame()
            .await
            .expect("SSE body ended")
            .expect("SSE body frame");
        if let Ok(data) = frame.into_data() {
            return data;
        }
    }
}

#[tokio::test]
async fn sse_frames_ids_names_and_escaped_json_without_compression() {
    let app = TestApp::new(router_config(3), api_config(), disabled_auth());
    let response = app
        .get(
            "/v1/events?tenant_id=tenant-a&kind=content&type=broadcast&channel=news&subscription_id=browser&queue_capacity=2",
            &[(header::ACCEPT_ENCODING.as_str(), "gzip")],
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    assert_eq!(
        response.headers()[header::CACHE_CONTROL],
        "no-cache, no-transform"
    );
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
    assert_eq!(app.router.status().active_connections, 1);
    assert_eq!(app.router.status().subscriptions, 1);

    let payload = Bytes::from_static(br#"{"text":"first\nsecond","quote":"\""}"#);
    let outcome = app
        .router
        .dispatch(message("event-17", "tenant-a", payload));
    assert_eq!(outcome.delivered_connections, 1);

    let mut body = response.into_body();
    let frame = String::from_utf8(next_data(&mut body).await.to_vec()).expect("UTF-8 SSE");
    assert!(frame.contains("id: event-17\n"));
    assert!(frame.contains("event: content.broadcast\n"));

    let data_lines: Vec<&str> = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect();
    assert_eq!(data_lines.len(), 1, "JSON newlines must remain escaped");
    let envelope: Value = serde_json::from_str(data_lines[0]).expect("delivery JSON");
    assert_eq!(envelope["operation"], "message");
    assert_eq!(envelope["subscription_ids"][0], "browser");
    assert_eq!(envelope["message"]["metadata"]["message_id"], "event-17");
    assert_eq!(envelope["message"]["payload"]["text"], "first\nsecond");
    assert_eq!(envelope["message"]["payload"]["quote"], "\"");

    drop(body);
    assert_eq!(app.router.status().active_connections, 0);
    assert_eq!(app.router.status().subscriptions, 0);
}

#[tokio::test(start_paused = true)]
async fn sse_keep_alive_uses_virtual_time() {
    let app = TestApp::new(router_config(3), api_config(), disabled_auth());
    let response = app.get("/v1/events?tenant_id=tenant-a", &[]).await;
    let mut body = response.into_body();
    let frame_task = tokio::spawn(async move { next_data(&mut body).await });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;

    let frame = frame_task.await.expect("keep-alive task");
    assert_eq!(frame, Bytes::from_static(b": keep-alive\n\n"));
}

#[tokio::test]
async fn sse_rejects_unknown_and_invalid_inputs_without_leaks() {
    let app = TestApp::new(router_config(3), api_config(), disabled_auth());

    for uri in [
        "/v1/events?tenant_id=tenant-a&unknown=value",
        "/v1/events?tenant_id=tenant-a&queue_capacity=0",
        "/v1/events?tenant_id=tenant-a&queue_capacity=9",
        "/v1/events?tenant_id=tenant-a&subscription_id=%20",
        "/v1/events?tenant_id=tenant-a&audience_type=team",
    ] {
        let response = app.get(uri, &[]).await;
        assert!(
            response.status().is_client_error(),
            "{uri} returned {}",
            response.status()
        );
        assert_eq!(app.router.status().active_connections, 0);
        assert_eq!(app.router.status().subscriptions, 0);
    }
}

#[tokio::test]
async fn sse_parses_last_event_id_but_delivers_only_new_live_events() {
    let app = TestApp::new(router_config(3), api_config(), disabled_auth());
    let response = app
        .get(
            "/v1/events?tenant_id=tenant-a",
            &[("last-event-id", "event-16")],
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-sse-replay"], "unsupported");

    let outcome = app.router.dispatch(message(
        "event-17",
        "tenant-a",
        Bytes::from_static(br#"{"sequence":17}"#),
    ));
    assert_eq!(outcome.delivered_connections, 1);

    let mut body = response.into_body();
    let frame = String::from_utf8(next_data(&mut body).await.to_vec()).expect("UTF-8 SSE");
    assert!(frame.contains(
        "id: event-17
"
    ));
    assert!(!frame.contains("event-16"));
    drop(body);
    assert_eq!(app.router.status().active_connections, 0);
}

#[tokio::test]
async fn sse_rejects_cross_tenant_filter_before_registration() {
    let mut bearer_tokens = BTreeMap::new();
    bearer_tokens.insert("secret-token".to_owned(), "tenant-a".to_owned());
    let auth = AuthConfig {
        mode: AuthMode::StaticBearer,
        bearer_tokens,
        ..AuthConfig::default()
    };
    let app = TestApp::new(router_config(3), api_config(), auth);

    let response = app
        .get(
            "/v1/events?tenant_id=tenant-b",
            &[(header::AUTHORIZATION.as_str(), "Bearer secret-token")],
        )
        .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(app.router.status().active_connections, 0);
    assert_eq!(app.router.status().subscriptions, 0);
}

#[tokio::test]
async fn sse_disconnect_unregisters_connection_and_subscription() {
    let app = TestApp::new(router_config(3), api_config(), disabled_auth());
    let response = app
        .get(
            "/v1/events?tenant_id=tenant-a&subscription_id=cancelled",
            &[],
        )
        .await;
    assert_eq!(app.router.status().active_connections, 1);
    assert_eq!(app.router.status().subscriptions, 1);

    drop(response);

    assert_eq!(app.router.status().active_connections, 0);
    assert_eq!(app.router.status().subscriptions, 0);
}

#[tokio::test]
async fn sse_queue_saturation_unregisters_only_the_slow_consumer() {
    let app = TestApp::new(router_config(1), api_config(), disabled_auth());
    let response = app
        .get("/v1/events?tenant_id=tenant-a&queue_capacity=1", &[])
        .await;

    let first = app.router.dispatch(message(
        "slow-1",
        "tenant-a",
        Bytes::from_static(br#"{"sequence":1}"#),
    ));
    let second = app.router.dispatch(message(
        "slow-2",
        "tenant-a",
        Bytes::from_static(br#"{"sequence":2}"#),
    ));

    assert_eq!(first.delivered_connections, 1);
    assert_eq!(second.full_connections, 1);
    assert_eq!(app.router.status().active_connections, 0);
    assert_eq!(app.router.status().subscriptions, 0);
    drop(response);
}
