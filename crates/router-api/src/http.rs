//! Axum HTTP, WebSocket, and Server-Sent Events endpoints.

use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

use async_stream::stream;
use axum::{
    extract::{
        ws::{close_code, CloseFrame, Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Response, Sse,
    },
    routing::{get, post},
    Json, Router as AxumRouter,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{header, HeaderMap, HeaderValue, StatusCode};
use router_core::{
    encode_delivery_json, render_prometheus, ConnectionId, CoreError, DeliveryProtocol,
    PublishCommand, PublishErrorKind, RouteFilter, SubscriptionId,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::watch};
use tower_http::{catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::{debug, warn};

use crate::{state::ConnectionGuard, ApiError, ApiState, Principal};

/// Builds the complete public HTTP application.
pub fn http_router(state: ApiState) -> AxumRouter {
    let body_limit = state.config.http_body_limit_bytes;
    AxumRouter::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/status", get(status))
        .route("/v1/publish", post(publish))
        .route("/v1/ws", get(websocket))
        .route("/v1/events", get(sse))
        .with_state(state)
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
}

/// Serves HTTP until shutdown is requested.
pub async fn serve_http(
    listener: TcpListener,
    state: ApiState,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    axum::serve(listener, http_router(state).into_make_service())
        .with_graceful_shutdown(async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
}

async fn live(State(state): State<ApiState>) -> Response {
    health_response(state.health.is_live(), "live")
}

async fn ready(State(state): State<ApiState>) -> Response {
    health_response(state.health.is_ready(), "ready")
}

fn health_response(ok: bool, state: &'static str) -> Response {
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(json!({ "status": state }))).into_response()
}

async fn status(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({
        "ready": state.health.is_ready(),
        "router": state.router.status(),
        "connections_by_protocol": {
            "websocket": state.router.connections_by_protocol(DeliveryProtocol::WebSocket),
            "sse": state.router.connections_by_protocol(DeliveryProtocol::Sse),
            "grpc": state.router.connections_by_protocol(DeliveryProtocol::Grpc),
            "http_webhook": state.router.connections_by_protocol(DeliveryProtocol::HttpWebhook),
        }
    }))
}

async fn metrics(State(state): State<ApiState>) -> Response {
    let status = state.router.status();
    let body = render_prometheus(
        status.metrics,
        status.active_connections,
        status.subscriptions,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

fn default_content_type() -> String {
    "application/json".to_owned()
}

#[derive(Debug, Deserialize)]
struct HttpPublishRequest {
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default, rename = "type")]
    message_type: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    actor_id: Option<String>,
    #[serde(default)]
    audience_type: Option<String>,
    #[serde(default)]
    audience_id: Option<String>,
    #[serde(default = "default_content_type")]
    content_type: String,
    payload: Value,
}

#[derive(Debug, Serialize)]
struct HttpPublishResponse {
    message_id: String,
    topic: String,
    partition: i32,
    offset: i64,
}

async fn publish(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<HttpPublishRequest>,
) -> Result<Json<HttpPublishResponse>, ApiError> {
    let principal = state
        .authenticator
        .authenticate_http(&headers, request.tenant_id.as_deref())?;
    authorize_requested_tenant(&principal, request.tenant_id.as_deref())?;
    let publisher = state
        .publisher
        .as_ref()
        .ok_or(ApiError::PublisherUnavailable)?;
    let payload = serde_json::to_vec(&request.payload)
        .map(Bytes::from)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let receipt = publisher
        .publish(PublishCommand {
            message_id: request.message_id.map(Arc::from),
            tenant_id: principal.tenant_id,
            kind: request.kind.map(Arc::from),
            message_type: request.message_type.map(Arc::from),
            channel: request.channel.map(Arc::from),
            actor_id: request.actor_id.map(Arc::from),
            audience_type: request.audience_type.map(Arc::from),
            audience_id: request.audience_id.map(Arc::from),
            content_type: Arc::from(request.content_type),
            payload,
        })
        .await
        .map_err(|error| match error.kind() {
            PublishErrorKind::InvalidInput => ApiError::BadRequest(error.to_string()),
            PublishErrorKind::Backend => {
                warn!(error = %error, "Kafka publish failed");
                ApiError::Backend("publish backend failed".to_owned())
            }
        })?;
    Ok(Json(HttpPublishResponse {
        message_id: receipt.message_id,
        topic: receipt.topic,
        partition: receipt.partition,
        offset: receipt.offset,
    }))
}

#[derive(Debug, Deserialize)]
struct ConnectionQuery {
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    queue_capacity: Option<usize>,
}

async fn websocket(
    websocket: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ConnectionQuery>,
) -> Result<Response, ApiError> {
    let principal = state
        .authenticator
        .authenticate_http(&headers, query.tenant_id.as_deref())?;
    authorize_requested_tenant(&principal, query.tenant_id.as_deref())?;
    let queue_capacity = stream_queue_capacity(&state, query.queue_capacity)?;
    let max_message_bytes = state.config.ws_max_message_bytes;
    let max_frame_bytes = state.config.ws_max_frame_bytes;
    Ok(websocket
        .max_message_size(max_message_bytes)
        .max_frame_size(max_frame_bytes)
        .on_upgrade(move |socket| websocket_session(socket, state, principal, queue_capacity)))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum WsCommand {
    Subscribe {
        subscription_id: String,
        filter: FilterInput,
    },
    Unsubscribe {
        subscription_id: String,
    },
    Ping {
        #[serde(default)]
        opaque: Option<String>,
    },
}

async fn websocket_session(
    socket: WebSocket,
    state: ApiState,
    principal: Principal,
    queue_capacity: usize,
) {
    let registration = match state.router.register_connection(
        &principal.tenant_id,
        DeliveryProtocol::WebSocket,
        Some(queue_capacity),
    ) {
        Ok(registration) => registration,
        Err(error) => {
            warn!(error = %error, "WebSocket registration failed");
            return;
        }
    };
    let connection_id = registration.connection_id;
    let _guard = ConnectionGuard::new(Arc::clone(&state.router), connection_id);
    let mut receiver = registration.receiver;
    let (mut sender, mut incoming) = socket.split();
    let mut rate_limiter = CommandRateLimiter::new(state.config.ws_max_commands_per_second);

    loop {
        tokio::select! {
            delivery = receiver.recv() => {
                let Some(delivery) = delivery else {
                    let _ = sender.send(Message::Close(Some(CloseFrame {
                        code: close_code::AGAIN,
                        reason: "slow_consumer".into(),
                    }))).await;
                    break;
                };
                let payload = encode_delivery_json(&delivery);
                let text = String::from_utf8_lossy(&payload).into_owned();
                if sender.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            message = incoming.next() => {
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(Message::Text(text)) => {
                        let response = if rate_limiter.allow() {
                            handle_ws_command(
                                &state,
                                &principal,
                                connection_id,
                                text.as_str(),
                            )
                        } else {
                            ws_error("rate_limited", "command rate limit exceeded")
                        };
                        if sender.send(Message::Text(response.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        let response = if rate_limiter.allow() {
                            ws_error("binary_not_supported", "binary commands are not supported")
                        } else {
                            ws_error("rate_limited", "command rate limit exceeded")
                        };
                        if sender.send(Message::Text(response.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        let _ = sender.send(Message::Close(frame)).await;
                        break;
                    }
                    Err(error) => {
                        let detail = error.to_string();
                        let frame = ws_transport_close(error);
                        debug!(error = %detail, %connection_id, "WebSocket transport error");
                        let _ = sender.send(Message::Close(Some(frame))).await;
                        break;
                    }
                    Ok(Message::Pong(_)) => {}
                }
            }
        }
    }
    debug!(%connection_id, "WebSocket disconnected");
}

fn ws_transport_close(error: axum::Error) -> CloseFrame {
    let inner = error.into_inner();
    let oversized = matches!(
        inner.downcast_ref::<tokio_tungstenite::tungstenite::Error>(),
        Some(tokio_tungstenite::tungstenite::Error::Capacity(
            tokio_tungstenite::tungstenite::error::CapacityError::MessageTooLong { .. }
        ))
    );
    if oversized {
        CloseFrame {
            code: close_code::SIZE,
            reason: "message_too_large".into(),
        }
    } else {
        CloseFrame {
            code: close_code::PROTOCOL,
            reason: "protocol_error".into(),
        }
    }
}
struct CommandRateLimiter {
    window_started: Instant,
    accepted: u32,
    limit: u32,
}

impl CommandRateLimiter {
    fn new(limit: u32) -> Self {
        Self {
            window_started: Instant::now(),
            accepted: 0,
            limit,
        }
    }

    fn allow(&mut self) -> bool {
        if self.window_started.elapsed() >= Duration::from_secs(1) {
            self.window_started = Instant::now();
            self.accepted = 0;
        }
        if self.accepted >= self.limit {
            return false;
        }
        self.accepted += 1;
        true
    }
}
fn handle_ws_command(
    state: &ApiState,
    principal: &Principal,
    connection_id: ConnectionId,
    text: &str,
) -> String {
    let command: WsCommand = match serde_json::from_str(text) {
        Ok(command) => command,
        Err(error) if error.is_syntax() || error.is_eof() => {
            return ws_error("invalid_json", "command is not valid JSON");
        }
        Err(_) => return ws_error("invalid_command", "command does not match the protocol"),
    };
    match command {
        WsCommand::Subscribe {
            subscription_id,
            filter,
        } => {
            let Ok(subscription_id) = SubscriptionId::new(subscription_id) else {
                return ws_error("invalid_subscription_id", "subscription_id is invalid");
            };
            let filter = match filter.into_filter(principal) {
                Ok(filter) => filter,
                Err(ApiError::Forbidden) => {
                    return ws_error("tenant_mismatch", "filter tenant is not authorized");
                }
                Err(_) => return ws_error("invalid_filter", "filter is invalid"),
            };
            match state
                .router
                .subscribe(connection_id, subscription_id.clone(), filter)
            {
                Ok(()) => json!({
                    "operation": "subscribed",
                    "subscription_id": subscription_id.as_str()
                })
                .to_string(),
                Err(error) => ws_subscribe_error(&error),
            }
        }
        WsCommand::Unsubscribe { subscription_id } => {
            let Ok(subscription_id) = SubscriptionId::new(subscription_id) else {
                return ws_error("invalid_subscription_id", "subscription_id is invalid");
            };
            match state.router.unsubscribe(connection_id, &subscription_id) {
                Ok(()) => json!({
                    "operation": "unsubscribed",
                    "subscription_id": subscription_id.as_str()
                })
                .to_string(),
                Err(CoreError::SubscriptionNotFound) => {
                    ws_error("subscription_not_found", "subscription does not exist")
                }
                Err(CoreError::ConnectionNotFound) => {
                    ws_error("connection_closed", "connection is no longer registered")
                }
                Err(_) => ws_error("unsubscribe_failed", "subscription could not be removed"),
            }
        }
        WsCommand::Ping { opaque } => json!({
            "operation": "pong",
            "opaque": opaque
        })
        .to_string(),
    }
}

fn ws_subscribe_error(error: &CoreError) -> String {
    match error {
        CoreError::SubscriptionExists => {
            ws_error("subscription_exists", "subscription_id already exists")
        }
        CoreError::SubscriptionLimitReached => ws_error(
            "subscription_limit_reached",
            "connection subscription limit reached",
        ),
        CoreError::TenantMismatch => ws_error("tenant_mismatch", "filter tenant is not authorized"),
        CoreError::ConnectionNotFound => {
            ws_error("connection_closed", "connection is no longer registered")
        }
        CoreError::InvalidIdentifier { .. }
        | CoreError::IncompleteAudience
        | CoreError::MissingField(_) => ws_error("invalid_filter", "filter is invalid"),
        CoreError::SubscriptionNotFound | CoreError::InvalidQueueCapacity { .. } => {
            ws_error("subscribe_failed", "subscription could not be created")
        }
    }
}

fn ws_error(code: &'static str, message: &'static str) -> String {
    json!({
        "operation": "error",
        "code": code,
        "message": message,
    })
    .to_string()
}
#[derive(Clone, Debug, Default, Deserialize)]
struct FilterInput {
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default, rename = "type")]
    message_type: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    actor_id: Option<String>,
    #[serde(default)]
    audience_type: Option<String>,
    #[serde(default)]
    audience_id: Option<String>,
}

impl FilterInput {
    fn into_filter(self, principal: &Principal) -> Result<RouteFilter, ApiError> {
        authorize_requested_tenant(principal, self.tenant_id.as_deref())?;
        Ok(RouteFilter {
            tenant_id: Arc::clone(&principal.tenant_id),
            kind: self.kind.map(Arc::from),
            message_type: self.message_type.map(Arc::from),
            channel: self.channel.map(Arc::from),
            actor_id: self.actor_id.map(Arc::from),
            audience_type: self.audience_type.map(Arc::from),
            audience_id: self.audience_id.map(Arc::from),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SseQuery {
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default, rename = "type")]
    message_type: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    actor_id: Option<String>,
    #[serde(default)]
    audience_type: Option<String>,
    #[serde(default)]
    audience_id: Option<String>,
    #[serde(default)]
    subscription_id: Option<String>,
    #[serde(default)]
    queue_capacity: Option<usize>,
}

impl SseQuery {
    fn into_filter(
        self,
        principal: &Principal,
    ) -> Result<(RouteFilter, String, Option<usize>), ApiError> {
        let filter = FilterInput {
            tenant_id: self.tenant_id,
            kind: self.kind,
            message_type: self.message_type,
            channel: self.channel,
            actor_id: self.actor_id,
            audience_type: self.audience_type,
            audience_id: self.audience_id,
        }
        .into_filter(principal)?;
        Ok((
            filter,
            self.subscription_id
                .unwrap_or_else(|| "sse-default".to_owned()),
            self.queue_capacity,
        ))
    }
}

async fn sse(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<SseQuery>,
) -> Result<Response, ApiError> {
    let principal = state
        .authenticator
        .authenticate_http(&headers, query.tenant_id.as_deref())?;
    parse_last_event_id(&headers)?;
    let (filter, subscription_id, requested_queue_capacity) = query.into_filter(&principal)?;
    let subscription_id = SubscriptionId::new(subscription_id)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let queue_capacity = stream_queue_capacity(&state, requested_queue_capacity)?;
    let registration = state
        .router
        .register_connection(
            &principal.tenant_id,
            DeliveryProtocol::Sse,
            Some(queue_capacity),
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    if let Err(error) = state
        .router
        .subscribe(registration.connection_id, subscription_id, filter)
    {
        state
            .router
            .unregister_connection(registration.connection_id);
        return Err(ApiError::BadRequest(error.to_string()));
    }

    let router = Arc::clone(&state.router);
    let connection_id = registration.connection_id;
    let mut receiver = registration.receiver;
    let guard = ConnectionGuard::new(router, connection_id);
    let output = stream! {
        let _guard = guard;
        while let Some(delivery) = receiver.recv().await {
            let message_id = delivery.message.metadata.message_id.to_string();
            let event_name = match (
                delivery.message.metadata.kind.as_deref(),
                delivery.message.metadata.message_type.as_deref(),
            ) {
                (Some(kind), Some(message_type)) => format!("{kind}.{message_type}"),
                (Some(kind), None) => kind.to_owned(),
                _ => "message".to_owned(),
            };
            let data = String::from_utf8_lossy(&encode_delivery_json(&delivery)).into_owned();
            yield Ok::<Event, Infallible>(Event::default().id(message_id).event(event_name).data(data));
        }
    };

    let mut response = Sse::new(output)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(state.config.sse_keep_alive_secs.max(1)))
                .text("keep-alive"),
        )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
        .headers_mut()
        .insert("x-sse-replay", HeaderValue::from_static("unsupported"));
    Ok(response)
}

fn parse_last_event_id(headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(value) = headers.get("last-event-id") else {
        return Ok(());
    };
    value.to_str().map_err(|_| {
        ApiError::BadRequest("Last-Event-ID must be a valid HTTP header value".to_owned())
    })?;
    Ok(())
}

fn stream_queue_capacity(state: &ApiState, requested: Option<usize>) -> Result<usize, ApiError> {
    crate::state::resolve_stream_queue_capacity(&state.config, requested).map_err(|maximum| {
        ApiError::BadRequest(format!("queue_capacity must be between 1 and {maximum}"))
    })
}

fn authorize_requested_tenant(
    principal: &Principal,
    requested_tenant: Option<&str>,
) -> Result<(), ApiError> {
    if requested_tenant
        .filter(|value| !value.is_empty())
        .is_some_and(|tenant| tenant != principal.tenant_id.as_ref())
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}
