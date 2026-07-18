//! Axum HTTP, WebSocket, and Server-Sent Events endpoints.

use std::{convert::Infallible, sync::Arc, time::Duration};

use async_stream::stream;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
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
use http::{header, HeaderMap, StatusCode};
use router_core::{
    encode_delivery_json, render_prometheus, ConnectionId, DeliveryProtocol, PublishCommand,
    PublishErrorKind, RouteFilter, SubscriptionId,
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
    let queue_capacity = stream_queue_capacity(&state, query.queue_capacity)?;
    Ok(websocket
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

    loop {
        tokio::select! {
            delivery = receiver.recv() => {
                let Some(delivery) = delivery else {
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
                        let response = handle_ws_command(
                            &state,
                            &principal,
                            connection_id,
                            text.as_str(),
                        );
                        if sender.send(Message::Text(response.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Binary(_) | Message::Pong(_)) => {}
                }
            }
        }
    }
    debug!(%connection_id, "WebSocket disconnected");
}

fn handle_ws_command(
    state: &ApiState,
    principal: &Principal,
    connection_id: ConnectionId,
    text: &str,
) -> String {
    let command: WsCommand = match serde_json::from_str(text) {
        Ok(command) => command,
        Err(error) => return ws_error("invalid_json", &error.to_string()),
    };
    match command {
        WsCommand::Subscribe {
            subscription_id,
            filter,
        } => {
            let subscription_id = match SubscriptionId::new(subscription_id) {
                Ok(value) => value,
                Err(error) => return ws_error("invalid_subscription", &error.to_string()),
            };
            let filter = match filter.into_filter(principal) {
                Ok(filter) => filter,
                Err(error) => return ws_error("invalid_filter", &error.to_string()),
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
                Err(error) => ws_error("subscribe_failed", &error.to_string()),
            }
        }
        WsCommand::Unsubscribe { subscription_id } => {
            let subscription_id = match SubscriptionId::new(subscription_id) {
                Ok(value) => value,
                Err(error) => return ws_error("invalid_subscription", &error.to_string()),
            };
            match state.router.unsubscribe(connection_id, &subscription_id) {
                Ok(()) => json!({
                    "operation": "unsubscribed",
                    "subscription_id": subscription_id.as_str()
                })
                .to_string(),
                Err(error) => ws_error("unsubscribe_failed", &error.to_string()),
            }
        }
        WsCommand::Ping { opaque } => json!({
            "operation": "pong",
            "opaque": opaque
        })
        .to_string(),
    }
}

fn ws_error(code: &str, message: &str) -> String {
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
struct SseQuery {
    #[serde(flatten)]
    filter: FilterInput,
    #[serde(default)]
    subscription_id: Option<String>,
    #[serde(default)]
    queue_capacity: Option<usize>,
}

async fn sse(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<SseQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let principal = state
        .authenticator
        .authenticate_http(&headers, query.filter.tenant_id.as_deref())?;
    let filter = query.filter.into_filter(&principal)?;
    let subscription_id = SubscriptionId::new(
        query
            .subscription_id
            .unwrap_or_else(|| "sse-default".to_owned()),
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let queue_capacity = stream_queue_capacity(&state, query.queue_capacity)?;
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
            yield Ok(Event::default().id(message_id).event(event_name).data(data));
        }
    };

    Ok(Sse::new(output).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(state.config.sse_keep_alive_secs.max(1)))
            .text("keep-alive"),
    ))
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
