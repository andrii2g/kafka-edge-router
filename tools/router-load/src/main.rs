//! Bounded end-to-end load and soak generator for public router protocols.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context};
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use http::header::{HeaderValue, AUTHORIZATION};
use router_proto::v1::{
    kafka_router_client::KafkaRouterClient, server_event, RouteFilter, SubscribeRequest,
};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
use tonic::{transport::Endpoint, Request};
use uuid::Uuid;

const MAX_MESSAGES: u64 = 10_000_000;
const MAX_CONNECTIONS: usize = 100_000;
const MAX_IN_FLIGHT: usize = 1_024;
const MAX_SSE_BUFFER: usize = 1_048_576;
const MAX_LATENCY_US: u64 = 3_600_000_000;

#[derive(Debug, Parser, Serialize)]
#[command(about = "Bounded WS/SSE/gRPC/webhook load generator")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    http_base: String,
    #[arg(long, default_value = "ws://127.0.0.1:8080/v1/ws")]
    websocket_url: String,
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    grpc_endpoint: String,
    #[arg(long)]
    bearer_token: Option<String>,
    #[arg(long, default_value = "tenant-load")]
    tenant: String,
    #[arg(long, default_value = "load")]
    channel: String,
    #[arg(long, default_value_t = 10_000)]
    messages: u64,
    #[arg(long, default_value_t = 1_000)]
    rate_per_second: u32,
    #[arg(long, default_value_t = 4)]
    publish_workers: usize,
    #[arg(long, default_value_t = 128)]
    max_in_flight: usize,
    #[arg(long, default_value_t = 1)]
    websocket_connections: usize,
    #[arg(long, default_value_t = 1)]
    sse_connections: usize,
    #[arg(long, default_value_t = 1)]
    grpc_connections: usize,
    #[arg(long, default_value_t = 0)]
    expected_webhooks_per_message: u64,
    #[arg(long)]
    webhook_listen: Option<SocketAddr>,
    #[arg(long, default_value_t = 0)]
    webhook_fail_every: u64,
    #[arg(long, default_value_t = 0)]
    slow_reader_delay_ms: u64,
    #[arg(long, default_value_t = 30)]
    publish_timeout_secs: u64,
    #[arg(long, default_value_t = 30)]
    drain_timeout_secs: u64,
    #[arg(long, default_value = "artifacts/load-report.json")]
    output: PathBuf,
}

#[derive(Clone, Copy)]
enum Protocol {
    WebSocket,
    Sse,
    Grpc,
    Webhook,
}

struct Recorder {
    starts: Mutex<HashMap<String, Instant>>,
    websocket: Mutex<Histogram<u64>>,
    sse: Mutex<Histogram<u64>>,
    grpc: Mutex<Histogram<u64>>,
    webhook: Mutex<Histogram<u64>>,
}

impl Recorder {
    fn new(capacity: usize) -> anyhow::Result<Self> {
        Ok(Self {
            starts: Mutex::new(HashMap::with_capacity(capacity)),
            websocket: Mutex::new(Histogram::new_with_max(MAX_LATENCY_US, 3)?),
            sse: Mutex::new(Histogram::new_with_max(MAX_LATENCY_US, 3)?),
            grpc: Mutex::new(Histogram::new_with_max(MAX_LATENCY_US, 3)?),
            webhook: Mutex::new(Histogram::new_with_max(MAX_LATENCY_US, 3)?),
        })
    }

    fn start(&self, message_id: String) {
        self.starts
            .lock()
            .expect("load start map mutex poisoned")
            .insert(message_id, Instant::now());
    }

    fn remove_start(&self, message_id: &str) {
        self.starts
            .lock()
            .expect("load start map mutex poisoned")
            .remove(message_id);
    }

    fn record(&self, protocol: Protocol, message_id: &str) {
        let Some(started) = self
            .starts
            .lock()
            .expect("load start map mutex poisoned")
            .get(message_id)
            .copied()
        else {
            return;
        };
        let latency = u64::try_from(started.elapsed().as_micros())
            .unwrap_or(MAX_LATENCY_US)
            .min(MAX_LATENCY_US);
        let histogram = match protocol {
            Protocol::WebSocket => &self.websocket,
            Protocol::Sse => &self.sse,
            Protocol::Grpc => &self.grpc,
            Protocol::Webhook => &self.webhook,
        };
        let _ = histogram
            .lock()
            .expect("load histogram mutex poisoned")
            .record(latency);
    }

    fn count(&self, protocol: Protocol) -> u64 {
        self.histogram(protocol)
            .lock()
            .expect("load histogram mutex poisoned")
            .len()
    }

    fn histogram(&self, protocol: Protocol) -> &Mutex<Histogram<u64>> {
        match protocol {
            Protocol::WebSocket => &self.websocket,
            Protocol::Sse => &self.sse,
            Protocol::Grpc => &self.grpc,
            Protocol::Webhook => &self.webhook,
        }
    }

    fn report(&self, protocol: Protocol, expected: u64) -> ProtocolReport {
        let histogram = self
            .histogram(protocol)
            .lock()
            .expect("load histogram mutex poisoned");
        ProtocolReport {
            expected,
            received: histogram.len(),
            p50_us: percentile(&histogram, 0.50),
            p95_us: percentile(&histogram, 0.95),
            p99_us: percentile(&histogram, 0.99),
            p999_us: percentile(&histogram, 0.999),
            max_us: (!histogram.is_empty()).then(|| histogram.max()),
        }
    }
}

fn percentile(histogram: &Histogram<u64>, quantile: f64) -> Option<u64> {
    (!histogram.is_empty()).then(|| histogram.value_at_quantile(quantile))
}

#[derive(Serialize)]
struct ProtocolReport {
    expected: u64,
    received: u64,
    p50_us: Option<u64>,
    p95_us: Option<u64>,
    p99_us: Option<u64>,
    p999_us: Option<u64>,
    max_us: Option<u64>,
}

#[derive(Serialize)]
struct RunReport<'a> {
    schema_version: u32,
    run_id: &'a str,
    started_unix_ms: u128,
    elapsed_ms: u128,
    successful_publishes: u64,
    failed_publishes: u64,
    publish_throughput_per_second: f64,
    logical_cpus: usize,
    os: &'static str,
    arch: &'static str,
    args: &'a Args,
    websocket: ProtocolReport,
    sse: ProtocolReport,
    grpc: ProtocolReport,
    webhook: ProtocolReport,
}

#[derive(Clone)]
struct WebhookState {
    recorder: Arc<Recorder>,
    attempts: Arc<AtomicU64>,
    fail_every: u64,
}

async fn webhook_handler(
    State(state): State<WebhookState>,
    Json(envelope): Json<Value>,
) -> StatusCode {
    let attempt = state.attempts.fetch_add(1, Ordering::Relaxed) + 1;
    if state.fail_every > 0 && attempt.is_multiple_of(state.fail_every) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if let Some(message_id) = envelope
        .pointer("/message/metadata/message_id")
        .and_then(Value::as_str)
    {
        state.recorder.record(Protocol::Webhook, message_id);
        StatusCode::NO_CONTENT
    } else {
        StatusCode::BAD_REQUEST
    }
}

fn authorization(token: Option<&str>) -> anyhow::Result<Option<HeaderValue>> {
    token
        .map(|value| HeaderValue::from_str(&format!("Bearer {value}")))
        .transpose()
        .context("bearer token is not a valid HTTP header value")
}

async fn websocket_listener(
    args: Arc<Args>,
    recorder: Arc<Recorder>,
    index: usize,
) -> anyhow::Result<()> {
    let mut request = args.websocket_url.clone().into_client_request()?;
    if let Some(value) = authorization(args.bearer_token.as_deref())? {
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    socket
        .send(Message::Text(
            json!({
                "operation": "subscribe",
                "subscription_id": format!("load-ws-{index}"),
                "filter": { "tenant_id": args.tenant, "channel": args.channel }
            })
            .to_string()
            .into(),
        ))
        .await?;
    while let Some(frame) = socket.next().await {
        if let Message::Text(text) = frame? {
            let value: Value = serde_json::from_str(&text)?;
            if let Some(message_id) = value
                .pointer("/message/metadata/message_id")
                .and_then(Value::as_str)
            {
                recorder.record(Protocol::WebSocket, message_id);
                reader_delay(args.slow_reader_delay_ms).await;
            }
        }
    }
    Ok(())
}

async fn sse_listener(args: Arc<Args>, recorder: Arc<Recorder>) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/events", args.http_base.trim_end_matches('/'));
    let mut request = client
        .get(url)
        .query(&[("tenant_id", &args.tenant), ("channel", &args.channel)]);
    if let Some(token) = args.bearer_token.as_deref() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?.error_for_status()?;
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk?);
        if buffer.len() > MAX_SSE_BUFFER {
            bail!("SSE frame buffer exceeded {MAX_SSE_BUFFER} bytes");
        }
        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = buffer.drain(..=position).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                if !data.is_empty() {
                    let value: Value = serde_json::from_slice(&data)?;
                    if let Some(message_id) = value
                        .pointer("/message/metadata/message_id")
                        .and_then(Value::as_str)
                    {
                        recorder.record(Protocol::Sse, message_id);
                        reader_delay(args.slow_reader_delay_ms).await;
                    }
                    data.clear();
                }
            } else if let Some(value) = line.strip_prefix(b"data:") {
                if !data.is_empty() {
                    data.push(b'\n');
                }
                data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
            }
        }
    }
    Ok(())
}

async fn grpc_listener(
    args: Arc<Args>,
    recorder: Arc<Recorder>,
    index: usize,
) -> anyhow::Result<()> {
    let channel = Endpoint::from_shared(args.grpc_endpoint.clone())?
        .connect()
        .await?;
    let mut client = KafkaRouterClient::new(channel);
    let mut request = Request::new(SubscribeRequest {
        subscription_id: format!("load-grpc-{index}"),
        filter: Some(RouteFilter {
            tenant_id: args.tenant.clone(),
            channel: Some(args.channel.clone()),
            ..RouteFilter::default()
        }),
        queue_capacity: None,
    });
    if let Some(token) = args.bearer_token.as_deref() {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .context("invalid gRPC token")?,
        );
    }
    let mut stream = client.subscribe(request).await?.into_inner();
    while let Some(event) = stream.message().await? {
        if let Some(server_event::Event::Message(message)) = event.event {
            if let Some(message_id) = message
                .message
                .and_then(|routed| routed.metadata)
                .map(|metadata| metadata.message_id)
            {
                recorder.record(Protocol::Grpc, &message_id);
                reader_delay(args.slow_reader_delay_ms).await;
            }
        }
    }
    Ok(())
}

async fn reader_delay(delay_ms: u64) {
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

struct PublishItem {
    message_id: String,
    sequence: u64,
}

async fn publish_worker(
    args: Arc<Args>,
    recorder: Arc<Recorder>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<PublishItem>>>,
    successes: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.publish_timeout_secs))
        .build()
        .expect("bounded HTTP client configuration");
    loop {
        let Some(item) = receiver.lock().await.recv().await else {
            return;
        };
        recorder.start(item.message_id.clone());
        let mut request = client
            .post(format!(
                "{}/v1/publish",
                args.http_base.trim_end_matches('/')
            ))
            .json(&json!({
                "message_id": item.message_id,
                "tenant_id": args.tenant,
                "channel": args.channel,
                "content_type": "application/json",
                "payload": { "sequence": item.sequence }
            }));
        if let Some(token) = args.bearer_token.as_deref() {
            request = request.bearer_auth(token);
        }
        match request
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            Ok(_) => {
                successes.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                eprintln!("publish failed: {error}");
                recorder.remove_start(&item.message_id);
                failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn validate(args: &Args) -> anyhow::Result<()> {
    let connections = args
        .websocket_connections
        .saturating_add(args.sse_connections)
        .saturating_add(args.grpc_connections);
    if args.messages == 0 || args.messages > MAX_MESSAGES {
        bail!("messages must be within 1..={MAX_MESSAGES}");
    }
    if args.rate_per_second == 0 || args.publish_workers == 0 {
        bail!("rate-per-second and publish-workers must be positive");
    }
    if !(1..=300).contains(&args.publish_timeout_secs)
        || !(1..=3_600).contains(&args.drain_timeout_secs)
    {
        bail!("publish-timeout-secs must be 1..=300 and drain-timeout-secs must be 1..=3600");
    }
    if args.max_in_flight == 0 || args.max_in_flight > MAX_IN_FLIGHT {
        bail!("max-in-flight must be within 1..={MAX_IN_FLIGHT}");
    }
    if connections > MAX_CONNECTIONS {
        bail!("total connections must not exceed {MAX_CONNECTIONS}");
    }
    Ok(())
}

fn expected_complete(recorder: &Recorder, successful: u64, args: &Args) -> bool {
    recorder.count(Protocol::WebSocket)
        >= successful.saturating_mul(args.websocket_connections as u64)
        && recorder.count(Protocol::Sse) >= successful.saturating_mul(args.sse_connections as u64)
        && recorder.count(Protocol::Grpc) >= successful.saturating_mul(args.grpc_connections as u64)
        && recorder.count(Protocol::Webhook)
            >= successful.saturating_mul(args.expected_webhooks_per_message)
}

#[tokio::main]
#[allow(
    clippy::too_many_lines,
    reason = "the entry point keeps one visible sequence for load lifecycle and cleanup"
)]
async fn main() -> anyhow::Result<()> {
    let args = Arc::new(Args::parse());
    validate(&args)?;
    let run_id = Uuid::new_v4().to_string();
    let started_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let recorder = Arc::new(Recorder::new(usize::try_from(args.messages)?)?);
    let mut listeners: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();

    if let Some(address) = args.webhook_listen {
        let state = WebhookState {
            recorder: Arc::clone(&recorder),
            attempts: Arc::new(AtomicU64::new(0)),
            fail_every: args.webhook_fail_every,
        };
        let app = Router::new()
            .route("/webhook", post(webhook_handler))
            .with_state(state);
        let listener = TcpListener::bind(address).await?;
        listeners.push(tokio::spawn(async move {
            axum::serve(listener, app).await.context("webhook receiver")
        }));
    }
    for index in 0..args.websocket_connections {
        listeners.push(tokio::spawn(websocket_listener(
            Arc::clone(&args),
            Arc::clone(&recorder),
            index,
        )));
    }
    for _ in 0..args.sse_connections {
        listeners.push(tokio::spawn(sse_listener(
            Arc::clone(&args),
            Arc::clone(&recorder),
        )));
    }
    for index in 0..args.grpc_connections {
        listeners.push(tokio::spawn(grpc_listener(
            Arc::clone(&args),
            Arc::clone(&recorder),
            index,
        )));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    for task in &mut listeners {
        if task.is_finished() {
            task.await.context("listener task panicked")??;
            bail!("a listener exited before publishing started");
        }
    }

    let (sender, receiver) = mpsc::channel(args.max_in_flight);
    let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
    let successes = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let mut publishers = Vec::with_capacity(args.publish_workers);
    for _ in 0..args.publish_workers {
        publishers.push(tokio::spawn(publish_worker(
            Arc::clone(&args),
            Arc::clone(&recorder),
            Arc::clone(&receiver),
            Arc::clone(&successes),
            Arc::clone(&failures),
        )));
    }

    let run_started = Instant::now();
    let period = Duration::from_secs_f64(1.0 / f64::from(args.rate_per_second));
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    for sequence in 0..args.messages {
        interval.tick().await;
        sender
            .send(PublishItem {
                message_id: format!("load-{run_id}-{sequence}"),
                sequence,
            })
            .await
            .context("publisher workers stopped")?;
    }
    drop(sender);
    for publisher in publishers {
        publisher.await.context("publisher worker panicked")?;
    }

    let successful = successes.load(Ordering::Relaxed);
    let drain_deadline = Instant::now() + Duration::from_secs(args.drain_timeout_secs);
    while !expected_complete(&recorder, successful, &args) && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elapsed = run_started.elapsed();
    for listener in listeners {
        listener.abort();
        let _ = listener.await;
    }

    let expected_ws = successful.saturating_mul(args.websocket_connections as u64);
    let expected_sse = successful.saturating_mul(args.sse_connections as u64);
    let expected_grpc = successful.saturating_mul(args.grpc_connections as u64);
    let expected_webhook = successful.saturating_mul(args.expected_webhooks_per_message);
    let failed = failures.load(Ordering::Relaxed);
    let delivery_complete = expected_complete(&recorder, successful, &args);
    let report = RunReport {
        schema_version: 1,
        run_id: &run_id,
        started_unix_ms,
        elapsed_ms: elapsed.as_millis(),
        successful_publishes: successful,
        failed_publishes: failed,
        publish_throughput_per_second: f64::from(
            u32::try_from(successful).expect("successful publishes are capped below u32::MAX"),
        ) / elapsed.as_secs_f64(),
        logical_cpus: std::thread::available_parallelism()?.get(),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        args: &args,
        websocket: recorder.report(Protocol::WebSocket, expected_ws),
        sse: recorder.report(Protocol::Sse, expected_sse),
        grpc: recorder.report(Protocol::Grpc, expected_grpc),
        webhook: recorder.report(Protocol::Webhook, expected_webhook),
    };
    let output = serde_json::to_vec_pretty(&report)?;
    if let Some(parent) = args.output.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&args.output, &output).await?;
    println!("{}", String::from_utf8(output)?);
    if failed > 0 || !delivery_complete {
        bail!("load run failed: {failed} publish failures or incomplete protocol delivery");
    }
    Ok(())
}
