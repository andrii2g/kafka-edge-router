//! `routerd` process composition and graceful lifecycle.

mod config;
mod readiness;
mod telemetry;

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use config::AppConfig;
use router_api::{serve_grpc, serve_http, ApiState, Authenticator, HealthState};
use router_core::{MessagePublisher, Router};
use router_kafka::{KafkaIngestor, KafkaPublisher};
use router_webhook::WebhookManager;
use tokio::{net::TcpListener, sync::watch, task::JoinSet, time::timeout};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Arguments {
    /// Path to daemon TOML configuration.
    #[arg(long, env = "ROUTER_CONFIG", default_value = "config/router.toml")]
    config: PathBuf,
    /// Parse and validate configuration, then exit.
    #[arg(long)]
    check_config: bool,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let configuration = AppConfig::load(&arguments.config)?;
    let telemetry = telemetry::TelemetryGuard::init(
        &configuration.logging,
        &configuration.observability.opentelemetry,
    );
    if arguments.check_config {
        info!(path = %arguments.config.display(), "configuration is valid");
        telemetry.shutdown();
        return Ok(());
    }

    let authenticator = Authenticator::new(configuration.auth.clone());
    authenticator
        .reload_jwks()
        .await
        .map_err(anyhow::Error::msg)
        .context("failed to load initial JWT key set")?;

    let health = Arc::new(HealthState::default());
    health.set_live(true);
    let router = Arc::new(Router::new(configuration.router.clone()));

    let publisher: Option<Arc<dyn MessagePublisher>> = if configuration.kafka.producer.enabled {
        Some(Arc::new(
            KafkaPublisher::new(&configuration.kafka.producer)
                .context("failed to construct Kafka producer")?,
        ))
    } else {
        None
    };
    let webhook_manager = WebhookManager::new(&configuration.webhooks, &router)
        .context("failed to construct webhook manager")?;
    let pre_commit_sinks = webhook_manager.pre_commit_sink().into_iter().collect();
    let ingestor = KafkaIngestor::with_pre_commit_sinks(
        &configuration.kafka.consumer,
        Arc::clone(&router),
        pre_commit_sinks,
    )
    .context("failed to construct Kafka consumer")?;
    let kafka_health = ingestor.health();
    let api_state = ApiState::with_authenticator(
        Arc::clone(&router),
        authenticator.clone(),
        publisher,
        Arc::clone(&health),
        configuration.api.clone(),
    );

    let (http_listener, grpc_listener) = bind_listeners(&configuration).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks: JoinSet<(&'static str, anyhow::Result<()>)> = JoinSet::new();

    if let Some(refresh_interval) = authenticator.jwks_refresh_interval() {
        let jwt_authenticator = authenticator;
        let mut jwt_shutdown = shutdown_rx.clone();
        let _jwt_task = tasks.spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(error) = jwt_authenticator.reload_jwks().await {
                            warn!(%error, "JWKS reload failed; retaining previous key set");
                        }
                    }
                    changed = jwt_shutdown.changed() => {
                        if changed.is_err() || *jwt_shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            ("jwks-refresh", Ok(()))
        });
    }
    let _kafka_task = tasks.spawn({
        let shutdown = shutdown_rx.clone();
        async move {
            ingestor.run(shutdown).await;
            ("kafka", Ok(()))
        }
    });
    let _http_task = tasks.spawn({
        let state = api_state.clone();
        let shutdown = shutdown_rx.clone();
        async move {
            let result = serve_http(http_listener, state, shutdown)
                .await
                .context("HTTP server failed");
            ("http", result)
        }
    });
    let _grpc_task = tasks.spawn({
        let state = api_state.clone();
        let shutdown = shutdown_rx.clone();
        async move {
            let result = serve_grpc(grpc_listener, state, shutdown)
                .await
                .context("gRPC server failed");
            ("grpc", result)
        }
    });
    let _webhook_task = tasks.spawn({
        let shutdown = shutdown_rx.clone();
        async move {
            let result = webhook_manager
                .run(shutdown)
                .await
                .context("webhook manager failed");
            ("webhooks", result)
        }
    });

    if configuration.observability.kafka_readiness.enabled {
        let readiness_config = configuration.observability.kafka_readiness.clone();
        let readiness_health = Arc::clone(&health);
        let readiness_shutdown = shutdown_rx;
        let _readiness_task = tasks.spawn(async move {
            readiness::monitor_kafka_readiness(
                readiness_config,
                kafka_health,
                readiness_health,
                readiness_shutdown,
            )
            .await;
            ("kafka-readiness", Ok(()))
        });
    } else {
        health.set_ready(true);
    }
    info!(
        http_addr = %configuration.server.http_addr,
        grpc_addr = %configuration.server.grpc_addr,
        kafka_readiness = configuration.observability.kafka_readiness.enabled,
        "router listeners started"
    );

    tokio::select! {
        signal = shutdown_signal() => {
            match signal {
                Ok(signal) => info!(%signal, "shutdown requested"),
                Err(error) => error!(%error, "signal handler failed; shutting down"),
            }
        }
        component = tasks.join_next() => {
            match component {
                Some(Ok((name, Ok(())))) => warn!(component = name, "component exited unexpectedly"),
                Some(Ok((name, Err(error)))) => error!(component = name, %error, "component failed"),
                Some(Err(error)) => error!(%error, "component task panicked or was cancelled"),
                None => warn!("all components exited"),
            }
        }
    }

    let grace = Duration::from_secs(configuration.server.shutdown_grace_secs.max(1));
    drain_components(&health, shutdown_tx, tasks, grace).await;
    telemetry.shutdown();
    Ok(())
}

async fn bind_listeners(configuration: &AppConfig) -> anyhow::Result<(TcpListener, TcpListener)> {
    let http_listener = TcpListener::bind(&configuration.server.http_addr)
        .await
        .with_context(|| {
            format!(
                "failed to bind HTTP listener {}",
                configuration.server.http_addr
            )
        })?;
    let grpc_listener = TcpListener::bind(&configuration.server.grpc_addr)
        .await
        .with_context(|| {
            format!(
                "failed to bind gRPC listener {}",
                configuration.server.grpc_addr
            )
        })?;
    Ok((http_listener, grpc_listener))
}

async fn drain_components(
    health: &HealthState,
    shutdown_tx: watch::Sender<bool>,
    mut tasks: JoinSet<(&'static str, anyhow::Result<()>)>,
    grace: Duration,
) {
    health.set_ready(false);
    let _ = shutdown_tx.send(true);
    let drained = timeout(grace, async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((name, Ok(()))) => info!(component = name, "component stopped"),
                Ok((name, Err(error))) => {
                    error!(component = name, %error, "component stopped with error");
                }
                Err(error) => error!(%error, "component task failed during shutdown"),
            }
        }
    })
    .await;
    if drained.is_err() {
        warn!(
            grace_seconds = grace.as_secs(),
            "shutdown deadline reached; aborting tasks"
        );
        tasks.abort_all();
    }
    health.set_live(false);
    info!("router stopped");
}

async fn shutdown_signal() -> anyhow::Result<&'static str> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok("SIGINT")
            }
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("CTRL_C")
    }
}
