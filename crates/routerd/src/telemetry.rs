//! Nonfatal OpenTelemetry trace export and bounded provider shutdown.

use std::time::Duration;

use anyhow::Context as _;
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::{Protocol, WithExportConfig as _};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

use crate::config::{LoggingConfig, OpenTelemetryConfig};

pub(crate) struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
    shutdown_timeout: Duration,
}

impl TelemetryGuard {
    pub(crate) fn init(logging: &LoggingConfig, config: &OpenTelemetryConfig) -> Self {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let provider = if config.enabled {
            match build_provider(config) {
                Ok(provider) => Some(provider),
                Err(error) => {
                    eprintln!(
                        "OpenTelemetry initialization failed; continuing without export: {error:#}"
                    );
                    None
                }
            }
        } else {
            None
        };

        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(logging.filter.clone()));
        let telemetry_layer = provider.as_ref().map(|provider| {
            let tracer = provider.tracer("routerd");
            tracing_opentelemetry::layer().with_tracer(tracer)
        });

        let initialized = if logging.json {
            tracing_subscriber::registry()
                .with(filter)
                .with(telemetry_layer)
                .with(tracing_subscriber::fmt::layer().json())
                .try_init()
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(telemetry_layer)
                .with(tracing_subscriber::fmt::layer().compact())
                .try_init()
        };
        if let Err(error) = initialized {
            eprintln!("tracing subscriber initialization failed: {error}");
        }

        if let Some(provider) = &provider {
            global::set_tracer_provider(provider.clone());
        }
        Self {
            provider,
            shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
        }
    }

    pub(crate) fn shutdown(mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(error) = provider.shutdown_with_timeout(self.shutdown_timeout) {
                tracing::warn!(%error, "OpenTelemetry shutdown did not complete cleanly");
            }
        }
    }
}

fn build_provider(config: &OpenTelemetryConfig) -> anyhow::Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(config.endpoint.clone())
        .with_timeout(Duration::from_millis(config.timeout_ms))
        .build()
        .context("failed to build OTLP HTTP span exporter")?;
    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .build();
    Ok(SdkTracerProvider::builder()
        .with_sampler(Sampler::TraceIdRatioBased(config.sampling_ratio))
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use opentelemetry_sdk::trace::SdkTracerProvider;

    use super::TelemetryGuard;

    #[test]
    fn trace_provider_shutdown_is_bounded_and_nonfatal() {
        let guard = TelemetryGuard {
            provider: Some(SdkTracerProvider::builder().build()),
            shutdown_timeout: Duration::from_secs(1),
        };
        guard.shutdown();
    }
}
