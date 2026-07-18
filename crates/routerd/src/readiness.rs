//! Optional Kafka-dependent readiness with bounded hysteresis.

use std::{sync::Arc, time::Duration};

use router_api::HealthState;
use router_kafka::KafkaHealth;
use tokio::{sync::watch, time};

use crate::config::KafkaReadinessConfig;

#[derive(Debug)]
struct ReadinessHysteresis {
    ready: bool,
    successes: u32,
    failures: u32,
    success_threshold: u32,
    failure_threshold: u32,
}

impl ReadinessHysteresis {
    fn new(success_threshold: u32, failure_threshold: u32) -> Self {
        Self {
            ready: false,
            successes: 0,
            failures: 0,
            success_threshold,
            failure_threshold,
        }
    }

    fn observe(&mut self, healthy: bool) -> bool {
        if healthy {
            self.failures = 0;
            self.successes = self.successes.saturating_add(1);
            if self.successes >= self.success_threshold {
                self.ready = true;
            }
        } else {
            self.successes = 0;
            self.failures = self.failures.saturating_add(1);
            if self.failures >= self.failure_threshold {
                self.ready = false;
            }
        }
        self.ready
    }
}

pub(crate) async fn monitor_kafka_readiness(
    config: KafkaReadinessConfig,
    kafka: KafkaHealth,
    health: Arc<HealthState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut hysteresis =
        ReadinessHysteresis::new(config.success_threshold, config.failure_threshold);
    let mut interval = time::interval(Duration::from_millis(config.check_interval_ms));
    let stale_after = Duration::from_secs(config.stale_after_secs);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                health.set_ready(hysteresis.observe(kafka.is_healthy(stale_after)));
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ReadinessHysteresis;

    #[test]
    fn transitions_only_at_configured_thresholds() {
        let mut state = ReadinessHysteresis::new(2, 3);
        assert!(!state.observe(true));
        assert!(state.observe(true));
        assert!(state.observe(false));
        assert!(state.observe(false));
        assert!(!state.observe(false));
        assert!(!state.observe(true));
        assert!(state.observe(true));
    }
}
