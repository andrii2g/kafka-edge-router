//! Bounded Kafka connectivity health shared with daemon readiness.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Cheap, cloneable Kafka health signal updated by consumer callbacks.
#[derive(Clone, Debug, Default)]
pub struct KafkaHealth {
    inner: Arc<KafkaHealthInner>,
}

#[derive(Debug, Default)]
struct KafkaHealthInner {
    connected: AtomicBool,
    last_healthy_epoch_millis: AtomicU64,
}

impl KafkaHealth {
    pub(crate) fn mark_healthy(&self) {
        self.inner.connected.store(true, Ordering::Relaxed);
        self.inner
            .last_healthy_epoch_millis
            .store(epoch_millis(), Ordering::Relaxed);
    }

    pub(crate) fn mark_unhealthy(&self) {
        self.inner.connected.store(false, Ordering::Relaxed);
    }

    /// Returns true while Kafka is connected and has reported health recently.
    pub fn is_healthy(&self, stale_after: Duration) -> bool {
        if !self.inner.connected.load(Ordering::Relaxed) {
            return false;
        }
        let last = self.inner.last_healthy_epoch_millis.load(Ordering::Relaxed);
        let stale_after_millis = u64::try_from(stale_after.as_millis()).unwrap_or(u64::MAX);
        last != 0 && epoch_millis().saturating_sub(last) <= stale_after_millis
    }
}

fn epoch_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::KafkaHealth;

    #[test]
    fn health_requires_a_recent_connected_observation() {
        let health = KafkaHealth::default();
        assert!(!health.is_healthy(Duration::from_secs(1)));
        health.mark_healthy();
        assert!(health.is_healthy(Duration::from_secs(1)));
        health.mark_unhealthy();
        assert!(!health.is_healthy(Duration::from_secs(1)));
    }
}
