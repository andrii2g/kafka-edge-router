//! Concurrent subscription index and non-blocking bounded fan-out.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use serde::Deserialize;
use smallvec::SmallVec;
use tokio::sync::mpsc;

use crate::{
    ConnectionId, CoreError, DeliveryProtocol, Metrics, RouteFilter, RouteKey, RoutedMessage,
    SubscriptionId,
};

fn default_queue_capacity() -> usize {
    256
}

fn default_max_queue_capacity() -> usize {
    4_096
}

fn default_subscription_limit() -> usize {
    128
}

fn default_slow_consumer_strikes() -> u32 {
    3
}

/// Hot-path and resource limits.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RouterConfig {
    /// Per-connection bounded queue capacity.
    pub default_queue_capacity: usize,
    /// Hard cap for every live-client and webhook queue registered in the process.
    pub max_queue_capacity: usize,
    /// Maximum subscriptions on one connection.
    pub max_subscriptions_per_connection: usize,
    /// Consecutive queue-full outcomes before disconnecting a consumer.
    pub slow_consumer_strikes: u32,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            default_queue_capacity: default_queue_capacity(),
            max_queue_capacity: default_max_queue_capacity(),
            max_subscriptions_per_connection: default_subscription_limit(),
            slow_consumer_strikes: default_slow_consumer_strikes(),
        }
    }
}

/// One fan-out item delivered to a protocol-specific connection task.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// Shared immutable message.
    pub message: Arc<RoutedMessage>,
    /// All subscriptions on this connection that matched the message.
    pub subscription_ids: SmallVec<[SubscriptionId; 4]>,
}

/// Registration returned to a transport adapter.
pub struct ConnectionRegistration {
    /// Generated connection id.
    pub connection_id: ConnectionId,
    /// Exclusive bounded delivery receiver.
    pub receiver: mpsc::Receiver<Delivery>,
}

#[derive(Debug)]
struct ConnectionState {
    tenant_id: Arc<str>,
    protocol: DeliveryProtocol,
    sender: mpsc::Sender<Delivery>,
    subscriptions: HashMap<SubscriptionId, RouteFilter>,
    full_strikes: u32,
}

type Bucket = HashMap<ConnectionId, SmallVec<[SubscriptionId; 2]>>;

/// Dispatch outcome used for logs and tests.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct DispatchReport {
    /// Number of unique matching subscriptions.
    pub matched_subscriptions: usize,
    /// Connections whose queue accepted the delivery.
    pub delivered_connections: usize,
    /// Connections whose queue was full.
    pub full_connections: usize,
    /// Connections whose receiver was closed.
    pub closed_connections: usize,
}

/// Current router cardinalities and counters.
#[derive(Clone, Debug, serde::Serialize)]
pub struct RouterStatus {
    /// Active protocol connections and webhook workers.
    pub active_connections: usize,
    /// Total active subscriptions.
    pub subscriptions: usize,
    /// Atomic counter snapshot.
    pub metrics: crate::MetricsSnapshot,
}

/// Shared, lock-sharded routing engine.
pub struct Router {
    config: RouterConfig,
    connections: DashMap<ConnectionId, ConnectionState>,
    routes: DashMap<RouteKey, Bucket>,
    mutation_lock: Mutex<()>,
    metrics: Arc<Metrics>,
}

impl Router {
    /// Constructs an empty routing engine.
    pub fn new(config: RouterConfig) -> Self {
        Self {
            config,
            connections: DashMap::new(),
            routes: DashMap::new(),
            mutation_lock: Mutex::new(()),
            metrics: Arc::new(Metrics::default()),
        }
    }

    /// Returns the shared metrics registry.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Creates a bounded delivery queue and registers its sender.
    pub fn register_connection(
        &self,
        tenant_id: &str,
        protocol: DeliveryProtocol,
        queue_capacity: Option<usize>,
    ) -> Result<ConnectionRegistration, CoreError> {
        crate::ids::validate_identifier("tenant_id", tenant_id, 256)?;
        let capacity = queue_capacity.unwrap_or(self.config.default_queue_capacity);
        if capacity == 0 || capacity > self.config.max_queue_capacity {
            return Err(CoreError::InvalidQueueCapacity {
                requested: capacity,
                maximum: self.config.max_queue_capacity,
            });
        }
        let (sender, receiver) = mpsc::channel(capacity);
        let connection_id = ConnectionId::new();
        let previous = self.connections.insert(
            connection_id,
            ConnectionState {
                tenant_id: Arc::from(tenant_id),
                protocol,
                sender,
                subscriptions: HashMap::new(),
                full_strikes: 0,
            },
        );
        debug_assert!(previous.is_none(), "random connection id collision");
        self.metrics.record_protocol_opened(protocol);
        Ok(ConnectionRegistration {
            connection_id,
            receiver,
        })
    }

    /// Adds a compiled filter to a connection and the route index.
    pub fn subscribe(
        &self,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        filter: RouteFilter,
    ) -> Result<(), CoreError> {
        filter.validate()?;
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        {
            let mut connection = self
                .connections
                .get_mut(&connection_id)
                .ok_or(CoreError::ConnectionNotFound)?;
            if connection.tenant_id != filter.tenant_id {
                return Err(CoreError::TenantMismatch);
            }
            if connection.subscriptions.contains_key(&subscription_id) {
                return Err(CoreError::SubscriptionExists);
            }
            if connection.subscriptions.len() >= self.config.max_subscriptions_per_connection {
                return Err(CoreError::SubscriptionLimitReached);
            }
            let previous = connection
                .subscriptions
                .insert(subscription_id.clone(), filter.clone());
            debug_assert!(previous.is_none(), "duplicate subscription was pre-checked");
        }

        self.routes
            .entry(RouteKey::from(&filter))
            .or_default()
            .entry(connection_id)
            .or_default()
            .push(subscription_id);
        Ok(())
    }

    /// Removes one filter from a connection and the route index.
    pub fn unsubscribe(
        &self,
        connection_id: ConnectionId,
        subscription_id: &SubscriptionId,
    ) -> Result<(), CoreError> {
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let filter = {
            let mut connection = self
                .connections
                .get_mut(&connection_id)
                .ok_or(CoreError::ConnectionNotFound)?;
            connection
                .subscriptions
                .remove(subscription_id)
                .ok_or(CoreError::SubscriptionNotFound)?
        };
        self.remove_from_route(connection_id, subscription_id, &filter);
        Ok(())
    }

    /// Removes a connection and every indexed subscription. Safe to call repeatedly.
    pub fn unregister_connection(&self, connection_id: ConnectionId) {
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some((_, connection)) = self.connections.remove(&connection_id) else {
            return;
        };
        for (subscription_id, filter) in connection.subscriptions {
            self.remove_from_route(connection_id, &subscription_id, &filter);
        }
    }

    /// Performs indexed matching and bounded non-blocking queue fan-out.
    pub fn dispatch(&self, message: Arc<RoutedMessage>) -> DispatchReport {
        self.metrics.record_valid_message();
        let mut matches: HashMap<ConnectionId, SmallVec<[SubscriptionId; 4]>> = HashMap::new();
        for candidate in RouteKey::candidates(&message.metadata) {
            if let Some(bucket) = self.routes.get(&candidate) {
                for (connection_id, subscription_ids) in bucket.iter() {
                    let entry = matches.entry(*connection_id).or_default();
                    for subscription_id in subscription_ids {
                        if !entry.contains(subscription_id) {
                            entry.push(subscription_id.clone());
                        }
                    }
                }
            }
        }

        let matched_subscriptions = matches.values().map(|ids| ids.len()).sum();
        let mut report = DispatchReport {
            matched_subscriptions,
            ..DispatchReport::default()
        };
        let mut disconnect = Vec::new();

        for (connection_id, subscription_ids) in matches {
            let Some(mut connection) = self.connections.get_mut(&connection_id) else {
                report.closed_connections += 1;
                continue;
            };
            let delivery = Delivery {
                message: Arc::clone(&message),
                subscription_ids,
            };
            match connection.sender.try_send(delivery) {
                Ok(()) => {
                    connection.full_strikes = 0;
                    report.delivered_connections += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    connection.full_strikes = connection.full_strikes.saturating_add(1);
                    report.full_connections += 1;
                    if connection.full_strikes >= self.config.slow_consumer_strikes.max(1) {
                        disconnect.push(connection_id);
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    report.closed_connections += 1;
                    disconnect.push(connection_id);
                }
            }
        }

        for connection_id in disconnect {
            self.metrics.record_slow_disconnect();
            self.unregister_connection(connection_id);
        }

        self.metrics.record_dispatch(
            report.matched_subscriptions,
            report.delivered_connections,
            report.full_connections,
            report.closed_connections,
        );
        report
    }

    /// Returns current cardinalities and atomic counters.
    pub fn status(&self) -> RouterStatus {
        RouterStatus {
            active_connections: self.connections.len(),
            subscriptions: self
                .connections
                .iter()
                .map(|entry| entry.subscriptions.len())
                .sum(),
            metrics: self.metrics.snapshot(),
        }
    }

    /// Counts active connections by protocol for diagnostics.
    pub fn connections_by_protocol(&self, protocol: DeliveryProtocol) -> usize {
        self.connections
            .iter()
            .filter(|entry| entry.protocol == protocol)
            .count()
    }

    fn remove_from_route(
        &self,
        connection_id: ConnectionId,
        subscription_id: &SubscriptionId,
        filter: &RouteFilter,
    ) {
        let key = RouteKey::from(filter);
        if let Some(mut bucket) = self.routes.get_mut(&key) {
            if let Some(subscription_ids) = bucket.get_mut(&connection_id) {
                subscription_ids.retain(|candidate| candidate != subscription_id);
                if subscription_ids.is_empty() {
                    let removed = bucket.remove(&connection_id);
                    debug_assert!(removed.is_some(), "route bucket entry disappeared");
                }
            }
        }
        let _removed = self.routes.remove_if(&key, |_, bucket| bucket.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::{Router, RouterConfig};
    use crate::{
        DeliveryProtocol, RouteFilter, RoutedMessage, RoutingMetadata, SubscriptionId,
    };

    fn filter(tenant: &str, channel: Option<&str>) -> RouteFilter {
        RouteFilter {
            tenant_id: Arc::from(tenant),
            kind: None,
            message_type: None,
            channel: channel.map(Arc::from),
            actor_id: None,
            audience_type: None,
            audience_id: None,
        }
    }

    fn message(tenant: &str, channel: &str) -> Arc<RoutedMessage> {
        Arc::new(
            RoutedMessage::new(
                RoutingMetadata {
                    message_id: Arc::from("m-1"),
                    tenant_id: Arc::from(tenant),
                    kind: Some(Arc::from("content")),
                    message_type: None,
                    channel: Some(Arc::from(channel)),
                    actor_id: None,
                    audience_type: None,
                    audience_id: None,
                    content_type: Arc::from("application/json"),
                    timestamp_ms: None,
                    source: None,
                },
                Bytes::from_static(br#"{"ok":true}"#),
            )
            .expect("valid message"),
        )
    }

    #[tokio::test]
    async fn deduplicates_multiple_matching_filters_per_connection() {
        let router = Router::new(RouterConfig::default());
        let mut registration = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("registration");
        router
            .subscribe(
                registration.connection_id,
                SubscriptionId::new("all").expect("id"),
                filter("tenant-a", None),
            )
            .expect("subscribe");
        router
            .subscribe(
                registration.connection_id,
                SubscriptionId::new("news").expect("id"),
                filter("tenant-a", Some("news")),
            )
            .expect("subscribe");

        let report = router.dispatch(message("tenant-a", "news"));
        assert_eq!(report.matched_subscriptions, 2);
        assert_eq!(report.delivered_connections, 1);
        let delivery = registration.receiver.recv().await.expect("delivery");
        assert_eq!(delivery.subscription_ids.len(), 2);
    }

    #[test]
    fn rejects_cross_tenant_subscription() {
        let router = Router::new(RouterConfig::default());
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::Sse, None)
            .expect("registration");
        let result = router.subscribe(
            registration.connection_id,
            SubscriptionId::new("bad").expect("id"),
            filter("tenant-b", None),
        );
        assert!(matches!(result, Err(crate::CoreError::TenantMismatch)));
    }

    #[test]
    fn bounded_queue_triggers_slow_consumer_policy() {
        let router = Router::new(RouterConfig {
            default_queue_capacity: 1,
            max_queue_capacity: 16,
            max_subscriptions_per_connection: 10,
            slow_consumer_strikes: 2,
        });
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::Sse, None)
            .expect("registration");
        router
            .subscribe(
                registration.connection_id,
                SubscriptionId::new("all").expect("id"),
                filter("tenant-a", None),
            )
            .expect("subscribe");
        assert_eq!(router.dispatch(message("tenant-a", "news")).delivered_connections, 1);
        assert_eq!(router.dispatch(message("tenant-a", "news")).full_connections, 1);
        assert_eq!(router.dispatch(message("tenant-a", "news")).full_connections, 1);
        assert_eq!(router.status().active_connections, 0);
    }

    #[test]
    fn rejects_queue_capacity_above_process_cap() {
        let router = Router::new(RouterConfig {
            default_queue_capacity: 4,
            max_queue_capacity: 8,
            max_subscriptions_per_connection: 10,
            slow_consumer_strikes: 2,
        });
        let result = router.register_connection(
            "tenant-a",
            DeliveryProtocol::WebSocket,
            Some(9),
        );
        assert!(matches!(
            result,
            Err(crate::CoreError::InvalidQueueCapacity {
                requested: 9,
                maximum: 8
            })
        ));
    }
}
