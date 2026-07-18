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
        let route_key = RouteKey::from(&filter);
        let _mutation_guard = self
            .mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                .insert(subscription_id.clone(), filter);
            debug_assert!(previous.is_none(), "duplicate subscription was pre-checked");
        }

        self.routes
            .entry(route_key)
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
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

        let matched_subscriptions = matches.values().map(SmallVec::len).sum();
        let mut report = DispatchReport {
            matched_subscriptions,
            ..DispatchReport::default()
        };
        let mut disconnect = Vec::new();

        let connection_count = matches.len();
        let mut message = Some(message);
        for (index, (connection_id, subscription_ids)) in matches.into_iter().enumerate() {
            let Some(mut connection) = self.connections.get_mut(&connection_id) else {
                report.closed_connections += 1;
                continue;
            };
            let delivery_message = if index + 1 == connection_count {
                message
                    .take()
                    .expect("final delivery owns the routed message")
            } else {
                Arc::clone(
                    message
                        .as_ref()
                        .expect("routed message remains available before final delivery"),
                )
            };
            let delivery = Delivery {
                message: delivery_message,
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
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use bytes::Bytes;

    use super::{Router, RouterConfig};
    use crate::{
        CoreError, DeliveryProtocol, RouteFilter, RoutedMessage, RoutingMetadata, SubscriptionId,
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

    fn test_config(slow_consumer_strikes: u32) -> RouterConfig {
        RouterConfig {
            default_queue_capacity: 1,
            max_queue_capacity: 16,
            max_subscriptions_per_connection: 10,
            slow_consumer_strikes,
        }
    }

    fn subscribe_all(router: &Router, connection_id: crate::ConnectionId, id: &str) {
        router
            .subscribe(
                connection_id,
                SubscriptionId::new(id).expect("id"),
                filter("tenant-a", None),
            )
            .expect("subscribe");
    }

    #[tokio::test]
    async fn deduplicates_multiple_matching_filters_per_connection() {
        let router = Router::new(RouterConfig::default());
        let mut registration = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("registration");
        subscribe_all(&router, registration.connection_id, "all");
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
        assert!(matches!(result, Err(CoreError::TenantMismatch)));
    }

    #[test]
    fn zero_and_one_strike_both_disconnect_on_first_full_queue() {
        for strikes in [0, 1] {
            let router = Router::new(test_config(strikes));
            let registration = router
                .register_connection("tenant-a", DeliveryProtocol::Sse, None)
                .expect("registration");
            subscribe_all(&router, registration.connection_id, "all");

            assert_eq!(
                router
                    .dispatch(message("tenant-a", "news"))
                    .delivered_connections,
                1
            );
            let report = router.dispatch(message("tenant-a", "news"));
            assert_eq!(report.full_connections, 1, "strike setting {strikes}");
            assert_eq!(router.status().active_connections, 0);
            assert!(router.routes.is_empty());
        }
    }

    #[test]
    fn bounded_queue_triggers_configured_slow_consumer_policy() {
        let router = Router::new(test_config(2));
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::Sse, None)
            .expect("registration");
        subscribe_all(&router, registration.connection_id, "all");
        assert_eq!(
            router
                .dispatch(message("tenant-a", "news"))
                .delivered_connections,
            1
        );
        assert_eq!(
            router
                .dispatch(message("tenant-a", "news"))
                .full_connections,
            1
        );
        assert_eq!(router.status().active_connections, 1);
        assert_eq!(
            router
                .dispatch(message("tenant-a", "news"))
                .full_connections,
            1
        );
        assert_eq!(router.status().active_connections, 0);
        assert!(router.routes.is_empty());
    }

    #[test]
    fn queue_capacity_accepts_exact_limit_and_rejects_zero_and_over_limit() {
        let router = Router::new(RouterConfig {
            default_queue_capacity: 4,
            max_queue_capacity: 8,
            max_subscriptions_per_connection: 10,
            slow_consumer_strikes: 2,
        });

        let zero = router.register_connection("tenant-a", DeliveryProtocol::WebSocket, Some(0));
        assert!(matches!(
            zero,
            Err(CoreError::InvalidQueueCapacity {
                requested: 0,
                maximum: 8
            })
        ));
        assert!(router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, Some(8))
            .is_ok());
        let over = router.register_connection("tenant-a", DeliveryProtocol::WebSocket, Some(9));
        assert!(matches!(
            over,
            Err(CoreError::InvalidQueueCapacity {
                requested: 9,
                maximum: 8
            })
        ));
    }

    #[test]
    fn duplicate_subscription_insertion_is_atomic() {
        let router = Arc::new(Router::new(RouterConfig::default()));
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("registration");
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let connection_id = registration.connection_id;
            workers.push(thread::spawn(move || {
                barrier.wait();
                router.subscribe(
                    connection_id,
                    SubscriptionId::new("duplicate").expect("id"),
                    filter("tenant-a", None),
                )
            }));
        }
        barrier.wait();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .collect();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(CoreError::SubscriptionExists)))
                .count(),
            1
        );
        assert_eq!(router.status().subscriptions, 1);
        assert_eq!(router.routes.len(), 1);
        assert_eq!(
            router
                .dispatch(message("tenant-a", "news"))
                .matched_subscriptions,
            1
        );
    }

    #[test]
    fn subscribe_racing_unregister_leaves_no_index_entries() {
        let router = Arc::new(Router::new(RouterConfig::default()));
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("registration");
        let barrier = Arc::new(Barrier::new(3));

        let subscribe = {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let connection_id = registration.connection_id;
            thread::spawn(move || {
                barrier.wait();
                router.subscribe(
                    connection_id,
                    SubscriptionId::new("racing").expect("id"),
                    filter("tenant-a", None),
                )
            })
        };
        let unregister = {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let connection_id = registration.connection_id;
            thread::spawn(move || {
                barrier.wait();
                router.unregister_connection(connection_id);
            })
        };

        barrier.wait();
        let subscribe_result = subscribe.join().expect("subscribe worker");
        unregister.join().expect("unregister worker");
        assert!(
            subscribe_result.is_ok()
                || matches!(subscribe_result, Err(CoreError::ConnectionNotFound))
        );
        assert_eq!(router.status().active_connections, 0);
        assert_eq!(router.status().subscriptions, 0);
        assert!(router.routes.is_empty());
    }

    #[test]
    fn unsubscribe_racing_dispatch_has_bounded_in_flight_semantics() {
        let router = Arc::new(Router::new(RouterConfig::default()));
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("registration");
        let subscription_id = SubscriptionId::new("racing").expect("id");
        router
            .subscribe(
                registration.connection_id,
                subscription_id.clone(),
                filter("tenant-a", None),
            )
            .expect("subscribe");
        let barrier = Arc::new(Barrier::new(3));

        let unsubscribe = {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let subscription_id = subscription_id.clone();
            let connection_id = registration.connection_id;
            thread::spawn(move || {
                barrier.wait();
                router.unsubscribe(connection_id, &subscription_id)
            })
        };
        let dispatch = {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                router.dispatch(message("tenant-a", "news"))
            })
        };

        barrier.wait();
        unsubscribe
            .join()
            .expect("unsubscribe worker")
            .expect("unsubscribe");
        let report = dispatch.join().expect("dispatch worker");
        assert!(report.matched_subscriptions <= 1);
        assert!(report.delivered_connections <= 1);
        assert_eq!(router.status().subscriptions, 0);
        assert!(router.routes.is_empty());
        let after_unsubscribe = router.dispatch(message("tenant-a", "news"));
        assert_eq!(after_unsubscribe.matched_subscriptions, 0);
        assert_eq!(after_unsubscribe.delivered_connections, 0);
    }

    #[test]
    fn concurrent_repeated_unregister_is_idempotent() {
        let router = Arc::new(Router::new(RouterConfig::default()));
        let registration = router
            .register_connection("tenant-a", DeliveryProtocol::Grpc, None)
            .expect("registration");
        subscribe_all(&router, registration.connection_id, "all");
        let barrier = Arc::new(Barrier::new(9));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let connection_id = registration.connection_id;
            workers.push(thread::spawn(move || {
                barrier.wait();
                router.unregister_connection(connection_id);
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().expect("unregister worker");
        }
        assert_eq!(router.status().active_connections, 0);
        assert!(router.routes.is_empty());
    }

    #[test]
    fn cleanup_cannot_remove_a_concurrently_repopulated_bucket() {
        let router = Arc::new(Router::new(RouterConfig::default()));
        let first = router
            .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
            .expect("first registration");
        let mut second = router
            .register_connection("tenant-a", DeliveryProtocol::Sse, None)
            .expect("second registration");
        let first_id = SubscriptionId::new("first").expect("id");
        router
            .subscribe(
                first.connection_id,
                first_id.clone(),
                filter("tenant-a", Some("news")),
            )
            .expect("first subscribe");
        let barrier = Arc::new(Barrier::new(3));

        let remove = {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let connection_id = first.connection_id;
            thread::spawn(move || {
                barrier.wait();
                router.unsubscribe(connection_id, &first_id)
            })
        };
        let repopulate = {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            let connection_id = second.connection_id;
            thread::spawn(move || {
                barrier.wait();
                router.subscribe(
                    connection_id,
                    SubscriptionId::new("second").expect("id"),
                    filter("tenant-a", Some("news")),
                )
            })
        };

        barrier.wait();
        remove.join().expect("remove worker").expect("unsubscribe");
        repopulate
            .join()
            .expect("repopulate worker")
            .expect("subscribe");
        assert_eq!(router.routes.len(), 1);
        assert_eq!(router.status().subscriptions, 1);
        let report = router.dispatch(message("tenant-a", "news"));
        assert_eq!(report.matched_subscriptions, 1);
        assert_eq!(report.delivered_connections, 1);
        let delivery = second.receiver.try_recv().expect("second delivery");
        assert_eq!(delivery.subscription_ids[0].as_str(), "second");
    }

    #[test]
    fn route_cardinality_returns_to_zero_after_churn() {
        let router = Router::new(RouterConfig::default());
        for iteration in 0..2_000 {
            let registration = router
                .register_connection("tenant-a", DeliveryProtocol::WebSocket, None)
                .expect("registration");
            let subscription_id =
                SubscriptionId::new(format!("sub-{iteration}")).expect("subscription id");
            router
                .subscribe(
                    registration.connection_id,
                    subscription_id.clone(),
                    filter("tenant-a", Some("news")),
                )
                .expect("subscribe");
            if iteration % 2 == 0 {
                router
                    .unsubscribe(registration.connection_id, &subscription_id)
                    .expect("unsubscribe");
            }
            router.unregister_connection(registration.connection_id);
        }
        assert_eq!(router.status().active_connections, 0);
        assert_eq!(router.status().subscriptions, 0);
        assert!(router.routes.is_empty());
    }
}
