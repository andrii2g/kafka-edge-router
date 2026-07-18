//! Broker-backed proof of Kafka routing and offset semantics.

use std::{
    collections::BTreeMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    ClientConfig,
};
use router_core::{
    DeliveryProtocol, MessagePublisher, PublishCommand, RouteFilter, Router, RouterConfig,
    SubscriptionId,
};
use router_kafka::{
    decode_message, KafkaConsumerConfig, KafkaIngestor, KafkaProducerConfig, KafkaPublisher,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, timeout},
};
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(30);

struct KafkaHarness {
    brokers: String,
    topic: String,
    group: String,
}

impl KafkaHarness {
    async fn new(test_name: &str, partitions: usize) -> Option<Self> {
        let brokers = match env::var("KAFKA_TEST_BROKERS") {
            Ok(brokers) => brokers,
            Err(error) if env::var("KAFKA_INTEGRATION_REQUIRED").as_deref() == Ok("1") => {
                panic!("KAFKA_TEST_BROKERS is required: {error}")
            }
            Err(error) => {
                eprintln!(
                    "skipping Kafka integration test; KAFKA_TEST_BROKERS is not set: {error}"
                );
                return None;
            }
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let topic = format!("router-{test_name}-{suffix}");
        let group = format!("router-{test_name}-{suffix}");

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .expect("admin client");
        let result = admin
            .create_topics(
                &[NewTopic::new(
                    &topic,
                    i32::try_from(partitions).expect("partition count fits i32"),
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new().operation_timeout(Some(DEADLINE)),
            )
            .await
            .expect("create topic request");
        assert!(
            result.iter().all(Result::is_ok),
            "topic creation failed: {result:?}"
        );

        let harness = Self {
            brokers,
            topic,
            group,
        };
        harness
            .poll_until("topic metadata", || {
                let consumer = harness.probe_consumer("metadata");
                consumer
                    .fetch_metadata(Some(&harness.topic), Timeout::After(Duration::from_secs(1)))
                    .is_ok_and(|metadata| {
                        metadata
                            .topics()
                            .first()
                            .is_some_and(|topic| topic.partitions().len() == partitions)
                    })
            })
            .await;
        Some(harness)
    }

    fn consumer_config(&self, client: &str, commit_invalid_messages: bool) -> KafkaConsumerConfig {
        KafkaConsumerConfig {
            brokers: self.brokers.clone(),
            group_id: self.group.clone(),
            client_id: format!("{client}-{}", Uuid::new_v4().simple()),
            topics: vec![self.topic.clone()],
            auto_offset_reset: "earliest".to_owned(),
            max_payload_bytes: 1024,
            commit_invalid_messages,
            properties: BTreeMap::new(),
        }
    }

    fn probe_consumer(&self, suffix: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group)
            .set("client.id", format!("probe-{suffix}"))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("probe consumer")
    }

    fn committed_offset(&self, partition: i32) -> Option<i64> {
        let consumer = self.probe_consumer("offset");
        let mut requested = TopicPartitionList::new();
        requested.add_partition(&self.topic, partition);
        let offsets = consumer
            .committed_offsets(requested, Timeout::After(Duration::from_secs(2)))
            .expect("fetch committed offsets");
        match offsets
            .find_partition(&self.topic, partition)
            .expect("requested partition")
            .offset()
        {
            Offset::Offset(offset) => Some(offset),
            _ => None,
        }
    }

    async fn wait_for_commit(&self, partition: i32, expected: i64) {
        self.poll_until("committed offset", || {
            self.committed_offset(partition)
                .is_some_and(|offset| offset >= expected)
        })
        .await;
    }

    async fn poll_until(&self, description: &str, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + DEADLINE;
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn produce_raw(&self, key: &str, payload: &[u8], headers: OwnedHeaders) -> (i32, i64) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .expect("raw producer");
        let record = FutureRecord::to(&self.topic)
            .key(key)
            .payload(payload)
            .headers(headers);
        let delivery = producer
            .send(record, Timeout::After(DEADLINE))
            .await
            .expect("produce record");
        (delivery.partition, delivery.offset)
    }
}

fn router_with_subscription() -> (
    Arc<Router>,
    tokio::sync::mpsc::Receiver<router_core::Delivery>,
) {
    let router = Arc::new(Router::new(RouterConfig {
        default_queue_capacity: 32,
        max_queue_capacity: 64,
        max_subscriptions_per_connection: 8,
        slow_consumer_strikes: 3,
    }));
    let registration = router
        .register_connection("tenant-a", DeliveryProtocol::Grpc, None)
        .expect("register test receiver");
    router
        .subscribe(
            registration.connection_id,
            SubscriptionId::new("all").expect("subscription id"),
            RouteFilter {
                tenant_id: Arc::from("tenant-a"),
                kind: None,
                message_type: None,
                channel: None,
                actor_id: None,
                audience_type: None,
                audience_id: None,
            },
        )
        .expect("subscribe test receiver");
    (router, registration.receiver)
}

fn spawn_ingestor(
    config: &KafkaConsumerConfig,
    router: Arc<Router>,
) -> (watch::Sender<bool>, JoinHandle<()>) {
    let ingestor = KafkaIngestor::new(config, router).expect("Kafka ingestor");
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(ingestor.run(receiver));
    (shutdown, task)
}

fn valid_headers(message_id: &'static str) -> OwnedHeaders {
    OwnedHeaders::new()
        .insert(Header {
            key: "x-tenant-id",
            value: Some(b"tenant-a"),
        })
        .insert(Header {
            key: "x-message-id",
            value: Some(message_id.as_bytes()),
        })
        .insert(Header {
            key: "x-channel",
            value: Some(b"news"),
        })
}

fn command(index: u8) -> PublishCommand {
    PublishCommand {
        message_id: Some(Arc::from(format!("ordered-{index}"))),
        tenant_id: Arc::from("tenant-a"),
        kind: Some(Arc::from("content")),
        message_type: None,
        channel: Some(Arc::from("news")),
        actor_id: None,
        audience_type: Some(Arc::from("team")),
        audience_id: Some(Arc::from("team-7")),
        content_type: Arc::from("application/octet-stream"),
        payload: Bytes::from(vec![index]),
    }
}

#[tokio::test]
async fn equal_entity_keys_preserve_partition_order_through_core() {
    let Some(harness) = KafkaHarness::new("ordering", 3).await else {
        return;
    };
    let (router, mut deliveries) = router_with_subscription();
    let (shutdown, ingestor) = spawn_ingestor(
        &harness.consumer_config("ordering", true),
        Arc::clone(&router),
    );
    let publisher = KafkaPublisher::new(&KafkaProducerConfig {
        enabled: true,
        brokers: harness.brokers.clone(),
        client_id: format!("publisher-{}", Uuid::new_v4().simple()),
        topic: harness.topic.clone(),
        delivery_timeout_ms: 30_000,
        properties: BTreeMap::new(),
    })
    .expect("Kafka publisher");

    let mut partition = None;
    for index in 0..8 {
        let receipt = publisher.publish(command(index)).await.expect("publish");
        assert_eq!(
            *partition.get_or_insert(receipt.partition),
            receipt.partition,
            "equal entity keys must select one partition"
        );
    }

    for expected in 0..8 {
        let delivery = timeout(DEADLINE, deliveries.recv())
            .await
            .expect("delivery timeout")
            .expect("delivery channel");
        assert_eq!(delivery.message.payload.as_ref(), &[expected]);
        assert_eq!(
            &*delivery.message.metadata.message_id,
            format!("ordered-{expected}")
        );
    }
    harness
        .wait_for_commit(partition.expect("partition"), 8)
        .await;
    shutdown.send(true).expect("shutdown ingestor");
    ingestor.await.expect("ingestor task");
}

#[tokio::test]
async fn valid_and_invalid_records_follow_commit_policy() {
    let Some(harness) = KafkaHarness::new("commit-policy", 1).await else {
        return;
    };
    let (router, mut deliveries) = router_with_subscription();
    let (shutdown, ingestor) = spawn_ingestor(
        &harness.consumer_config("commit-true", true),
        Arc::clone(&router),
    );

    let (partition, valid_offset) = harness
        .produce_raw(
            "entity",
            b"valid",
            OwnedHeaders::new().insert(Header {
                key: "x-tenant-id",
                value: Some(b"tenant-a"),
            }),
        )
        .await;
    let delivery = timeout(DEADLINE, deliveries.recv())
        .await
        .expect("valid delivery timeout")
        .expect("valid delivery");
    assert_eq!(
        &*delivery.message.metadata.message_id,
        format!("{}:{partition}:{valid_offset}", harness.topic)
    );
    assert_eq!(
        &*delivery.message.metadata.content_type,
        "application/octet-stream"
    );
    harness.wait_for_commit(partition, valid_offset + 1).await;

    let (_, invalid_offset) = harness
        .produce_raw("entity", b"invalid", OwnedHeaders::new())
        .await;
    harness.wait_for_commit(partition, invalid_offset + 1).await;
    harness
        .poll_until("invalid-message metric", || {
            router.status().metrics.invalid_messages == 1
        })
        .await;
    shutdown.send(true).expect("shutdown ingestor");
    ingestor.await.expect("ingestor task");

    let Some(blocking) = KafkaHarness::new("uncommitted-poison", 1).await else {
        return;
    };
    let (_, poison_offset) = blocking
        .produce_raw("entity", b"poison", OwnedHeaders::new())
        .await;
    blocking
        .produce_raw(
            "entity",
            b"must-not-be-skipped",
            valid_headers("after-poison"),
        )
        .await;
    let (router, _) = router_with_subscription();
    let (_shutdown, ingestor) = spawn_ingestor(
        &blocking.consumer_config("commit-false", false),
        Arc::clone(&router),
    );
    timeout(DEADLINE, ingestor)
        .await
        .expect("consumer should stop on uncommitted poison")
        .expect("ingestor task");
    assert_eq!(router.status().metrics.invalid_messages, 1);
    assert_eq!(router.status().metrics.valid_messages, 0);
    assert!(
        blocking
            .committed_offset(0)
            .is_none_or(|offset| offset <= poison_offset),
        "poison offset must not be advanced"
    );
}

#[tokio::test]
async fn restart_before_commit_redelivers_the_same_message_id() {
    let Some(harness) = KafkaHarness::new("restart-duplicate", 1).await else {
        return;
    };
    let (partition, offset) = harness
        .produce_raw("entity", b"duplicate", valid_headers("stable-duplicate-id"))
        .await;

    let (router, mut deliveries) = router_with_subscription();
    let consumer = harness.probe_consumer("before-commit");
    consumer.subscribe(&[&harness.topic]).expect("subscribe");
    let deadline = Instant::now() + DEADLINE;
    let record = loop {
        if let Some(result) = consumer.poll(Duration::from_millis(250)) {
            break result.expect("consume before commit").detach();
        }
        assert!(
            Instant::now() < deadline,
            "timed out consuming before commit"
        );
    };
    let decoded = decode_message(&record, 1024).expect("decode");
    router.dispatch(Arc::new(decoded));
    let first = deliveries.recv().await.expect("first delivery");
    assert_eq!(&*first.message.metadata.message_id, "stable-duplicate-id");
    drop(consumer);
    assert!(harness.committed_offset(partition).is_none());

    let (shutdown, ingestor) = spawn_ingestor(
        &harness.consumer_config("after-restart", true),
        Arc::clone(&router),
    );
    let duplicate = timeout(DEADLINE, deliveries.recv())
        .await
        .expect("duplicate timeout")
        .expect("duplicate delivery");
    assert_eq!(
        duplicate.message.metadata.message_id,
        first.message.metadata.message_id
    );
    assert_eq!(
        duplicate
            .message
            .metadata
            .source
            .as_ref()
            .expect("source")
            .offset,
        offset
    );
    harness.wait_for_commit(partition, offset + 1).await;
    shutdown.send(true).expect("shutdown ingestor");
    ingestor.await.expect("ingestor task");
}

#[tokio::test]
async fn joining_consumer_forces_rebalance_callbacks() {
    let Some(harness) = KafkaHarness::new("rebalance", 2).await else {
        return;
    };
    let first_router = Arc::new(Router::new(RouterConfig::default()));
    let (first_shutdown, first_task) = spawn_ingestor(
        &harness.consumer_config("rebalance-first", true),
        Arc::clone(&first_router),
    );
    harness
        .poll_until("first assignment", || {
            first_router.status().metrics.kafka_rebalance_assignments > 0
        })
        .await;

    let second_router = Arc::new(Router::new(RouterConfig::default()));
    let (second_shutdown, second_task) = spawn_ingestor(
        &harness.consumer_config("rebalance-second", true),
        Arc::clone(&second_router),
    );
    harness
        .poll_until("rebalance revocation and reassignment", || {
            first_router.status().metrics.kafka_rebalance_revocations > 0
                && second_router.status().metrics.kafka_rebalance_assignments > 0
        })
        .await;

    first_shutdown.send(true).expect("shutdown first");
    second_shutdown.send(true).expect("shutdown second");
    first_task.await.expect("first ingestor");
    second_task.await.expect("second ingestor");
}
