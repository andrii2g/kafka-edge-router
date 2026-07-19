//! Generated-client gRPC contract, flow-control, and lifecycle tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::Bytes;
use router_api::{serve_grpc, ApiConfig, ApiState, AuthConfig, AuthMode, HealthState};
use router_core::{RoutedMessage, Router, RouterConfig, RoutingMetadata};
use router_proto::v1::{
    client_command, kafka_router_client::KafkaRouterClient, server_event, ClientCommand,
    GetStatusRequest, Ping, PublishRequest, RouteFilter, SubscribeCommand, SubscribeRequest,
    UnsubscribeCommand,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Code, Request};

struct TestServer {
    endpoint: String,
    router: Arc<Router>,
    health: Arc<HealthState>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(api: ApiConfig, auth: AuthConfig) -> Self {
        let router = Arc::new(Router::new(RouterConfig {
            default_queue_capacity: 2,
            max_queue_capacity: 8,
            max_subscriptions_per_connection: 4,
            slow_consumer_strikes: 1,
            ..RouterConfig::default()
        }));
        let health = Arc::new(HealthState::default());
        let state = ApiState::new(Arc::clone(&router), auth, None, Arc::clone(&health), api);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind gRPC");
        let endpoint = format!("http://{}", listener.local_addr().expect("local address"));
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            serve_grpc(listener, state, receiver)
                .await
                .expect("gRPC server");
        });
        Self {
            endpoint,
            router,
            health,
            shutdown,
            task,
        }
    }

    async fn client(&self) -> KafkaRouterClient<Channel> {
        KafkaRouterClient::new(self.channel().await)
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(self.endpoint.clone())
            .expect("endpoint")
            .connect()
            .await
            .expect("gRPC channel")
    }

    async fn stop(self) {
        self.shutdown.send(true).expect("shutdown receiver");
        self.task.abort();
        let result = self.task.await;
        assert!(result.is_ok() || result.is_err_and(|error| error.is_cancelled()));
    }
}

fn api_config() -> ApiConfig {
    ApiConfig {
        stream_queue_capacity: 2,
        max_stream_queue_capacity: 8,
        grpc_max_decoding_message_bytes: 1_024,
        grpc_max_encoding_message_bytes: 1_024,
        grpc_concurrency_limit: 8,
        grpc_keep_alive_interval_secs: 5,
        grpc_keep_alive_timeout_secs: 2,
        ..ApiConfig::default()
    }
}

fn disabled_auth() -> AuthConfig {
    AuthConfig {
        default_tenant: Some("tenant-a".to_owned()),
        publish_tenants: BTreeSet::from(["tenant-a".to_owned()]),
        ..AuthConfig::default()
    }
}

fn filter(tenant: &str) -> RouteFilter {
    RouteFilter {
        tenant_id: tenant.to_owned(),
        kind: Some("content".to_owned()),
        r#type: Some("broadcast".to_owned()),
        channel: Some("news".to_owned()),
        actor_id: None,
        recipient_type: None,
        recipient_identity: None,
    }
}

fn routed_message(id: &str) -> Arc<RoutedMessage> {
    Arc::new(
        RoutedMessage::new(
            RoutingMetadata {
                message_id: Arc::from(id),
                tenant_id: Arc::from("tenant-a"),
                kind: Some(Arc::from("content")),
                message_type: Some(Arc::from("broadcast")),
                channel: Some(Arc::from("news")),
                actor_id: None,
                recipient_type: None,
                recipient_identity: None,
                content_type: Arc::from("application/json"),
                timestamp_ms: None,
                source: None,
            },
            Bytes::from_static(br#"{"ok":true}"#),
        )
        .expect("routed message"),
    )
}

async fn wait_for_router_counts(router: &Router, connections: usize, subscriptions: usize) {
    for _ in 0..100 {
        let status = router.status();
        if status.active_connections == connections && status.subscriptions == subscriptions {
            return;
        }
        tokio::task::yield_now().await;
    }
    let status = router.status();
    assert_eq!(status.active_connections, connections);
    assert_eq!(status.subscriptions, subscriptions);
}

#[tokio::test]
async fn generated_client_server_stream_delivers_and_cancels_cleanly() {
    let server = TestServer::start(api_config(), disabled_auth()).await;
    let mut client = server.client().await;
    let response = client
        .subscribe(SubscribeRequest {
            subscription_id: "fixed".to_owned(),
            filter: Some(filter("tenant-a")),
            queue_capacity: Some(2),
        })
        .await
        .expect("subscribe");
    let mut stream = response.into_inner();
    assert_eq!(server.router.status().active_connections, 1);
    assert_eq!(server.router.status().subscriptions, 1);

    let report = server.router.dispatch(routed_message("message-1"));
    assert_eq!(report.delivered_connections, 1);
    let event = stream
        .message()
        .await
        .expect("stream status")
        .expect("message event");
    let server_event::Event::Message(message) = event.event.expect("event oneof") else {
        panic!("expected message event");
    };
    assert_eq!(message.subscription_ids, ["fixed"]);
    assert_eq!(
        message
            .message
            .expect("routed message")
            .metadata
            .expect("metadata")
            .message_id,
        "message-1"
    );

    drop(stream);
    wait_for_router_counts(&server.router, 0, 0).await;
    server.stop().await;
}

#[tokio::test]
async fn generated_bidi_client_subscribes_pings_delivers_and_unsubscribes() {
    let server = TestServer::start(api_config(), disabled_auth()).await;
    let mut client = server.client().await;
    let (commands, input) = mpsc::channel(4);
    let mut events = client
        .connect(ReceiverStream::new(input))
        .await
        .expect("connect")
        .into_inner();

    commands
        .send(ClientCommand {
            command: Some(client_command::Command::Ping(Ping {
                opaque: "probe".to_owned(),
            })),
        })
        .await
        .expect("send ping");
    let event = events.message().await.expect("ping status").expect("pong");
    let server_event::Event::Pong(pong) = event.event.expect("event") else {
        panic!("expected pong");
    };
    assert_eq!(pong.opaque, "probe");

    commands
        .send(ClientCommand {
            command: Some(client_command::Command::Subscribe(SubscribeCommand {
                subscription_id: "dynamic".to_owned(),
                filter: Some(filter("tenant-a")),
            })),
        })
        .await
        .expect("send subscribe");
    let event = events.message().await.expect("ack status").expect("ack");
    let server_event::Event::Ack(ack) = event.event.expect("event") else {
        panic!("expected subscribe ack");
    };
    assert_eq!(ack.operation, "subscribed");
    assert_eq!(server.router.status().subscriptions, 1);

    assert_eq!(
        server
            .router
            .dispatch(routed_message("message-2"))
            .delivered_connections,
        1
    );
    let event = events
        .message()
        .await
        .expect("delivery status")
        .expect("delivery");
    assert!(matches!(event.event, Some(server_event::Event::Message(_))));

    commands
        .send(ClientCommand {
            command: Some(client_command::Command::Unsubscribe(UnsubscribeCommand {
                subscription_id: "dynamic".to_owned(),
            })),
        })
        .await
        .expect("send unsubscribe");
    let event = events
        .message()
        .await
        .expect("unsubscribe status")
        .expect("unsubscribe ack");
    let server_event::Event::Ack(ack) = event.event.expect("event") else {
        panic!("expected unsubscribe ack");
    };
    assert_eq!(ack.operation, "unsubscribed");
    assert_eq!(server.router.status().subscriptions, 0);

    drop(events);
    drop(commands);
    wait_for_router_counts(&server.router, 0, 0).await;
    server.stop().await;
}

#[tokio::test]
async fn invalid_stream_commands_return_stable_invalid_argument_statuses() {
    let server = TestServer::start(api_config(), disabled_auth()).await;

    for command in [
        ClientCommand { command: None },
        ClientCommand {
            command: Some(client_command::Command::Subscribe(SubscribeCommand {
                subscription_id: "missing-filter".to_owned(),
                filter: None,
            })),
        },
    ] {
        let mut client = server.client().await;
        let (commands, input) = mpsc::channel(1);
        let mut events = client
            .connect(ReceiverStream::new(input))
            .await
            .expect("connect")
            .into_inner();
        commands.send(command).await.expect("send invalid command");
        let status = events
            .message()
            .await
            .expect_err("invalid command must terminate stream");
        assert_eq!(status.code(), Code::InvalidArgument);
        drop(commands);
    }

    wait_for_router_counts(&server.router, 0, 0).await;
    server.stop().await;
}

#[tokio::test]
async fn duplicate_subscription_returns_invalid_argument_without_leaking() {
    let server = TestServer::start(api_config(), disabled_auth()).await;
    let mut client = server.client().await;
    let (commands, input) = mpsc::channel(2);
    let mut events = client
        .connect(ReceiverStream::new(input))
        .await
        .expect("connect")
        .into_inner();
    let subscribe = || ClientCommand {
        command: Some(client_command::Command::Subscribe(SubscribeCommand {
            subscription_id: "duplicate".to_owned(),
            filter: Some(filter("tenant-a")),
        })),
    };

    commands.send(subscribe()).await.expect("first subscribe");
    events
        .message()
        .await
        .expect("first status")
        .expect("first ack");
    commands
        .send(subscribe())
        .await
        .expect("duplicate subscribe");
    let status = events
        .message()
        .await
        .expect_err("duplicate must terminate stream");
    assert_eq!(status.code(), Code::InvalidArgument);

    drop(commands);
    wait_for_router_counts(&server.router, 0, 0).await;
    server.stop().await;
}

#[tokio::test]
async fn fixed_subscribe_rejects_missing_filter_and_queue_outside_cap() {
    let server = TestServer::start(api_config(), disabled_auth()).await;

    for request in [
        SubscribeRequest {
            subscription_id: "missing".to_owned(),
            filter: None,
            queue_capacity: None,
        },
        SubscribeRequest {
            subscription_id: "zero".to_owned(),
            filter: Some(filter("tenant-a")),
            queue_capacity: Some(0),
        },
        SubscribeRequest {
            subscription_id: "over".to_owned(),
            filter: Some(filter("tenant-a")),
            queue_capacity: Some(9),
        },
    ] {
        let status = server
            .client()
            .await
            .subscribe(request)
            .await
            .expect_err("invalid subscription");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    assert_eq!(server.router.status().active_connections, 0);
    server.stop().await;
}

#[tokio::test]
async fn tenant_mismatch_is_permission_denied_and_auth_is_shared() {
    let mut bearer_tokens = BTreeMap::new();
    bearer_tokens.insert("token-a".to_owned(), "tenant-a".to_owned());
    let auth = AuthConfig {
        mode: AuthMode::StaticBearer,
        bearer_tokens,
        ..AuthConfig::default()
    };
    let server = TestServer::start(api_config(), auth).await;
    let mut client = server.client().await;

    let unauthenticated = client
        .get_status(GetStatusRequest {})
        .await
        .expect_err("status requires authentication");
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let mut request = Request::new(SubscribeRequest {
        subscription_id: "cross-tenant".to_owned(),
        filter: Some(filter("tenant-b")),
        queue_capacity: None,
    });
    request.metadata_mut().insert(
        "authorization",
        "Bearer token-a".parse().expect("metadata value"),
    );
    let status = client
        .subscribe(request)
        .await
        .expect_err("cross-tenant filter");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(server.router.status().active_connections, 0);
    server.stop().await;
}

#[tokio::test]
async fn unavailable_publisher_and_oversized_input_have_stable_statuses() {
    let mut config = api_config();
    config.grpc_max_decoding_message_bytes = 128;
    let server = TestServer::start(config, disabled_auth()).await;
    let mut client = server.client().await;

    let unavailable = client
        .publish(PublishRequest {
            tenant_id: "tenant-a".to_owned(),
            payload: Vec::new(),
            ..PublishRequest::default()
        })
        .await
        .expect_err("publisher is unavailable");
    assert_eq!(unavailable.code(), Code::FailedPrecondition);

    let oversized = client
        .publish(PublishRequest {
            tenant_id: "tenant-a".to_owned(),
            payload: vec![0; 256],
            ..PublishRequest::default()
        })
        .await
        .expect_err("message exceeds decoding cap");
    assert_eq!(oversized.code(), Code::OutOfRange);
    server.stop().await;
}

#[tokio::test]
async fn standard_health_tracks_readiness_and_reflection_is_optional() {
    use tonic_health::pb::{
        health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
    };
    use tonic_reflection::pb::v1::{
        server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
        ServerReflectionRequest,
    };

    let mut config = api_config();
    config.grpc_reflection_enabled = true;
    let server = TestServer::start(config, disabled_auth()).await;
    let channel = server.channel().await;

    let mut health = HealthClient::new(channel.clone());
    let mut statuses = health
        .watch(HealthCheckRequest {
            service: "router.v1.KafkaRouter".to_owned(),
        })
        .await
        .expect("health watch")
        .into_inner();
    assert_eq!(
        statuses
            .message()
            .await
            .expect("health status")
            .expect("initial health")
            .status,
        ServingStatus::NotServing as i32
    );
    server.health.set_ready(true);
    assert_eq!(
        statuses
            .message()
            .await
            .expect("health status")
            .expect("ready health")
            .status,
        ServingStatus::Serving as i32
    );

    let mut reflection = ServerReflectionClient::new(channel);
    let request = tokio_stream::iter([ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }]);
    let response = reflection
        .server_reflection_info(request)
        .await
        .expect("reflection enabled")
        .into_inner()
        .message()
        .await
        .expect("reflection status")
        .expect("reflection response");
    let Some(MessageResponse::ListServicesResponse(services)) = response.message_response else {
        panic!("expected reflected service list");
    };
    assert!(services
        .service
        .iter()
        .any(|service| service.name == "router.v1.KafkaRouter"));

    server.stop().await;

    let mut config = api_config();
    config.grpc_health_enabled = false;
    config.grpc_reflection_enabled = false;
    let server = TestServer::start(config, disabled_auth()).await;
    let channel = server.channel().await;
    let health_status = HealthClient::new(channel.clone())
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect_err("health disabled");
    assert_eq!(health_status.code(), Code::Unimplemented);
    let reflection_status = ServerReflectionClient::new(channel)
        .server_reflection_info(tokio_stream::iter([ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        }]))
        .await
        .expect_err("reflection disabled");
    assert_eq!(reflection_status.code(), Code::Unimplemented);

    server.stop().await;
}
