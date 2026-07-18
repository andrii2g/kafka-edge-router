//! Tonic gRPC server-streaming and bidirectional-streaming adapter.

use std::{pin::Pin, sync::Arc};

use async_stream::try_stream;
use bytes::Bytes;
use futures_util::Stream;
use router_core::{
    ConnectionId, Delivery, DeliveryProtocol, PublishCommand, PublishErrorKind, RouteFilter,
    Router, SubscriptionId,
};
use router_proto::v1::{
    client_command,
    kafka_router_server::{KafkaRouter, KafkaRouterServer},
    server_event, Ack, ClientCommand, GetStatusRequest, KafkaPosition, MessageEvent, Pong,
    PublishRequest, PublishResponse, RouteFilter as ProtoRouteFilter,
    RoutedMessage as ProtoRoutedMessage, RoutingMetadata as ProtoRoutingMetadata, ServerEvent,
    StatusResponse, SubscribeCommand, SubscribeRequest,
};
use tokio::{net::TcpListener, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::warn;

use crate::{state::ConnectionGuard, ApiError, ApiState, Principal};

/// Serves the public gRPC API until shutdown.
pub async fn serve_grpc(
    listener: TcpListener,
    state: ApiState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(KafkaRouterServer::new(GrpcService { state }))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
}

struct GrpcService {
    state: ApiState,
}

type EventStream = Pin<Box<dyn Stream<Item = Result<ServerEvent, Status>> + Send + 'static>>;

enum ConnectInput {
    Delivery(Delivery),
    Command(ClientCommand),
}

#[tonic::async_trait]
impl KafkaRouter for GrpcService {
    type SubscribeStream = EventStream;
    type ConnectStream = EventStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let requested_tenant = request
            .get_ref()
            .filter
            .as_ref()
            .map(|filter| filter.tenant_id.as_str())
            .filter(|value| !value.is_empty());
        let principal = self
            .state
            .authenticator
            .authenticate_grpc(request.metadata(), requested_tenant)
            .map_err(ApiError::into_status)?;
        let request = request.into_inner();
        let filter = request
            .filter
            .ok_or_else(|| Status::invalid_argument("filter is required"))?;
        let filter = proto_filter(filter, &principal).map_err(ApiError::into_status)?;
        let subscription_id = SubscriptionId::new(request.subscription_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let registration = self
            .state
            .router
            .register_connection(
                &principal.tenant_id,
                DeliveryProtocol::Grpc,
                Some(grpc_queue_capacity(
                    &self.state,
                    request.queue_capacity.map(|capacity| capacity as usize),
                )?),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if let Err(error) =
            self.state
                .router
                .subscribe(registration.connection_id, subscription_id, filter)
        {
            self.state
                .router
                .unregister_connection(registration.connection_id);
            return Err(Status::invalid_argument(error.to_string()));
        }

        let router = Arc::clone(&self.state.router);
        let connection_id = registration.connection_id;
        let mut receiver = registration.receiver;
        let guard = ConnectionGuard::new(router, connection_id);
        let output = try_stream! {
            let _guard = guard;
            while let Some(delivery) = receiver.recv().await {
                yield proto_delivery(&delivery);
            }
        };
        Ok(Response::new(Box::pin(output)))
    }

    async fn connect(
        &self,
        request: Request<Streaming<ClientCommand>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let principal = self
            .state
            .authenticator
            .authenticate_grpc(request.metadata(), None)
            .map_err(ApiError::into_status)?;
        let mut incoming = request.into_inner();
        let registration = self
            .state
            .router
            .register_connection(
                &principal.tenant_id,
                DeliveryProtocol::Grpc,
                Some(self.state.config.stream_queue_capacity),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let router = Arc::clone(&self.state.router);
        let connection_id = registration.connection_id;
        let mut receiver = registration.receiver;
        let guard = ConnectionGuard::new(Arc::clone(&router), connection_id);

        let output = try_stream! {
            let _guard = guard;
            loop {
                let next: Option<Result<ConnectInput, Status>> = tokio::select! {
                    delivery = receiver.recv() => {
                        delivery.map(ConnectInput::Delivery).map(Ok)
                    }
                    command = incoming.message() => {
                        match command {
                            Ok(Some(command)) => Some(Ok(ConnectInput::Command(command))),
                            Ok(None) => None,
                            Err(error) => Some(Err(error)),
                        }
                    }
                };
                let Some(next) = next else {
                    break;
                };
                match next? {
                    ConnectInput::Delivery(delivery) => yield proto_delivery(&delivery),
                    ConnectInput::Command(command) => match command.command {
                        Some(client_command::Command::Subscribe(command)) => {
                            let ack =
                                grpc_subscribe(&router, connection_id, &principal, command)?;
                            yield ServerEvent {
                                event: Some(server_event::Event::Ack(ack)),
                            };
                        }
                        Some(client_command::Command::Unsubscribe(command)) => {
                            let subscription_id = SubscriptionId::new(command.subscription_id)
                                .map_err(|error| Status::invalid_argument(error.to_string()))?;
                            router
                                .unsubscribe(connection_id, &subscription_id)
                                .map_err(|error| Status::invalid_argument(error.to_string()))?;
                            yield ServerEvent {
                                event: Some(server_event::Event::Ack(Ack {
                                    operation: "unsubscribed".to_owned(),
                                    subscription_id: subscription_id.to_string(),
                                })),
                            };
                        }
                        Some(client_command::Command::Ping(ping)) => {
                            yield ServerEvent {
                                event: Some(server_event::Event::Pong(Pong {
                                    opaque: ping.opaque,
                                })),
                            };
                        }
                        None => Err(Status::invalid_argument("command is required"))?,
                    },
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let requested_tenant = (!request.get_ref().tenant_id.is_empty())
            .then_some(request.get_ref().tenant_id.as_str());
        let principal = self
            .state
            .authenticator
            .authenticate_grpc(request.metadata(), requested_tenant)
            .map_err(ApiError::into_status)?;
        let request = request.into_inner();
        authorize_tenant(&principal, Some(request.tenant_id.as_str()))
            .map_err(ApiError::into_status)?;
        let publisher = self
            .state
            .publisher
            .as_ref()
            .ok_or(ApiError::PublisherUnavailable.into_status())?;
        let content_type = if request.content_type.is_empty() {
            "application/octet-stream".to_owned()
        } else {
            request.content_type
        };
        let receipt = publisher
            .publish(PublishCommand {
                message_id: request.message_id.map(Arc::from),
                tenant_id: principal.tenant_id,
                kind: request.kind.map(Arc::from),
                message_type: request.r#type.map(Arc::from),
                channel: request.channel.map(Arc::from),
                actor_id: request.actor_id.map(Arc::from),
                audience_type: request.audience_type.map(Arc::from),
                audience_id: request.audience_id.map(Arc::from),
                content_type: Arc::from(content_type),
                payload: Bytes::from(request.payload),
            })
            .await
            .map_err(|error| match error.kind() {
                PublishErrorKind::InvalidInput => Status::invalid_argument(error.to_string()),
                PublishErrorKind::Backend => {
                    warn!(error = %error, "Kafka publish failed");
                    Status::internal("publish backend failed")
                }
            })?;
        Ok(Response::new(PublishResponse {
            message_id: receipt.message_id,
            topic: receipt.topic,
            partition: receipt.partition,
            offset: receipt.offset,
        }))
    }

    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let status = self.state.router.status();
        Ok(Response::new(StatusResponse {
            ready: self.state.health.is_ready(),
            active_connections: status.active_connections as u64,
            subscriptions: status.subscriptions as u64,
            kafka_messages: status.metrics.kafka_messages,
            valid_messages: status.metrics.valid_messages,
            invalid_messages: status.metrics.invalid_messages,
            delivered_connections: status.metrics.delivered_connections,
            dropped_connections: status.metrics.full_connections
                + status.metrics.closed_connections,
        }))
    }
}

fn grpc_queue_capacity(state: &ApiState, requested: Option<usize>) -> Result<usize, Status> {
    crate::state::resolve_stream_queue_capacity(&state.config, requested).map_err(|maximum| {
        Status::invalid_argument(format!("queue_capacity must be between 1 and {maximum}"))
    })
}

fn grpc_subscribe(
    router: &Router,
    connection_id: ConnectionId,
    principal: &Principal,
    command: SubscribeCommand,
) -> Result<Ack, Status> {
    let subscription_id = SubscriptionId::new(command.subscription_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let filter = command
        .filter
        .ok_or_else(|| Status::invalid_argument("filter is required"))?;
    let filter = proto_filter(filter, principal).map_err(ApiError::into_status)?;
    router
        .subscribe(connection_id, subscription_id.clone(), filter)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(Ack {
        operation: "subscribed".to_owned(),
        subscription_id: subscription_id.to_string(),
    })
}

fn proto_filter(filter: ProtoRouteFilter, principal: &Principal) -> Result<RouteFilter, ApiError> {
    authorize_tenant(principal, Some(filter.tenant_id.as_str()))?;
    Ok(RouteFilter {
        tenant_id: Arc::clone(&principal.tenant_id),
        kind: filter.kind.map(Arc::from),
        message_type: filter.r#type.map(Arc::from),
        channel: filter.channel.map(Arc::from),
        actor_id: filter.actor_id.map(Arc::from),
        audience_type: filter.audience_type.map(Arc::from),
        audience_id: filter.audience_id.map(Arc::from),
    })
}

fn authorize_tenant(principal: &Principal, requested: Option<&str>) -> Result<(), ApiError> {
    if requested
        .filter(|value| !value.is_empty())
        .is_some_and(|tenant| tenant != principal.tenant_id.as_ref())
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

fn proto_delivery(delivery: &Delivery) -> ServerEvent {
    let metadata = &delivery.message.metadata;
    let source = metadata.source.as_ref().map(|source| KafkaPosition {
        topic: source.topic.to_string(),
        partition: source.partition,
        offset: source.offset,
    });
    ServerEvent {
        event: Some(server_event::Event::Message(MessageEvent {
            subscription_ids: delivery
                .subscription_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
            message: Some(ProtoRoutedMessage {
                metadata: Some(ProtoRoutingMetadata {
                    message_id: metadata.message_id.to_string(),
                    tenant_id: metadata.tenant_id.to_string(),
                    kind: metadata.kind.as_deref().map(str::to_owned),
                    r#type: metadata.message_type.as_deref().map(str::to_owned),
                    channel: metadata.channel.as_deref().map(str::to_owned),
                    actor_id: metadata.actor_id.as_deref().map(str::to_owned),
                    audience_type: metadata.audience_type.as_deref().map(str::to_owned),
                    audience_id: metadata.audience_id.as_deref().map(str::to_owned),
                    content_type: metadata.content_type.to_string(),
                    timestamp_ms: metadata.timestamp_ms,
                    source,
                }),
                payload: delivery.message.payload.to_vec(),
            }),
        })),
    }
}
