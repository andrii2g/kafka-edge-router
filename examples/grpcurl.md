# gRPC examples

The service is plaintext in local development. Use TLS or an authenticated ingress in production.

```bash
# Status
grpcurl -plaintext -import-path crates/router-proto/proto \
  -proto router/v1/router.proto \
  127.0.0.1:9090 router.v1.KafkaRouter/GetStatus

# Fixed server stream
grpcurl -plaintext -import-path crates/router-proto/proto \
  -proto router/v1/router.proto \
  -d '{"subscriptionId":"grpc-news","filter":{"tenantId":"tenant-demo","kind":"content","channel":"news"}}' \
  127.0.0.1:9090 router.v1.KafkaRouter/Subscribe

# Publish
grpcurl -plaintext -import-path crates/router-proto/proto \
  -proto router/v1/router.proto \
  -d '{"tenantId":"tenant-demo","kind":"content","channel":"news","contentType":"application/json","payload":"eyJncnBjIjp0cnVlfQ=="}' \
  127.0.0.1:9090 router.v1.KafkaRouter/Publish
```
