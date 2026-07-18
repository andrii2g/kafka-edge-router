# Durable webhook operations

## Topic provisioning

Durable mode requires three topics with the same partition count. The count cannot change
independently because `destination_id` must map to the same partition number in delivery
and retry topics.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic router.webhook.delivery --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000

kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic router.webhook.retry --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=604800000

kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic router.webhook.dead-letter --partitions 6 --replication-factor 3 \
  --config cleanup.policy=delete --config retention.ms=2592000000
```

Set delivery and retry retention longer than the maximum operational outage plus retry
horizon. Restrict DLQ read/write permissions because records contain original payloads.
Do not enable compaction: multiple attempts and duplicate windows are audit evidence.

All router replicas for one deployment use the same `webhooks.durable.group_id` and
identical destination ids/configuration. Durable consumer invariants force manual commit,
earliest recovery, partition EOF events, and the range assignor.

## Monitoring

Alert on sustained growth or non-zero rates for:

```text
router_webhook_retries_scheduled_total
router_webhook_dead_letters_total
router_webhook_failures_total
```

Use these for throughput and restart diagnosis:

```text
router_webhook_durable_commands_total
router_webhook_recovery_replays_total
router_webhook_attempts_total
router_webhook_successes_total
```

A worker exits when retry recovery exceeds `max_recovery_records`, a durable record
exceeds `max_record_bytes`, Kafka publication fails, or an offset commit fails. The
daemon then becomes unready and shuts down. Correct the broker/topic/configuration issue
before restart; do not raise bounds without checking memory and recovery-time impact.

## DLQ replay

1. Stop or correct the failing receiver/destination first.
2. Inspect record metadata without logging or exporting `body_base64`.
3. Verify `destination_id` still exists and the receiver durably deduplicates
   `original_message_id`.
4. Republish the complete unchanged record to `router.webhook.delivery` with Kafka key
   equal to `destination_id`; set `state` to `delivery`, `attempt` to 1,
   `next_attempt_at_ms` to the current Unix time, and clear `last_error_class`.
5. Wait for a broker acknowledgement before recording the DLQ source offset as replayed.
6. Monitor success, retry, and DLQ counters. Replaying can issue duplicate HTTP requests.

Never put destination URLs, signing secrets, configured authorization headers, or Kafka
credentials into replay records. Never delete the DLQ source until the replay audit and
receiver outcome are confirmed.

