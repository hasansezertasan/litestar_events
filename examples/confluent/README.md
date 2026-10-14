# confluent-kafka event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by `confluent-kafka`
(librdkafka — the C-extension Kafka client). Same protocol and semantics as
the [`kafka`](../kafka) backend, faster but heavier to install.

## Run

```bash
# 1. Kafka (single-node KRaft mode)
docker run --rm -p 9092:9092 apache/kafka:3.7.0

# 2. App
uv run litestar --app examples.confluent.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

## Inspect

```bash
# List topics the broker knows about
docker exec -it <container> /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Tail a specific event topic
docker exec -it <container> /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic litestar.events.user_registered \
    --from-beginning
```

## Delivery semantics

- **At-least-once** with consumer-group offsets (auto-commit on by default).
- **Broadcast vs work-queue** is controlled by `group_id`:
  - Default: random UUID per emitter → every replica receives every event.
  - Shared `group_id="my-app"` across replicas → exactly one receives each
    event (work-queue).
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.

## Confluent vs Kafka

This backend wraps `confluent-kafka` (librdkafka). It's the same Kafka
protocol as the [`kafka`](../kafka) backend (which uses `aiokafka`), so they
interoperate. Pick `confluent` for production throughput, `kafka` for
easier installs and pure-Python tooling.

## Configuration

```python
ConfluentEventEmitter(
    listeners,
    bootstrap_servers="localhost:9092",
    group_id="my-app",                 # set for work-queue, omit for broadcast
    topic_prefix="litestar.events.",
    producer_config={"compression.type": "lz4"},     # passed to librdkafka
    consumer_config={"session.timeout.ms": "10000"},
)
```
