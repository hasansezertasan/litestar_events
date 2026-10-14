# Kafka event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by `aiokafka`
(pure-Python Kafka client). Same protocol and semantics as the
[`confluent`](../confluent) backend, easier to install but slower.

## Run

```bash
# 1. Kafka (single-node KRaft mode)
docker run --rm -p 9092:9092 apache/kafka:3.7.0

# 2. App
uv run litestar --app examples.kafka.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

## Inspect

```bash
# List topics
docker exec -it <container> /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Tail a topic
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

## Kafka vs Confluent

This backend wraps `aiokafka` (pure-Python). The [`confluent`](../confluent)
backend wraps `confluent-kafka` (C-extension). Same wire protocol, same
broker, fully interoperable — they're just different client trade-offs.

## Configuration

```python
KafkaEventEmitter(
    listeners,
    bootstrap_servers="localhost:9092",
    group_id="my-app",                 # set for work-queue, omit for broadcast
    topic_prefix="litestar.events.",
)
```
