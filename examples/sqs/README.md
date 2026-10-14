# AWS SQS event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by AWS SQS
(via [`aiobotocore`](https://github.com/aio-libs/aiobotocore)).

## Run

```bash
# 1. LocalStack (stand-in for AWS during development)
docker run --rm -p 4566:4566 localstack/localstack:3

# 2. App (the backend auto-creates the queue on startup)
uv run litestar --app examples.sqs.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Inspect

```bash
# List queues (LocalStack)
aws --endpoint-url http://localhost:4566 sqs list-queues

# Messages in flight / waiting
aws --endpoint-url http://localhost:4566 sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/litestar_events \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

## Delivery semantics

- **Durable, at-least-once.** A message is deleted only after all matching
  listeners run, so a crash mid-dispatch leaves it to reappear after the
  visibility timeout.
- **Work-queue across instances.** SQS is a point-to-point queue. Multiple app
  instances sharing one queue are *competing consumers* — each event is handled
  by exactly one instance.
- **Per-listener error isolation.** One listener raising does not cancel
  siblings; exceptions are logged.
- **Poison messages.** Unparseable bodies are dropped with a logged error.
  Attach a redrive policy (DLQ) to the queue to retain them instead.

## When not to use this backend

- You need every replica to see every event (broadcast fanout) → front the
  queue with an SNS topic (SNS → per-instance SQS subscription). That is out of
  scope for this backend.
- You want a self-hosted broker rather than a managed AWS service → use
  [`rabbit`](../rabbit) or [`kafka`](../kafka).

## Configuration

```python
SQSEventEmitter(
    listeners,
    queue_name="litestar_events",   # or pass an explicit queue_url=
    region_name="eu-central-1",
    create_queue=True,              # auto-create when resolving by name
    wait_time_seconds=20,           # long-poll window
    max_messages=10,                # ReceiveMessage batch size
    visibility_timeout=30,
    # endpoint_url / aws_access_key_id / aws_secret_access_key:
    # mainly for LocalStack and tests; omit for real AWS (boto credential chain).
)
```
