# GCP Pub/Sub event emitter example

Drop-in `BaseEventEmitterBackend` for Litestar backed by Google Cloud Pub/Sub
(via [`gcloud-aio-pubsub`](https://github.com/talkiq/gcloud-aio)).

## Run

```bash
# 1. Pub/Sub emulator (stand-in for GCP during development)
docker run --rm -p 8681:8681 messagebird/gcloud-pubsub-emulator:latest

# 2. App (creates the topic + a per-instance subscription on startup)
uv run litestar --app examples.pubsub.main:app run

# 3. Trigger an event
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners (`send_welcome_email`, `record_analytics`) react concurrently.

## Delivery semantics

- **Durable, at-least-once.** A message is acked only after its listeners run.
- **Broadcast fanout by default.** Each instance creates its own subscription,
  so every instance sees every event — Pub/Sub's native topic→subscription
  model. Pass a shared `subscription_name=` for work-queue / competing-consumer
  semantics instead.
- **Per-listener error isolation.** One listener raising does not cancel
  siblings, and never causes infinite redelivery.
- **Poison messages.** Unparseable bodies are acked and dropped with a logged
  error.

## When not to use this backend

- You are not on GCP → use [`sqs`](../sqs) (AWS) or a self-hosted broker like
  [`rabbit`](../rabbit) / [`kafka`](../kafka).

## Configuration

```python
PubSubEventEmitter(
    listeners,
    project_id="my-gcp-project",
    topic_id="litestar_events",
    subscription_name=None,   # None -> unique per-instance sub (broadcast)
    create_resources=True,    # auto-create topic + subscription
    emulator_host=None,       # "host:port" for the local emulator / tests
)
```
