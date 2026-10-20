# litestar_events

[![CI](https://github.com/hasansezertasan/litestar_events/actions/workflows/ci.yml/badge.svg)](https://github.com/hasansezertasan/litestar_events/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/hasansezertasan/litestar_events)](https://codecov.io/gh/hasansezertasan/litestar_events)
[![Documentation Status](https://img.shields.io/github/deployments/hasansezertasan/litestar_events/github-pages?label=docs)](https://hasansezertasan.github.io/litestar_events)
[![PyPI - Version](https://img.shields.io/pypi/v/litestar_events.svg)](https://pypi.org/project/litestar_events)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/litestar_events.svg)](https://pypi.org/project/litestar_events)
[![License - MIT](https://img.shields.io/github/license/hasansezertasan/litestar_events.svg)](https://opensource.org/licenses/MIT)
[![GitHub Stars](https://img.shields.io/github/stars/hasansezertasan/litestar_events?style=social)](https://github.com/hasansezertasan/litestar_events/stargazers)
[![Latest Commit](https://img.shields.io/github/last-commit/hasansezertasan/litestar_events)](https://github.com/hasansezertasan/litestar_events)

[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/hasansezertasan/litestar_events/badge)](https://scorecard.dev/viewer/?uri=github.com/hasansezertasan/litestar_events)
[![GitHub Tag](https://img.shields.io/github/tag/hasansezertasan/litestar_events?include_prereleases=&sort=semver&color=black)](https://github.com/hasansezertasan/litestar_events/releases/)

[![Downloads](https://pepy.tech/badge/litestar_events)](https://pepy.tech/project/litestar_events)
[![Downloads/Month](https://pepy.tech/badge/litestar_events/month)](https://pepy.tech/project/litestar_events)
[![Downloads/Week](https://pepy.tech/badge/litestar_events/week)](https://pepy.tech/project/litestar_events)

> Event Emitter Backends for Litestar.

-----

## Table of Contents

- [Installation](#installation)
- [Backends](#backends)
- [Quick start](#quick-start)
- [Examples](#examples)
- [Delivery semantics](#delivery-semantics)
- [Support](#support-heart)
- [Motivation](#motivation)
- [About](#about)
- [Author](#author-person_with_crown)
- [Analysis](#analysis)
- [Contributing](#contributing-heart)
- [Development](#development-toolbox)
- [Releasing](#releasing)
- [Credits](#credits)
- [License](#license-scroll)
- [Changelog](#changelog-memo)

## Installation

`litestar_events` is a library. Add it to your project as a dependency:

```console
uv add litestar_events
```

Or with `pip`:

```console
pip install litestar_events
```

Install the core package plus the extra for the backend(s) you want:

```bash
# RabbitMQ backend
pip install "litestar_events[rabbit]"

# Multiple backends at once
pip install "litestar_events[rabbit,redis,nats]"
```

Using [`uv`](https://docs.astral.sh/uv/):

```bash
uv add "litestar_events[rabbit]"
```

### Verify the installation

The package root is importable after installation:

```pycon
>>> from litestar_events import __doc__
>>> isinstance(__doc__, str)
True

```

<!--
pytest runs the pycon examples in this file (--doctest-glob=README.md).
doctest cannot see Markdown and ends an example's expected output at the first
blank line, so leave a blank line before every closing fence -- as the example
above does. Without it the closing delimiter is read as part of the expected
output and the example fails ("Expected: 2 / Got: 2"). Examples with no output
line at all need the blank line too.
-->

## Backends

| Backend     | Extra       | Client library                                                                                  |
| ----------- | ----------- | ----------------------------------------------------------------------------------------------- |
| RabbitMQ    | `rabbit`    | [`aio-pika`](https://aio-pika.readthedocs.io)                                                   |
| Kafka       | `kafka`     | [`aiokafka`](https://aiokafka.readthedocs.io)                                                   |
| Kafka (C)   | `confluent` | [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)                     |
| NATS        | `nats`      | [`nats-py`](https://github.com/nats-io/nats.py)                                                 |
| Redis       | `redis`     | [`redis`](https://github.com/redis/redis-py) (async)                                            |
| MQTT        | `mqtt`      | [`aiomqtt`](https://github.com/empicano/aiomqtt)                                                |
| PostgreSQL  | `postgres`  | [`psycopg`](https://www.psycopg.org/psycopg3/) (`LISTEN/NOTIFY`)                                |
| AWS SQS     | `sqs`       | [`aiobotocore`](https://github.com/aio-libs/aiobotocore)                                        |
| GCP Pub/Sub | `pubsub`    | [`gcloud-aio-pubsub`](https://github.com/talkiq/gcloud-aio)                                     |
| ZeroMQ      | `zmq`       | [`pyzmq`](https://pyzmq.readthedocs.io) (brokerless)                                            |

## Quick start

The same Litestar app, three different transports. The app logic — your
route handler, your listeners, your emitted events — is identical. Only
the `event_emitter_backend=` line changes.

### 1. Define your event and listeners

```python
# app.py
from __future__ import annotations

from typing import Any

from litestar import Request, post
from litestar.events import listener

USER_REGISTERED = "user_registered"


@listener(USER_REGISTERED)
async def send_welcome_email(*, email: str, **_: Any) -> None:
    print(f"[email] welcome to {email}")


@listener(USER_REGISTERED)
async def record_analytics(*, email: str, **_: Any) -> None:
    print(f"[analytics] new signup: {email}")


@post("/users")
async def register_user(request: Request, data: dict[str, str]) -> dict[str, str]:
    email = data["email"]
    request.app.emit(USER_REGISTERED, email=email)
    return {"status": "queued", "email": email}
```

Two listeners react concurrently to one event. Notice the listeners use
`**_: Any` — that's the [Litestar convention][events-docs] for keeping
listener signatures forwards-compatible when multiple listeners share an
event payload.

[events-docs]: https://docs.litestar.dev/2/usage/events.html

### 2. Pick a backend

Every backend constructor accepts `listeners: Sequence[EventListener]` plus
its own keyword arguments. Use `functools.partial` to bake in the kwargs
and hand the result to Litestar.

#### RabbitMQ (durable, at-least-once)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.rabbit import RabbitEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        RabbitEventEmitter,
        amqp_url="amqp://guest:guest@localhost/",
        # queue_name="my-app",   # set for work-queue semantics across replicas
    ),
)
```

#### Redis (fire-and-forget pub/sub)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.redis import RedisEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        RedisEventEmitter, redis_url="redis://localhost:6379/0"
    ),
)
```

#### PostgreSQL (`LISTEN`/`NOTIFY`)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.postgres import PostgresEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        PostgresEventEmitter, dsn="postgresql://postgres:postgres@localhost:5432/events"
    ),
)
```

#### AWS SQS (durable, at-least-once, managed)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.sqs import SQSEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        SQSEventEmitter,
        queue_name="litestar_events",
        region_name="eu-central-1",
        # credentials come from the standard boto chain (env, instance role, ...)
    ),
)
```

#### GCP Pub/Sub (durable, broadcast fanout)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.pubsub import PubSubEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        PubSubEventEmitter,
        project_id="my-gcp-project",
        topic_id="litestar_events",
        # subscription_name="my-app",  # set for work-queue across replicas
    ),
)
```

#### ZeroMQ (brokerless, fire-and-forget)

```python
from functools import partial

from litestar import Litestar

from litestar_events.contrib.zmq import ZeroMQEventEmitter

app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        ZeroMQEventEmitter, pub_address="tcp://127.0.0.1:5557"
    ),
)
```

### 3. Trigger an event

```bash
curl -X POST http://localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"email": "ada@example.com"}'
```

Both listeners run concurrently for every emit, regardless of which backend
you picked. The choice of backend changes **how** the event gets from
emitter to listener (and what happens when things go wrong), not whether
the listener fires.

## Examples

Runnable examples for each backend live under [`examples/`](./examples):

- [`examples/rabbit`](./examples/rabbit) — RabbitMQ via `aio-pika`
- [`examples/kafka`](./examples/kafka) — Kafka via `aiokafka`
- [`examples/confluent`](./examples/confluent) — Kafka via `confluent-kafka`
- [`examples/nats`](./examples/nats) — NATS
- [`examples/redis`](./examples/redis) — Redis Pub/Sub
- [`examples/mqtt`](./examples/mqtt) — MQTT
- [`examples/postgres`](./examples/postgres) — PostgreSQL `LISTEN/NOTIFY`
- [`examples/sqs`](./examples/sqs) — AWS SQS (LocalStack-friendly)
- [`examples/pubsub`](./examples/pubsub) — GCP Pub/Sub (emulator-friendly)
- [`examples/zmq`](./examples/zmq) — ZeroMQ (brokerless PUB/SUB)

## Delivery semantics

Different backends offer different guarantees. The library tries to give each
backend the **strongest reasonable default** for its transport, while letting
you weaken or tune those guarantees via constructor arguments:

- **RabbitMQ** — at-least-once with per-listener error isolation and a
  dead-letter exchange for unparseable messages.
- **Kafka / Confluent** — at-least-once with consumer-group offsets.
- **AWS SQS** — at-least-once and durable; a message is deleted only after its
  listeners run. Instances sharing one queue are competing consumers
  (work-queue); broadcast fanout requires fronting the queue with SNS.
- **GCP Pub/Sub** — at-least-once and durable. Each instance gets its own
  subscription by default (broadcast fanout); a shared `subscription_name`
  gives work-queue semantics.
- **ZeroMQ** — fire-and-forget, at-most-once, brokerless. Events emitted before
  a subscriber finishes connecting are lost (no buffer, no persistence).
- **NATS** — at-most-once by default; JetStream-backed at-least-once when
  configured.
- **Redis / MQTT / Postgres `LISTEN/NOTIFY`** — fire-and-forget pub/sub; events
  emitted while no subscriber is connected are lost.

See each backend's docstring for the exact knobs.

## Support :heart:

If you have any questions or need help, feel free to open an issue on the [GitHub repository][litestar_events].

## Motivation

Litestar ships with an in-process event emitter (`SimpleEventEmitter`) that runs
listeners in the same process that emitted the event. That is great for
side-effects that can tolerate "best-effort, same-process" delivery, but it
falls short as soon as you need:

- **Cross-process fanout** — multiple app instances reacting to the same event.
- **Durability** — events that survive a worker crash or restart.
- **Backpressure / decoupling** — producers that should not wait on slow
  listeners.

## About

`litestar_events` provides drop-in `BaseEventEmitterBackend` implementations
backed by common messaging systems, so you can keep Litestar's `@listener`
ergonomics while swapping the transport underneath.

## Author :person_with_crown:

This project is maintained by [Hasan Sezer Taşan][author], It's me :wave:

## Analysis

- [Snyk Python Package Health Analysis](https://snyk.io/advisor/python/litestar_events)
- [Libraries.io - PyPI](https://libraries.io/pypi/litestar_events)
- [Safety DB](https://data.safetycli.com/packages/pypi/litestar_events)
- [PePy Download Stats](https://www.pepy.tech/projects/litestar_events)
- [PyPI Download Stats](https://pypistats.org/packages/litestar_events)
- [Pip Trends Download Stats](https://piptrends.com/package/litestar_events)
- [PyPI Map Dependency Graph](https://pypimap.com/package/litestar_events)

## Contributing :heart:

Any contributions are welcome! Please follow the [Contributing Guidelines](./.github/CONTRIBUTING.md) to contribute to this project.

## Development :toolbox:

See the [Contributing Guidelines](./.github/CONTRIBUTING.md#your-first-code-contribution)
for local setup, the common development tasks (exposed via [mise](https://mise.jdx.dev)),
building and previewing the documentation, and the VS Code debugging configurations.

## Releasing

Versioning and releases are automated with [release-please](https://github.com/googleapis/release-please), driven by [Conventional Commit](https://www.conventionalcommits.org/en/v1.0.0/) PR titles squash-merged into `main`. release-please maintains a release PR that bumps the version and `CHANGELOG.md`; merging it tags the release and publishes to PyPI. See the [Contributing Guidelines](./.github/CONTRIBUTING.md#releasing) for the commit conventions, and the [Repository setup](./REPOSITORY_SETUP.rst) guide for one-time configuration and optional post-launch integrations such as a social preview, downstream packaging, and Repology.

Pause merges that change `.github/workflows/`, including Renovate action-pin updates, from merging a release PR until the entire Release run finishes. Workflow differences between the release commit and current `main` can block tag or release creation with a 403 when using `GITHUB_TOKEN`. See [Release recovery](./REPOSITORY_SETUP.rst#keep-workflow-changes-out-of-an-active-release) for prevention and recovery steps.

For adopting this project's tooling into an existing package, or reconciling a
template update with existing project behavior, see the **Template updates** and
**Adopting into an existing project** sections of
[REPOSITORY_SETUP.rst](./REPOSITORY_SETUP.rst). They cover the baseline,
workflow and required-check overlap, custom tooling and documentation, and ask
for a decision before consequential cleanup. Work through the repository
settings and release steps in the same file.

## Credits

This package was created with [Copier](https://github.com/copier-org/copier) and the [hasansezertasan/copier-pyproject](https://github.com/hasansezertasan/copier-pyproject) project template.

## License :scroll:

This project is licensed under the [MIT License](https://spdx.org/licenses/MIT.html).

## Changelog :memo:

For a detailed list of changes, please refer to the [CHANGELOG](./CHANGELOG.md).

<!-- Refs -->
[author]: https://github.com/hasansezertasan
[litestar_events]: https://github.com/hasansezertasan/litestar_events
