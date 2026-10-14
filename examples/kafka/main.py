"""Example Litestar app using the aiokafka event emitter backend.

Run a local Kafka broker first, e.g.:

    docker run --rm -p 9092:9092 apache/kafka:3.7.0

Then start the app:

    uv run litestar --app examples.kafka.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \\
        -H 'content-type: application/json' \\
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event travels through Kafka. By default every app
instance gets a unique consumer group (broadcast/fanout). Set ``group_id``
to a shared value for work-queue semantics.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.kafka import KafkaEventEmitter

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


app = Litestar(
    route_handlers=[register_user],
    listeners=[send_welcome_email, record_analytics],
    event_emitter_backend=partial(
        KafkaEventEmitter,
        bootstrap_servers="localhost:9092",
        # group_id="my-app",      # set for work-queue semantics across N replicas
        # topic_prefix="myapp.events.",
    ),
)
