"""Example Litestar app using the aio-pika event emitter backend.

Run a local RabbitMQ first, e.g.:

    docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management

Then start the app:

    uv run litestar --app examples.rabbit.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \
        -H 'content-type: application/json' \
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event will travel through RabbitMQ and be picked up by
both listeners below. Stop and restart the app: events emitted while it was
down survive only if you set ``queue_name`` (durable, work-queue mode).
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.rabbit import RabbitEventEmitter

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
        RabbitEventEmitter,
        amqp_url="amqp://guest:guest@localhost/",
        # queue_name="my-app",   # uncomment for durable work-queue semantics
    ),
)
