"""Example Litestar app using the NATS event emitter backend.

Run a local NATS server first, e.g.:

    docker run --rm -p 4222:4222 nats:2

Then start the app:

    uv run litestar --app examples.nats.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \\
        -H 'content-type: application/json' \\
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event will travel through NATS and be picked up by
both listeners below. Note: core NATS pub/sub is fire-and-forget. Events
emitted while no subscriber is connected are lost. For durability, use
JetStream (not provided by this backend) or the rabbit backend.

Inspect live subjects with the NATS CLI:

    nats sub 'litestar.events.>'
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.nats import NATSEventEmitter

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
        NATSEventEmitter,
        servers="nats://localhost:4222",
        # subject_prefix="myapp.events.",  # customize per-app namespace
    ),
)
