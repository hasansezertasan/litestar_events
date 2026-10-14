"""Example Litestar app using the PostgreSQL ``LISTEN/NOTIFY`` event emitter.

Run a local PostgreSQL first, e.g.:

    docker run --rm -p 5432:5432 \\
        -e POSTGRES_PASSWORD=postgres \\
        -e POSTGRES_DB=events \\
        postgres:16

Then start the app:

    uv run litestar --app examples.postgres.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \\
        -H 'content-type: application/json' \\
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event will travel through PostgreSQL ``NOTIFY`` and
be picked up by both listeners below. Note: ``LISTEN/NOTIFY`` is
fire-and-forget. Events emitted while no subscriber is connected are lost.
If you need durability, use the rabbit backend instead.

Inspect live channels with:

    psql ... -c "SELECT pg_listening_channels();"
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.postgres import PostgresEventEmitter

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
        PostgresEventEmitter,
        dsn="postgresql://postgres:postgres@localhost:5432/events",
        # channel_prefix="myapp_events_",  # customize per-app namespace
    ),
)
