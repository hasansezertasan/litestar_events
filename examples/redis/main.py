"""Example Litestar app using the Redis event emitter backend.

Run a local Redis first, e.g.:

    docker run --rm -p 6379:6379 redis:7

Then start the app:

    uv run litestar --app examples.redis.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \
        -H 'content-type: application/json' \
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event will travel through Redis Pub/Sub and be picked
up by both listeners below. Note: Redis Pub/Sub is fire-and-forget. Events
emitted while no subscriber is connected are lost. If you need durability,
use the rabbit backend instead.

Inspect live channels with:

    redis-cli PUBSUB CHANNELS 'litestar.events:*'
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.redis import RedisEventEmitter

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
        RedisEventEmitter,
        redis_url="redis://localhost:6379/0",
        # channel_prefix="myapp.events:",  # customize per-app namespace
    ),
)
