"""Example Litestar app using the MQTT event emitter backend (aiomqtt).

Run a local MQTT broker first, e.g.:

    docker run --rm -p 1883:1883 eclipse-mosquitto:2 \\
        sh -c 'echo "listener 1883\\nallow_anonymous true" > /m.conf && mosquitto -c /m.conf'

Then start the app:

    uv run litestar --app examples.mqtt.main:app run

And trigger an event:

    curl -X POST http://localhost:8000/users \\
        -H 'content-type: application/json' \\
        -d '{"email": "ada@example.com"}'

The ``user_registered`` event travels through MQTT. Default QoS is 0
(at-most-once). Bump to 1 or 2 for stronger guarantees.

Inspect from the mosquitto CLI:

    mosquitto_sub -h localhost -t 'litestar/events/#'
"""

from __future__ import annotations

from functools import partial
from typing import Any

from litestar import Litestar, Request, post
from litestar.events import listener

from litestar_events.contrib.mqtt import MQTTEventEmitter

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
        MQTTEventEmitter,
        hostname="localhost",
        port=1883,
        # qos=1,  # at-least-once
        # topic_prefix="myapp/events/",
    ),
)
