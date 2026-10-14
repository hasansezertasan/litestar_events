"""Smoke tests: every backend imports and exposes a BaseEventEmitterBackend subclass.

These catch packaging / __init__.py regressions (like the rabbit aio_pika
import bug we hit early on) before they reach integration tests.
"""

from __future__ import annotations

import importlib

import pytest
from litestar.events import BaseEventEmitterBackend

BACKENDS = [
    ("litestar_events.contrib.confluent", "ConfluentEventEmitter"),
    ("litestar_events.contrib.kafka", "KafkaEventEmitter"),
    ("litestar_events.contrib.mqtt", "MQTTEventEmitter"),
    ("litestar_events.contrib.nats", "NATSEventEmitter"),
    ("litestar_events.contrib.postgres", "PostgresEventEmitter"),
    ("litestar_events.contrib.pubsub", "PubSubEventEmitter"),
    ("litestar_events.contrib.rabbit", "RabbitEventEmitter"),
    ("litestar_events.contrib.redis", "RedisEventEmitter"),
    ("litestar_events.contrib.sqs", "SQSEventEmitter"),
    ("litestar_events.contrib.zmq", "ZeroMQEventEmitter"),
]


@pytest.mark.smoke
@pytest.mark.parametrize(("module_path", "class_name"), BACKENDS)
def test_backend_importable(module_path, class_name) -> None:
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    assert issubclass(cls, BaseEventEmitterBackend), (
        f"{class_name} must subclass BaseEventEmitterBackend"
    )
    assert class_name in mod.__all__, (
        f"{class_name} must appear in {module_path}.__all__"
    )


@pytest.mark.smoke
@pytest.mark.parametrize(("module_path", "class_name"), BACKENDS)
def test_backend_constructs_with_empty_listeners(module_path, class_name) -> None:
    """Sync construction should not touch the network."""
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    if class_name == "PostgresEventEmitter":
        instance = cls([], dsn="postgresql://localhost/x")
    elif class_name == "PubSubEventEmitter":
        instance = cls([], project_id="test-project")
    else:
        instance = cls([])
    assert isinstance(instance, BaseEventEmitterBackend)
