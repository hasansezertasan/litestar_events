"""Session-scoped broker fixtures backed by testcontainers.

Each container starts once per test session. Tests use unique event_ids /
channel prefixes (typically the test function name) to avoid cross-test
bleed inside a shared broker.
"""

from __future__ import annotations

import time

import pytest


@pytest.fixture(scope="session")
def rabbit_url():
    from testcontainers.community.rabbitmq import RabbitMqContainer

    with RabbitMqContainer("rabbitmq:3-management") as c:
        params = c.get_connection_params()
        yield f"amqp://guest:guest@{params.host}:{params.port}/"


@pytest.fixture(scope="session")
def redis_url():
    from testcontainers.community.redis import RedisContainer

    with RedisContainer("redis:7") as c:
        yield f"redis://{c.get_container_host_ip()}:{c.get_exposed_port(6379)}/0"


@pytest.fixture(scope="session")
def postgres_dsn():
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as c:
        yield c.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest.fixture(scope="session")
def sqs_endpoint():
    from testcontainers.community.localstack import LocalStackContainer

    with LocalStackContainer("localstack/localstack:3").with_services("sqs") as c:
        yield c.get_url()


@pytest.fixture(scope="session")
def pubsub_emulator():
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = DockerContainer(
        "messagebird/gcloud-pubsub-emulator:latest"
    ).with_exposed_ports(8681)
    container.waiting_for(
        LogMessageWaitStrategy("Server started").with_startup_timeout(60)
    )
    with container as c:
        yield f"{c.get_container_host_ip()}:{c.get_exposed_port(8681)}"


@pytest.fixture(scope="session")
def kafka_bootstrap():
    from testcontainers.community.kafka import KafkaContainer

    with KafkaContainer() as c:
        yield c.get_bootstrap_server()


@pytest.fixture(scope="session")
def nats_url():
    from testcontainers.community.nats import NatsContainer

    with NatsContainer() as c:
        yield c.nats_uri()


@pytest.fixture(scope="session")
def mqtt_host_port():
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    config = b"listener 1883\nallow_anonymous true\n"
    container = (
        DockerContainer("eclipse-mosquitto:2")
        .with_exposed_ports(1883)
        .with_command(
            "sh -c 'printf %s \"" + config.decode() + '" > /m.conf && '
            "mosquitto -c /m.conf'"
        )
    )
    container.waiting_for(
        LogMessageWaitStrategy("mosquitto version").with_startup_timeout(30)
    )
    with container as c:
        time.sleep(0.5)
        yield c.get_container_host_ip(), int(c.get_exposed_port(1883))
