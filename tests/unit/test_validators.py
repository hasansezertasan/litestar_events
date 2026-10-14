"""Unit tests for each backend's channel/topic/subject validator.

These run with no broker, no network, no event loop. They pin the validation
rules so a regression (e.g. a regex tweak that accidentally accepts wildcards)
is caught immediately.
"""

from __future__ import annotations

import pytest


class TestPostgresValidator:
    """psycopg LISTEN/NOTIFY channels must be valid PG identifiers."""

    @pytest.fixture
    def _validate(self):
        from litestar_events.contrib.postgres.emitter import _validate_channel

        return _validate_channel

    @pytest.mark.parametrize(
        "channel",
        [
            "user_registered",
            "litestar_events_user_registered",
            "_leading_underscore",
            "with$dollar",
            "Mixed_Case_42",
        ],
    )
    def test_accepts_valid(self, _validate, channel) -> None:
        _validate(channel)

    @pytest.mark.parametrize(
        "channel",
        [
            "",
            "1leading_digit",
            "user.registered",
            "user-registered",
            "user:registered",
            "has space",
            "a" * 64,  # 64 bytes — one past the 63-byte limit
        ],
    )
    def test_rejects_invalid(self, _validate, channel) -> None:
        with pytest.raises(ValueError):
            _validate(channel)


class TestNATSValidator:
    """NATS subjects are dot-separated; tokens may not contain spaces, *, or >."""

    @pytest.fixture
    def _validate(self):
        from litestar_events.contrib.nats.emitter import _validate_subject

        return _validate_subject

    @pytest.mark.parametrize(
        "subject",
        [
            "user_registered",
            "litestar.events.user_registered",
            "a.b.c.d",
            "has-dashes",
            "has:colons",
        ],
    )
    def test_accepts_valid(self, _validate, subject) -> None:
        _validate(subject)

    @pytest.mark.parametrize(
        "subject",
        [
            "",
            "trailing.",
            ".leading",
            "double..dot",
            "with space",
            "with*star",
            "with>angle",
        ],
    )
    def test_rejects_invalid(self, _validate, subject) -> None:
        with pytest.raises(ValueError):
            _validate(subject)


class TestKafkaValidator:
    """Kafka topic names: [A-Za-z0-9._-], 1..249 chars. Shared by kafka & confluent."""

    @pytest.fixture(params=["kafka", "confluent"])
    def _validate(self, request):
        if request.param == "kafka":
            from litestar_events.contrib.kafka.emitter import _validate_topic
        else:
            from litestar_events.contrib.confluent.emitter import _validate_topic
        return _validate_topic

    @pytest.mark.parametrize(
        "topic",
        [
            "user_registered",
            "litestar.events.user_registered",
            "with-dashes",
            "with.dots",
            "1leading_digit_is_fine_in_kafka",
            "a" * 249,
        ],
    )
    def test_accepts_valid(self, _validate, topic) -> None:
        _validate(topic)

    @pytest.mark.parametrize(
        "topic", ["", "has space", "has:colon", "has/slash", "a" * 250]
    )
    def test_rejects_invalid(self, _validate, topic) -> None:
        with pytest.raises(ValueError):
            _validate(topic)


class TestMQTTValidator:
    """MQTT topics: anything except + (wildcard), # (wildcard), NUL, empty."""

    @pytest.fixture
    def _validate(self):
        from litestar_events.contrib.mqtt.emitter import _validate_topic

        return _validate_topic

    @pytest.mark.parametrize(
        "topic",
        [
            "user_registered",
            "litestar/events/user_registered",
            "has-dashes",
            "has.dots",
            "even spaces are fine in mqtt",
        ],
    )
    def test_accepts_valid(self, _validate, topic) -> None:
        _validate(topic)

    @pytest.mark.parametrize(
        "topic", ["", "litestar/+/oops", "litestar/#", "has\x00nul"]
    )
    def test_rejects_invalid(self, _validate, topic) -> None:
        with pytest.raises(ValueError):
            _validate(topic)
