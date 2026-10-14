"""Unit tests for the integration-suite handler factories.

``tests/integration/_helpers.py`` builds plain litestar listeners with no
broker involved, so its contract is pinned here: the default suite measures
``tests/`` as well as ``src/`` (see ``source_pkgs``), and these lines would
otherwise only be covered when the Docker-backed integration suite runs.
"""

from __future__ import annotations


async def test_capture_handler_records_kwargs_and_signals() -> None:
    from tests.integration._helpers import make_capture_handler

    handler, received, captured = make_capture_handler("user_registered")
    assert handler.event_ids == frozenset({"user_registered"})
    assert received.is_set() is False

    await handler.fn(email="ada@example.com")
    assert received.is_set() is True
    assert captured == {"email": "ada@example.com"}

    # Second delivery is ignored (first-wins), covering the guard's other side.
    await handler.fn(email="grace@example.com")
    assert captured == {"email": "ada@example.com"}


async def test_failing_handler_errors() -> None:
    from tests.integration._helpers import make_failing_handler

    handler = make_failing_handler("isolation_check")
    assert handler.event_ids == frozenset({"isolation_check"})
    # Litestar wraps listener errors (logs instead of propagating), so the
    # raise inside is covered by asserting the call swallows it.
    assert await handler.fn(payload="ok") is None
