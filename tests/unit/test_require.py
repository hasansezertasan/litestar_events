"""Unit test for the ``require`` narrowing helper.

``require`` is the shared primitive behind the Optional-narrowing guards in the
publisher/consumer loops (queue, client, pool, producer). It stands in for
``assert x is not None`` but raises unconditionally, so it survives ``python
-O`` where ``assert`` would be stripped -- turning a stray ``None`` into a clear
``RuntimeError`` instead of an opaque ``AttributeError`` inside a detached task.
"""

from __future__ import annotations

import pytest

from litestar_events._queue import require


def test_require_returns_value_when_set() -> None:
    sentinel = object()
    assert require(sentinel, "thing") is sentinel


def test_require_raises_when_none() -> None:
    with pytest.raises(RuntimeError, match="thing is unavailable"):
        require(None, "thing")


@pytest.mark.parametrize("value", [0, "", [], False])
def test_require_treats_falsy_non_none_as_present(value: object) -> None:
    # Only ``None`` is missing; falsy-but-set values (0, "", [], False) pass
    # through unchanged rather than tripping the guard.
    assert require(value, "thing") is value
