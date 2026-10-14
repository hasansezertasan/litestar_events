"""Unit test for the Postgres emitter's ``dsn``/``pool`` construction contract.

The backend owns the pool only when constructed from a ``dsn``; otherwise it
borrows a caller-supplied ``pool``. Exactly one must be given -- that XOR
invariant is what makes the later ``require(self._dsn, ...)`` narrowing under
the pool-ownership branch safe. Enforced in ``__init__``, so no broker is
needed to cover it.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from litestar_events.contrib.postgres.emitter import PostgresEventEmitter


def test_neither_dsn_nor_pool_rejected() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PostgresEventEmitter([])


def test_both_dsn_and_pool_rejected() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        PostgresEventEmitter([], dsn="postgresql://x", pool=cast(Any, object()))


def test_dsn_only_owns_pool() -> None:
    emitter = PostgresEventEmitter([], dsn="postgresql://x")
    assert emitter._owns_pool is True


def test_pool_only_does_not_own_pool() -> None:
    emitter = PostgresEventEmitter([], pool=cast(Any, object()))
    assert emitter._owns_pool is False
