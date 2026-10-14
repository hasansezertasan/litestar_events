"""Smoke tests for litestar_events package."""


def test_smoke() -> None:
    """Test that the package can be imported."""
    import litestar_events

    assert litestar_events is not None
