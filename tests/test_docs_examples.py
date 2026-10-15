"""Keep the Python modules embedded in the documentation executable."""

from __future__ import annotations

import importlib.util
import sys
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

EXAMPLES_DIR = Path(__file__).parents[1] / "docs" / "examples"
REPO_EXAMPLES_DIR = Path(__file__).parents[1] / "examples"
DISTRIBUTION_NAME = "litestar_events"


def _load_example(path: Path) -> ModuleType:
    """Import one documentation example directly from its source path."""
    spec = importlib.util.spec_from_file_location(f"docs_example_{path.stem}", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    # Litestar resolves postponed annotations through ``sys.modules``.
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_all_documentation_examples_are_importable() -> None:
    """Every nested Python module in ``docs/examples`` imports successfully."""
    examples = sorted(EXAMPLES_DIR.rglob("*.py"))
    assert examples, "docs/examples must contain at least one tested module"
    for path in examples:
        _load_example(path)


def test_version_lookup_example_uses_the_installed_distribution() -> None:
    """The usage-page example resolves the same version as package metadata."""
    example = _load_example(EXAMPLES_DIR / "version_lookup.py")
    assert example.version_lookup() == version(DISTRIBUTION_NAME)


def test_producer_worker_example_builds_a_producer_app() -> None:
    """The example app wires the plugin and configures the web role."""
    module = _load_example(REPO_EXAMPLES_DIR / "producer_worker" / "app.py")
    assert module.app.event_emitter.role.value == "producer"
