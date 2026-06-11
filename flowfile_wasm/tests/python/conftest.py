"""Pytest setup for the in-browser Pyodide engine.

`src/pyodide/engine.py` is the exact Python that ships into the app as text via a
`?raw` import (see src/stores/pyodide-store.ts). It imports only polars/pydantic
plus stdlib and touches no Pyodide-specific API, so we import it here as a normal
module and exercise the real, shipped code under CPython.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "pyodide"))

import engine  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_engine_state():
    """Start every test from a clean engine (LazyFrame registry + caches)."""
    engine.clear_all()
    yield
    engine.clear_all()
