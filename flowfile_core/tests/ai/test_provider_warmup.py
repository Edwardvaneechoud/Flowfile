"""Unit tests for the litellm provider-SDK warm-up (keeps the heavy import off the
request hot path so a cold first AI call doesn't spuriously time out)."""

from __future__ import annotations

import threading
from unittest.mock import Mock

from flowfile_core.ai.providers import _litellm_base


def test_prewarm_imports_litellm_once(monkeypatch) -> None:
    fake = Mock()
    monkeypatch.setattr(_litellm_base, "_lazy_litellm", fake)
    _litellm_base.prewarm()
    fake.assert_called_once_with()


def test_prewarm_swallows_failure(monkeypatch) -> None:
    def boom() -> None:
        raise RuntimeError("litellm unavailable")

    monkeypatch.setattr(_litellm_base, "_lazy_litellm", boom)
    # Warm-up is best-effort and must never propagate — startup depends on it.
    _litellm_base.prewarm()


def test_start_prewarm_runs_in_background(monkeypatch) -> None:
    done = threading.Event()
    monkeypatch.setattr(_litellm_base, "_lazy_litellm", lambda: done.set())
    _litellm_base.start_prewarm()
    assert done.wait(timeout=2.0), "start_prewarm did not invoke prewarm in a thread"
