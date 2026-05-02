"""Shared fixtures for the AI test suite.

W17 introduces a process-wide ``FEATURE_FLAG_AI`` kill switch. In production
the flag ships off through Phase 0, so unit tests must explicitly opt in to
exercise the live ``/ai/*`` router. This autouse fixture flips the flag on
for every test in ``flowfile_core/tests/ai/`` so workstream suites (W10
skeleton, W11 providers, W12 BYOK, W13 streaming, W15 audit) see the AI
subsystem as enabled. ``test_feature_flag.py`` opts back out of this default
when asserting the disabled branch.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from flowfile_core.configs.settings import FEATURE_FLAG_AI


@pytest.fixture(autouse=True)
def _ai_feature_enabled() -> Iterator[None]:
    original = FEATURE_FLAG_AI.value
    FEATURE_FLAG_AI.set(True)
    try:
        yield
    finally:
        FEATURE_FLAG_AI.set(original)
