"""Shared fixtures for the AI test suite. ``FEATURE_FLAG_AI`` ships on by
default, but tests in this directory may flip it off via the ``flag_off``
fixture or rebuild the settings module with the env var unset. This autouse
fixture restores the on-state at the start of every test so suites that
depend on the live ``/ai/*`` router (W10 skeleton, providers, BYOK,
streaming, audit) see the AI subsystem as enabled regardless of what the
prior test left behind. ``test_feature_flag.py`` opts back out of this
default when asserting the disabled branch.
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
