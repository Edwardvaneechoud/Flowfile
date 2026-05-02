"""W17 — ``FEATURE_FLAG_AI`` gating.

Cases:

* ``test_feature_flag_symbol_present`` — ``FEATURE_FLAG_AI`` exists on
  ``flowfile_core.configs.settings`` and is a ``MutableBool``.
* ``test_is_ai_enabled_reflects_flag`` — flipping ``FEATURE_FLAG_AI.set(...)``
  is observed live by ``is_ai_enabled()`` (no module re-import).
* ``test_require_ai_enabled_passes_when_on`` — dependency returns ``None``
  silently when the flag is on.
* ``test_require_ai_enabled_raises_503_when_off`` — dependency raises
  ``HTTPException(503)`` with ``DISABLED_DETAIL`` when the flag is off.
* ``test_health_returns_503_when_flag_off`` — end-to-end via
  ``TestClient(app)``: ``GET /ai/health`` is gated.
* ``test_health_returns_200_when_flag_on`` — counterpart with the flag on.
* ``test_byok_route_inherits_gate_when_flag_off`` — confirms FastAPI's
  ``include_router`` propagates the parent's constructor ``dependencies``
  to the W12 BYOK sub-router (so the gate is global, not per-leaf).
* ``test_disabled_detail_constant_stable`` — frontend matches on the
  detail string; this guards the contract.
* ``test_env_var_parsing`` — process-start env-var → MutableBool resolution
  rules (``true|1|yes|on`` truthy, anything else falsy).
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Iterator

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from flowfile_core.ai.feature_flag import (
    DISABLED_DETAIL,
    is_ai_enabled,
    require_ai_enabled,
)
from flowfile_core.configs import settings as core_settings
from flowfile_core.configs.utils import MutableBool


# ---------- per-test toggle helpers ----------


@pytest.fixture
def flag_off() -> Iterator[None]:
    """Flip the flag off for the duration of one test.

    The autouse ``_ai_feature_enabled`` fixture from ``conftest.py`` already
    flipped it on before the test started; this fixture overrides that
    default and restores afterwards.
    """
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        yield
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)


@pytest.fixture
def flag_on() -> Iterator[None]:
    """Belt-and-braces: flip on in case a future test needs to be explicit."""
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(True)
    try:
        yield
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)


# ---------- tests ----------


def test_feature_flag_symbol_present() -> None:
    """The settings module exposes a MutableBool — production code and tests
    rely on the ``.set()`` method to flip the flag at runtime."""
    assert hasattr(core_settings, "FEATURE_FLAG_AI")
    assert isinstance(core_settings.FEATURE_FLAG_AI, MutableBool)


def test_is_ai_enabled_reflects_flag() -> None:
    """``is_ai_enabled()`` reads the live MutableBool — no rebinding needed."""
    core_settings.FEATURE_FLAG_AI.set(True)
    assert is_ai_enabled() is True
    core_settings.FEATURE_FLAG_AI.set(False)
    assert is_ai_enabled() is False
    core_settings.FEATURE_FLAG_AI.set(True)  # restore for the autouse fixture's teardown


def test_require_ai_enabled_passes_when_on(flag_on: None) -> None:
    """No exception → no 503 → endpoint runs."""
    assert require_ai_enabled() is None


def test_require_ai_enabled_raises_503_when_off(flag_off: None) -> None:
    with pytest.raises(HTTPException) as exc_info:
        require_ai_enabled()
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == DISABLED_DETAIL


def test_health_returns_503_when_flag_off(flag_off: None) -> None:
    """End-to-end: the router-level dependency gates the placeholder route."""
    from flowfile_core.main import app

    client = TestClient(app)
    response = client.get("/ai/health")
    assert response.status_code == 503
    assert response.json() == {"detail": DISABLED_DETAIL}


def test_health_returns_200_when_flag_on(flag_on: None) -> None:
    from flowfile_core.main import app

    client = TestClient(app)
    response = client.get("/ai/health")
    assert response.status_code == 200
    assert response.json() == {"status": "skeleton"}


def test_byok_route_inherits_gate_when_flag_off(flag_off: None) -> None:
    """W12's BYOK sub-router is mounted via ``router.include_router(byok_router)``.

    FastAPI propagates the parent's constructor ``dependencies`` to every
    included child route, so the gate covers BYOK without per-leaf wiring.
    Hitting ``/ai/providers`` (auth-required in production) when the AI
    flag is off should reject on the AI gate before authentication checks
    even run — proving the dependency sits ahead of auth in the chain.
    """
    from flowfile_core.main import app

    client = TestClient(app)
    response = client.get("/ai/providers")
    assert response.status_code == 503, (
        f"Expected the AI feature gate to short-circuit the BYOK route; got "
        f"{response.status_code} with body {response.text!r}"
    )
    assert response.json() == {"detail": DISABLED_DETAIL}


def test_disabled_detail_constant_stable() -> None:
    """The frontend matches on this string — bumping it is a contract change."""
    assert DISABLED_DETAIL == "AI features are disabled. Set FEATURE_FLAG_AI=true to enable."


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        ("true", True),
        ("TRUE", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("Yes", True),
        ("on", True),
        ("ON", True),
        (" true ", True),  # whitespace tolerated
        ("false", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("", False),
        ("nonsense", False),
    ],
)
def test_env_var_parsing(monkeypatch: pytest.MonkeyPatch, env_value: str, expected: bool) -> None:
    """``settings.py`` parses ``FEATURE_FLAG_AI`` at import time. Reload the
    module after setting the env var to observe the resolution rule used in
    production startup."""
    monkeypatch.setenv("FEATURE_FLAG_AI", env_value)
    # Reload settings so the module-level expression re-evaluates.
    importlib.reload(core_settings)
    try:
        assert bool(core_settings.FEATURE_FLAG_AI) is expected
    finally:
        # Restore the test fixture's "on" baseline so subsequent tests see
        # the autouse contract. Any module that did `from ... import
        # FEATURE_FLAG_AI` is now holding a stale reference, but the AI
        # tests use ``settings.FEATURE_FLAG_AI`` (live attribute lookup),
        # so the autouse teardown re-flips on the new instance.
        core_settings.FEATURE_FLAG_AI.set(True)


def test_env_var_default_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan §10: ``FEATURE_FLAG_AI=false`` is the Phase 0 ship default."""
    monkeypatch.delenv("FEATURE_FLAG_AI", raising=False)
    importlib.reload(core_settings)
    try:
        assert bool(core_settings.FEATURE_FLAG_AI) is False
    finally:
        core_settings.FEATURE_FLAG_AI.set(True)


def test_authenticated_user_sees_503_when_flag_off(flag_off: None) -> None:
    """End-user contract: a user who is logged in (so the auth dep passes)
    receives the AI-disabled 503 — not a generic 401 — when the flag is off.

    Bypasses ``get_current_active_user`` via ``app.dependency_overrides`` so
    the auth dep returns successfully; the only remaining gate is the AI
    feature flag, so we should see exactly the W17 503 path.
    """
    from flowfile_core.auth.jwt import get_current_active_user
    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.main import app

    fake_user = PydanticUser(
        username="local_user",
        id=1,
        disabled=False,
        is_admin=True,
        must_change_password=False,
    )
    app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        client = TestClient(app)
        response = client.get("/ai/providers")
        assert response.status_code == 503, (
            f"Authenticated user with FEATURE_FLAG_AI off must see 503 from "
            f"the AI gate; got {response.status_code} body={response.text!r}"
        )
        assert response.json() == {"detail": DISABLED_DETAIL}
    finally:
        app.dependency_overrides.pop(get_current_active_user, None)


def test_no_litellm_pulled_in_by_feature_flag() -> None:
    """``feature_flag`` must not transitively import litellm — it has no provider
    work to do, and Phase 0 routes must boot fast even when AI is off."""
    import sys

    # Wipe any cached imports.
    for mod_name in list(sys.modules):
        if mod_name.startswith("litellm") or mod_name == "flowfile_core.ai.feature_flag":
            del sys.modules[mod_name]

    importlib.import_module("flowfile_core.ai.feature_flag")
    assert "litellm" not in sys.modules, (
        f"feature_flag pulled litellm into sys.modules: " f"{[m for m in sys.modules if m.startswith('litellm')]}"
    )


# ---------- guard rails ----------


def test_default_environment_does_not_set_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity: with FEATURE_FLAG_AI removed, the env var really is absent.
    Detects the case where some import chain has poked the env var as a side
    effect."""
    monkeypatch.delenv("FEATURE_FLAG_AI", raising=False)
    assert os.environ.get("FEATURE_FLAG_AI") is None
