"""admin AI feature-flag toggle tests.

Cases:

* ``test_admin_enable_flag_sets_state_and_env`` — admin POSTs ``{"enabled":
  True}``: 200, ``FEATURE_FLAG_AI`` MutableBool flips True, ``os.environ``
  shows ``"true"``, ``is_ai_enabled()`` returns True immediately.
* ``test_admin_disable_flag_sets_state_and_env`` — admin POSTs ``{"enabled":
  False}``: 200, MutableBool flips False, env shows ``"false"``,
  ``is_ai_enabled()`` returns False.
* ``test_non_admin_cannot_toggle_returns_403`` — authenticated non-admin →
  403 Forbidden; ``os.environ`` and the MutableBool unchanged.
* ``test_unauthenticated_returns_401`` — no auth header → 401 Unauthorized;
  state unchanged.
* ``test_endpoint_reachable_when_flag_off`` — the whole point of this
  endpoint is that it works when the AI gate is off; flip ``FEATURE_FLAG_AI``
  to False, POST to ``/system/feature_flags/ai`` succeeds (200), and the
  AI router still 503s on its own routes (proving the admin endpoint is
  mounted outside the gated router).
* ``test_get_returns_current_state`` — GET reflects the live MutableBool;
  same admin-only auth as POST.
* ``test_missing_body_returns_422`` — Pydantic rejects body without
  ``enabled``.
* ``test_response_shape_is_stable`` — frontend reads ``enabled`` and
  ``persisted``; this guards the contract.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai.feature_flag import is_ai_enabled
from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings

# ---------- shared fixtures ----------


@pytest.fixture
def admin_user() -> PydanticUser:
    return PydanticUser(
        username="local_user",
        id=1,
        disabled=False,
        is_admin=True,
        must_change_password=False,
    )


@pytest.fixture
def non_admin_user() -> PydanticUser:
    return PydanticUser(
        username="not_admin",
        id=2,
        disabled=False,
        is_admin=False,
        must_change_password=False,
    )


@pytest.fixture
def admin_client(admin_user: PydanticUser) -> Iterator[TestClient]:
    """TestClient with auth overridden to return an admin user.

    Override both ``get_current_active_user`` (for any active-user endpoints)
    and ``get_current_user`` (the dep that ``get_current_admin_user`` resolves
    through). Bypasses the JWT round-trip entirely for these unit tests.
    """
    main.app.dependency_overrides[get_current_active_user] = lambda: admin_user
    main.app.dependency_overrides[get_current_user] = lambda: admin_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def non_admin_client(non_admin_user: PydanticUser) -> Iterator[TestClient]:
    """TestClient with auth resolving to a non-admin — 403 on admin routes."""
    main.app.dependency_overrides[get_current_active_user] = lambda: non_admin_user
    main.app.dependency_overrides[get_current_user] = lambda: non_admin_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def unauth_client() -> Iterator[TestClient]:
    """TestClient with no auth override — relies on the default JWT path which
    will reject because no Authorization header is sent."""
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)
    main.app.dependency_overrides.pop(get_current_admin_user, None)
    yield TestClient(main.app)


@pytest.fixture
def restore_flag_state() -> Iterator[None]:
    """Capture FEATURE_FLAG_AI + os.environ entry; restore on teardown.

    The autouse ``_ai_feature_enabled`` fixture from ``conftest.py`` sets the
    flag True before the test runs; this fixture goes one step further and
    snapshots ``os.environ["FEATURE_FLAG_AI"]`` so a test that pokes the env
    var doesn't bleed into the next test.
    """
    original_flag = core_settings.FEATURE_FLAG_AI.value
    original_env = os.environ.get("FEATURE_FLAG_AI")
    try:
        yield
    finally:
        core_settings.FEATURE_FLAG_AI.set(original_flag)
        if original_env is None:
            os.environ.pop("FEATURE_FLAG_AI", None)
        else:
            os.environ["FEATURE_FLAG_AI"] = original_env


# ---------- happy paths ----------


def test_admin_enable_flag_sets_state_and_env(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    """Admin enable: 200, body matches contract, MutableBool + env in sync."""
    core_settings.FEATURE_FLAG_AI.set(False)

    response = admin_client.post("/system/feature_flags/ai", json={"enabled": True})

    assert response.status_code == 200, response.text
    assert response.json() == {"enabled": True, "persisted": False}
    assert os.environ["FEATURE_FLAG_AI"] == "true"
    assert is_ai_enabled() is True


def test_admin_disable_flag_sets_state_and_env(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    core_settings.FEATURE_FLAG_AI.set(True)

    response = admin_client.post("/system/feature_flags/ai", json={"enabled": False})

    assert response.status_code == 200, response.text
    assert response.json() == {"enabled": False, "persisted": False}
    assert os.environ["FEATURE_FLAG_AI"] == "false"
    assert is_ai_enabled() is False


# ---------- auth gates ----------


def test_non_admin_cannot_toggle_returns_403(
    non_admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    """Non-admin → 403; nothing mutates."""
    core_settings.FEATURE_FLAG_AI.set(True)
    pre_env = os.environ.get("FEATURE_FLAG_AI")

    response = non_admin_client.post("/system/feature_flags/ai", json={"enabled": False})

    assert response.status_code == 403, response.text
    assert "admin" in response.json()["detail"].lower()
    assert is_ai_enabled() is True
    assert os.environ.get("FEATURE_FLAG_AI") == pre_env


def test_unauthenticated_returns_401(
    unauth_client: TestClient,
    restore_flag_state: None,
) -> None:
    """No auth header → 401; nothing mutates."""
    core_settings.FEATURE_FLAG_AI.set(True)
    pre_env = os.environ.get("FEATURE_FLAG_AI")

    response = unauth_client.post("/system/feature_flags/ai", json={"enabled": False})

    assert response.status_code == 401, response.text
    assert is_ai_enabled() is True
    assert os.environ.get("FEATURE_FLAG_AI") == pre_env


# ---------- mounted outside the gated router ----------


def test_endpoint_reachable_when_flag_off(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    """The whole point: admin endpoint works even when the AI gate is closed.

    Flip the flag off, POST succeeds (200), then verify the AI gate is still
    closing the AI router — proving the admin endpoint sits *outside* the
    gated router rather than inheriting its dependency.
    """
    core_settings.FEATURE_FLAG_AI.set(False)
    assert is_ai_enabled() is False

    response = admin_client.post("/system/feature_flags/ai", json={"enabled": True})
    assert response.status_code == 200, response.text

    # Flip back off and verify the AI router is still gated by — i.e.
    # the admin endpoint is NOT under the same dependency.
    core_settings.FEATURE_FLAG_AI.set(False)
    ai_response = admin_client.get("/ai/health")
    assert ai_response.status_code == 503, (
        f"AI router should still 503 when flag is off; got {ai_response.status_code}. "
        "If this passes 200, the admin endpoint may have unintentionally been "
        "mounted under the gated /ai router."
    )


# ---------- read endpoint ----------


def test_get_returns_current_state(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    core_settings.FEATURE_FLAG_AI.set(True)
    response = admin_client.get("/system/feature_flags/ai")
    assert response.status_code == 200, response.text
    assert response.json() == {"enabled": True, "persisted": False}

    core_settings.FEATURE_FLAG_AI.set(False)
    response = admin_client.get("/system/feature_flags/ai")
    assert response.status_code == 200, response.text
    assert response.json() == {"enabled": False, "persisted": False}


def test_get_requires_admin(
    non_admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    response = non_admin_client.get("/system/feature_flags/ai")
    assert response.status_code == 403, response.text


# ---------- request validation ----------


def test_missing_body_returns_422(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    response = admin_client.post("/system/feature_flags/ai", json={})
    assert response.status_code == 422, response.text


def test_invalid_enabled_type_returns_422(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    response = admin_client.post("/system/feature_flags/ai", json={"enabled": "maybe"})
    assert response.status_code == 422, response.text


# ---------- contract ----------


def test_response_shape_is_stable(
    admin_client: TestClient,
    restore_flag_state: None,
) -> None:
    """The frontend reads ``enabled`` and ``persisted`` — guard the contract.

    ``persisted`` is intentionally always ``False`` in; cross-restart
    persistence is the user's responsibility (set ``FEATURE_FLAG_AI=true`` in
    ``.env``). If a future workstream adds a write-to-disk path, that change
    should be opt-in via a separate field rather than flipping this one.
    """
    response = admin_client.post("/system/feature_flags/ai", json={"enabled": True})
    body = response.json()
    assert set(body.keys()) == {"enabled", "persisted"}
    assert isinstance(body["enabled"], bool)
    assert body["persisted"] is False
