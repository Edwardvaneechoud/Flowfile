"""/telemetry consent endpoint tests.

Every test isolates the consent file by monkeypatching ``shared.telemetry._settings_file``
to a tmp_path, and resets the shared client's cached state so nothing leaks between tests.
``FLOWFILE_MODE`` is only ever set through ``monkeypatch.setenv`` — never process-wide,
since the whole suite shares this interpreter.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_core import events, main
from flowfile_core import telemetry as glue
from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from shared import telemetry
from tools.telemetry_collector.funnel import compute_funnel

STATUS_KEYS = {"available", "consent", "env_kill_switch", "endpoint_configured", "can_manage"}


@pytest.fixture
def admin_user() -> PydanticUser:
    return PydanticUser(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)


@pytest.fixture
def non_admin_user() -> PydanticUser:
    return PydanticUser(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)


def _client_for(user: PydanticUser) -> Iterator[TestClient]:
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def admin_client(admin_user: PydanticUser) -> Iterator[TestClient]:
    yield from _client_for(admin_user)


@pytest.fixture
def non_admin_client(non_admin_user: PydanticUser) -> Iterator[TestClient]:
    yield from _client_for(non_admin_user)


@pytest.fixture
def unauth_client() -> Iterator[TestClient]:
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)
    main.app.dependency_overrides.pop(get_current_admin_user, None)
    yield TestClient(main.app)


@pytest.fixture(autouse=True)
def isolated_consent_file(tmp_path, monkeypatch) -> Iterator[None]:
    """Point the consent store and the spool at tmp_path, and clear the client's cached state."""
    target = tmp_path / "telemetry.yaml"
    monkeypatch.setattr(telemetry, "_settings_file", lambda: target)
    monkeypatch.setattr(telemetry, "_spool_file", lambda: tmp_path / "telemetry_spool.jsonl")
    telemetry._reset_for_tests()
    yield
    telemetry._reset_for_tests()


def test_status_shape_and_no_install_id(admin_client: TestClient) -> None:
    response = admin_client.get("/telemetry/status")
    assert response.status_code == 200
    body = response.json()
    assert set(body) == STATUS_KEYS
    assert "install_id" not in body
    assert body["consent"] is None


def test_consent_grant_then_status_reflects_it(admin_client: TestClient, tmp_path) -> None:
    post = admin_client.post("/telemetry/consent", json={"enabled": True})
    assert post.status_code == 200
    assert post.json()["consent"] is True
    assert set(post.json()) == STATUS_KEYS

    assert admin_client.get("/telemetry/status").json()["consent"] is True
    stored = (tmp_path / "telemetry.yaml").read_text(encoding="utf-8")
    assert "consent: true" in stored
    assert "install_id:" in stored


def test_consent_revoke_clears_install_id(admin_client: TestClient, tmp_path) -> None:
    admin_client.post("/telemetry/consent", json={"enabled": True})
    response = admin_client.post("/telemetry/consent", json={"enabled": False})
    assert response.status_code == 200
    assert response.json()["consent"] is False

    stored = (tmp_path / "telemetry.yaml").read_text(encoding="utf-8")
    assert "consent: false" in stored
    assert "install_id" not in stored
    assert telemetry.install_id() is None


def test_unwritable_consent_file_returns_503_not_a_false_success(admin_client: TestClient, monkeypatch) -> None:
    """A read-only store must not answer 200 with consent still unset."""

    def _boom(src, dst):
        raise OSError("read-only file system")

    monkeypatch.setattr(os, "replace", _boom)
    response = admin_client.post("/telemetry/consent", json={"enabled": True})

    assert response.status_code == 503, "the renderer would otherwise toast a choice that was never stored"
    assert response.json()["detail"]["error_code"] == "TELEMETRY_PERSIST_FAILED"
    assert telemetry.consent() is None
    assert admin_client.get("/telemetry/status").json()["consent"] is None


def test_malformed_body_returns_422(admin_client: TestClient) -> None:
    assert admin_client.post("/telemetry/consent", json={}).status_code == 422
    assert admin_client.post("/telemetry/consent", json={"enabled": "maybe"}).status_code == 422


def test_unauthenticated_returns_401(unauth_client: TestClient) -> None:
    assert unauth_client.get("/telemetry/status").status_code == 401
    assert unauth_client.post("/telemetry/consent", json={"enabled": True}).status_code == 401


def test_docker_non_admin_cannot_manage(non_admin_client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    status = non_admin_client.get("/telemetry/status")
    assert status.status_code == 200
    assert status.json()["can_manage"] is False

    post = non_admin_client.post("/telemetry/consent", json={"enabled": True})
    assert post.status_code == 403
    assert telemetry.consent() is None


def test_docker_admin_can_manage(admin_client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    post = admin_client.post("/telemetry/consent", json={"enabled": True})
    assert post.status_code == 200
    assert post.json()["can_manage"] is True
    assert post.json()["consent"] is True


def test_non_docker_non_admin_can_manage(non_admin_client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    post = non_admin_client.post("/telemetry/consent", json={"enabled": True})
    assert post.status_code == 200
    assert post.json()["can_manage"] is True


def test_env_flags_are_reflected(admin_client: TestClient, monkeypatch) -> None:
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", "https://example.invalid/events")
    monkeypatch.delenv(telemetry.ENV_KILL_SWITCH, raising=False)
    admin_client.post("/telemetry/consent", json={"enabled": True})

    body = admin_client.get("/telemetry/status").json()
    assert body["endpoint_configured"] is True
    assert body["env_kill_switch"] is False
    assert body["available"] is True

    monkeypatch.setenv(telemetry.ENV_KILL_SWITCH, "off")
    body = admin_client.get("/telemetry/status").json()
    assert body["env_kill_switch"] is True
    assert body["available"] is False

    monkeypatch.delenv(telemetry.ENV_KILL_SWITCH, raising=False)
    monkeypatch.delenv("FLOWFILE_TELEMETRY_ENDPOINT", raising=False)
    # Releases bake in DEFAULT_ENDPOINT, so "no endpoint" needs that cleared too.
    monkeypatch.setattr(telemetry, "DEFAULT_ENDPOINT", "")
    body = admin_client.get("/telemetry/status").json()
    assert body["endpoint_configured"] is False
    assert body["available"] is False


def test_testing_env_marker_keeps_telemetry_unavailable(admin_client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", "https://example.invalid/events")
    assert os.environ.get("TESTING") == "True"
    body = admin_client.get("/telemetry/status").json()
    assert body["endpoint_configured"] is True
    assert body["available"] is False


@pytest.fixture
def sent(monkeypatch) -> Iterator[list[dict]]:
    """All four gates open, delivery captured at the client seam, worker thread off.

    The observer is re-wired from scratch so exactly one handler set sits on the
    bus (a bare ``_subscribe()`` would double-register), and left the way
    ``main.py`` left it.
    """
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.delenv(telemetry.ENV_KILL_SWITCH, raising=False)
    monkeypatch.setenv(telemetry.ENV_ENDPOINT, "https://example.invalid/events")

    captured: list[dict] = []

    def _capture(url: str, json_body: dict, timeout: float | None = None):
        captured.extend(json_body["events"])
        return None

    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)
    monkeypatch.setattr(telemetry, "_post", _capture)

    events._reset_for_tests()
    glue._subscribed = False
    glue._subscribe()
    yield captured
    events._reset_for_tests()
    glue._subscribed = False
    glue._subscribe()


def _drain(captured: list[dict]) -> list[dict]:
    telemetry.flush(1.0)
    return captured


def _names(captured: list[dict]) -> list[str]:
    return [envelope["event"] for envelope in _drain(captured)]


class _FakeGraph:
    """Enough of a graph for the run snapshot; a plain class so it can be weak-referenced."""

    class _Node:
        def __init__(self) -> None:
            self.node_type = "manual_input"
            self.node_id = 1
            self.node_template = SimpleNamespace(input=0)

    def __init__(self) -> None:
        self.nodes = [self._Node()]
        self.flow_settings = SimpleNamespace(is_canceled=False)


def test_granting_consent_publishes_one_app_started(admin_client: TestClient, sent: list[dict]) -> None:
    """The lifespan publish always precedes consent and is dropped; the grant is the first contact."""
    assert admin_client.post("/telemetry/consent", json={"enabled": True}).status_code == 200
    assert _names(sent) == ["app_started"]


def test_re_granting_and_revoking_publish_nothing(admin_client: TestClient, sent: list[dict]) -> None:
    admin_client.post("/telemetry/consent", json={"enabled": True})
    _drain(sent)
    sent.clear()

    admin_client.post("/telemetry/consent", json={"enabled": True})
    assert _names(sent) == [], "the install id is unchanged, so a repeat grant is not a new launch"

    admin_client.post("/telemetry/consent", json={"enabled": False})
    assert _names(sent) == []


def test_a_failed_consent_write_publishes_nothing(admin_client: TestClient, sent: list[dict], monkeypatch) -> None:
    def _boom(src, dst):
        raise OSError("read-only file system")

    monkeypatch.setattr(os, "replace", _boom)
    assert admin_client.post("/telemetry/consent", json={"enabled": True}).status_code == 503
    assert _names(sent) == []


def test_first_run_sequence_counts_as_launched_in_the_funnel(
    admin_client: TestClient, sent: list[dict], tmp_path
) -> None:
    """install -> grant -> run, read back through the real funnel: launched must equal installs."""
    events.publish("app_started")  # the lifespan publish, pre-consent
    admin_client.post("/telemetry/consent", json={"enabled": True})
    events.publish("flow_run_started", graph=_FakeGraph())

    envelopes = _drain(sent)
    path = tmp_path / "events.jsonl"
    path.write_text(
        "".join(json.dumps({**envelope, "received_at": envelope["ts"]}) + "\n" for envelope in envelopes),
        encoding="utf-8",
    )

    stats = compute_funnel(path)
    assert stats.installs == 1
    assert stats.run_attempted == 1
    assert stats.launched == stats.installs, "an install that consented and ran once must not read as a dead install"
