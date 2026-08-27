"""Route tests for the notification channel / rule / history endpoints.

Run with:
    pytest flowfile_core/tests/test_notifications_api.py -v

SSRF policy in these tests: the webhook host is always a **public IP literal**
(``93.184.216.34``), so ``validate_webhook_url`` resolves it locally and passes with
no network and no DNS. ``FLOWFILE_NOTIFY_ALLOW_PRIVATE_HOSTS`` is deliberately *not*
set — the rejection tests need the guard armed.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.auth.jwt import get_current_active_user, get_current_admin_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    NotificationChannel,
    NotificationOutbox,
    NotificationRule,
    User,
)
from flowfile_core.routes.notifications import _mask_url
from shared.notifications import senders

# Public IP literal: passes the SSRF guard without touching DNS or the network.
PUBLIC_URL = "https://93.184.216.34/services/T000/B000/9f3ab"
OTHER_PUBLIC_URL = "https://93.184.216.34/services/T111/B111/1c2d"

OWNER_ID = 1
OTHER_OWNER_USERNAME = "notify_other_owner"


# ---------- fixtures ----------


@pytest.fixture
def other_owner_id() -> int:
    """A second real user row, so owner-scoping is exercised against a live FK."""
    with get_db_context() as db:
        user = db.query(User).filter(User.username == OTHER_OWNER_USERNAME).first()
        if user is None:
            user = User(
                username=OTHER_OWNER_USERNAME,
                email=f"{OTHER_OWNER_USERNAME}@example.com",
                full_name="Other Owner",
                hashed_password="x",
                disabled=False,
                is_admin=False,
                must_change_password=False,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        return user.id


def _user_from_header(request: Request) -> PydanticUser:
    """Resolve the caller from a test-only header.

    ``dependency_overrides`` is app-global, so a per-client closure would let the
    second client's override clobber the first's and quietly make both requests the
    same user — which is exactly what the owner-scoping tests must not do. Keying on
    a header keeps two identities live against one override.
    """
    user_id = int(request.headers.get("X-Test-User-Id", OWNER_ID))
    return PydanticUser(id=user_id, username=f"user_{user_id}")


@pytest.fixture
def authed_app() -> Iterator[None]:
    """Override auth for the whole test.

    The ``/auth/token`` round-trip can only ever mint ``local_user`` (id 1) in
    electron mode, so a second owner has to arrive through the dependency override.
    """
    main.app.dependency_overrides[get_current_active_user] = _user_from_header
    try:
        yield
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _client_for(user_id: int) -> TestClient:
    client = TestClient(main.app)
    client.headers = {"X-Test-User-Id": str(user_id)}
    return client


@pytest.fixture
def client(authed_app: None) -> TestClient:
    return _client_for(OWNER_ID)


@pytest.fixture
def other_client(authed_app: None, other_owner_id: int) -> TestClient:
    return _client_for(other_owner_id)


@pytest.fixture(autouse=True)
def clean_notification_tables() -> Iterator[None]:
    """Notifications are global rows, not flow-scoped — wipe them around every test."""

    def _wipe() -> None:
        with get_db_context() as db:
            db.query(NotificationOutbox).delete()
            db.query(NotificationRule).delete()
            db.query(NotificationChannel).delete()
            db.commit()

    _wipe()
    yield
    _wipe()


@pytest.fixture
def registration() -> Iterator[FlowRegistration]:
    """A registration owned by ``OWNER_ID``, plus a schedule on it."""
    with get_db_context() as db:
        reg = FlowRegistration(name="Notify Flow", flow_path="/tmp/notify.flowfile", owner_id=OWNER_ID)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        db.expunge(reg)
    yield reg
    with get_db_context() as db:
        db.query(FlowSchedule).filter(FlowSchedule.registration_id == reg.id).delete()
        db.query(FlowRegistration).filter(FlowRegistration.id == reg.id).delete()
        db.commit()


@pytest.fixture
def schedule(registration: FlowRegistration) -> FlowSchedule:
    with get_db_context() as db:
        sched = FlowSchedule(
            registration_id=registration.id,
            owner_id=OWNER_ID,
            schedule_type="interval",
            interval_seconds=3600,
            name="Nightly",
        )
        db.add(sched)
        db.commit()
        db.refresh(sched)
        db.expunge(sched)
        return sched


def _create_channel(client: TestClient, name: str = "Ops Slack", url: str = PUBLIC_URL, **kwargs) -> dict:
    body = {"name": name, "channel_type": "slack", "webhook_url": url, **kwargs}
    resp = client.post("/notifications/channels", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------- _mask_url ----------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://hooks.slack.com/services/T0/B0/XYZf3ab", "https://hooks.slack.com/…f3ab"),
        ("http://example.com:8080/a/b/cdef", "http://example.com:8080/…cdef"),
        # Userinfo must be dropped, not echoed back.
        ("https://user:pw@hooks.example.com/path/wxyz", "https://hooks.example.com/…wxyz"),
        # No scheme / no host / junk → tail only, never the path.
        ("not-a-url-at-all", "…-all"),
        ("", "…"),
        ("https:///nohost1234", "…1234"),
    ],
)
def test_mask_url(url: str, expected: str) -> None:
    assert _mask_url(url) == expected


def test_mask_url_survives_an_unparseable_port() -> None:
    """``urlparse(...).port`` raises on a non-numeric port; the mask must not."""
    assert _mask_url("https://example.com:notaport/hook/abcd") == "…abcd"


# ---------- channels ----------


def test_create_channel_masks_url_and_stores_it_encrypted(client: TestClient) -> None:
    created = _create_channel(client)

    assert created["webhook_url_preview"] == "https://93.184.216.34/…f3ab"
    assert created["owner_id"] == OWNER_ID
    assert created["enabled"] is True
    # The full URL must not appear anywhere in the response, under any key.
    assert PUBLIC_URL not in client.get("/notifications/channels").text
    assert "webhook_url" not in created

    with get_db_context() as db:
        row = db.get(NotificationChannel, created["id"])
        assert row.webhook_url_encrypted.startswith("$ffsec$1$")
        assert PUBLIC_URL not in row.webhook_url_encrypted


def test_create_channel_rejects_a_private_host(client: TestClient) -> None:
    resp = client.post(
        "/notifications/channels",
        json={"name": "Local", "channel_type": "generic", "webhook_url": "http://127.0.0.1:9000/hook"},
    )
    assert resp.status_code == 422, resp.text
    assert "non-public" in resp.json()["detail"]


def test_create_channel_rejects_a_bad_scheme(client: TestClient) -> None:
    resp = client.post(
        "/notifications/channels",
        json={"name": "FTP", "channel_type": "generic", "webhook_url": "ftp://93.184.216.34/hook"},
    )
    assert resp.status_code == 422, resp.text
    assert "http or https" in resp.json()["detail"]


def test_create_channel_rejects_a_blank_name(client: TestClient) -> None:
    resp = client.post(
        "/notifications/channels",
        json={"name": "   ", "channel_type": "slack", "webhook_url": PUBLIC_URL},
    )
    assert resp.status_code == 422, resp.text


def test_create_channel_generates_the_master_key_on_a_virgin_install(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    """The shared crypto mirror only reads keys, so the first channel on a fresh install
    would 500 without core generating one first."""
    store = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(store))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(store))
    # SecureStorage resolves its path once, at import — rebuild the module singleton.
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    assert core_secrets.get_password("flowfile", "master_key") is None

    created = _create_channel(client)

    assert created["webhook_url_preview"] == "https://93.184.216.34/…f3ab"
    assert core_secrets.get_password("flowfile", "master_key") is not None


def test_update_channel_reencrypts_a_new_url(client: TestClient) -> None:
    created = _create_channel(client)
    with get_db_context() as db:
        before = db.get(NotificationChannel, created["id"]).webhook_url_encrypted

    resp = client.put(f"/notifications/channels/{created['id']}", json={"webhook_url": OTHER_PUBLIC_URL})
    assert resp.status_code == 200, resp.text
    assert resp.json()["webhook_url_preview"] == "https://93.184.216.34/…1c2d"

    with get_db_context() as db:
        after = db.get(NotificationChannel, created["id"]).webhook_url_encrypted
    assert after != before
    assert after.startswith("$ffsec$1$")


def test_update_channel_name_only_leaves_the_url_intact(client: TestClient) -> None:
    created = _create_channel(client)
    with get_db_context() as db:
        before = db.get(NotificationChannel, created["id"]).webhook_url_encrypted

    resp = client.put(f"/notifications/channels/{created['id']}", json={"name": "Renamed"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "Renamed"
    # Same URL still decrypts to the same preview.
    assert body["webhook_url_preview"] == "https://93.184.216.34/…f3ab"

    with get_db_context() as db:
        assert db.get(NotificationChannel, created["id"]).webhook_url_encrypted == before


def test_update_channel_rejects_a_private_url(client: TestClient) -> None:
    created = _create_channel(client)
    resp = client.put(
        f"/notifications/channels/{created['id']}", json={"webhook_url": "http://10.0.0.5/hook"}
    )
    assert resp.status_code == 422, resp.text


def test_channels_are_scoped_to_their_owner(client: TestClient, other_client: TestClient) -> None:
    mine = _create_channel(client, name="Mine")
    theirs = _create_channel(other_client, name="Theirs")

    assert [c["name"] for c in client.get("/notifications/channels").json()] == ["Mine"]
    assert [c["name"] for c in other_client.get("/notifications/channels").json()] == ["Theirs"]

    # Another owner's id is 404, never 403 — a 403 would confirm the row exists.
    assert client.put(f"/notifications/channels/{theirs['id']}", json={"name": "x"}).status_code == 404
    assert client.delete(f"/notifications/channels/{theirs['id']}").status_code == 404
    assert client.post(f"/notifications/channels/{theirs['id']}/test").status_code == 404
    # ...and the foreign row is untouched.
    assert other_client.get("/notifications/channels").json()[0]["name"] == "Theirs"
    assert mine["id"] != theirs["id"]


def test_delete_channel_also_deletes_its_rules(client: TestClient) -> None:
    channel = _create_channel(client)
    rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()

    assert client.delete(f"/notifications/channels/{channel['id']}").status_code == 204
    assert client.get("/notifications/channels").json() == []
    assert client.get("/notifications/rules").json() == []

    with get_db_context() as db:
        assert db.get(NotificationRule, rule["id"]) is None


# ---------- channel tests (delivery) ----------


class _FakeResponse:
    def __init__(self, status_code: int = 200, text: str = "ok") -> None:
        self.status_code = status_code
        self.text = text


def test_channel_test_endpoint_reports_success(client: TestClient, monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    def _fake_post(url: str, json: dict):
        calls.append((url, json))
        return _FakeResponse()

    monkeypatch.setattr(senders, "_post", _fake_post)

    channel = _create_channel(client)
    resp = client.post(f"/notifications/channels/{channel['id']}/test")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"ok": True, "error": None}

    assert len(calls) == 1
    url, body = calls[0]
    # The decrypted URL is what actually gets posted to.
    assert url == PUBLIC_URL
    assert "test notification" in body["text"]


def test_channel_test_endpoint_reports_failure_without_a_500(client: TestClient, monkeypatch) -> None:
    def _boom(url: str, json: dict):
        raise ConnectionError("connection refused by the far end")

    monkeypatch.setattr(senders, "_post", _boom)

    channel = _create_channel(client)
    resp = client.post(f"/notifications/channels/{channel['id']}/test")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is False
    assert "connection refused" in body["error"]


def test_channel_test_endpoint_truncates_a_long_error(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(senders, "_post", lambda url, json: _FakeResponse(500, "x" * 5000))

    channel = _create_channel(client)
    body = client.post(f"/notifications/channels/{channel['id']}/test").json()
    assert body["ok"] is False
    assert len(body["error"]) <= 300


def test_test_url_endpoint_needs_no_saved_channel(client: TestClient, monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(senders, "_post", lambda url, json: calls.append(url) or _FakeResponse())

    resp = client.post(
        "/notifications/channels/test-url",
        json={"channel_type": "discord", "webhook_url": PUBLIC_URL},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["ok"] is True
    assert calls == [PUBLIC_URL]
    # Nothing was persisted by a pre-save test.
    assert client.get("/notifications/channels").json() == []


def test_test_url_endpoint_reports_a_rejected_url(client: TestClient) -> None:
    resp = client.post(
        "/notifications/channels/test-url",
        json={"channel_type": "slack", "webhook_url": "http://localhost/hook"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["ok"] is False
    assert "non-public" in resp.json()["error"]


# ---------- rules ----------


def test_create_rule_for_a_schedule(client: TestClient, schedule: FlowSchedule) -> None:
    channel = _create_channel(client)
    resp = client.post(
        "/notifications/rules",
        json={"channel_id": channel["id"], "schedule_id": schedule.id, "on_success": True},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["schedule_id"] == schedule.id
    assert body["schedule_name"] == "Nightly"
    assert body["registration_id"] is None
    assert body["channel_name"] == "Ops Slack"
    assert body["channel_type"] == "slack"
    assert (body["on_failure"], body["on_success"], body["on_recovery"]) == (True, True, True)


def test_create_rule_for_a_registration(client: TestClient, registration: FlowRegistration) -> None:
    channel = _create_channel(client)
    resp = client.post(
        "/notifications/rules",
        json={"channel_id": channel["id"], "registration_id": registration.id},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["registration_id"] == registration.id
    assert body["flow_name"] == "Notify Flow"
    assert body["schedule_id"] is None


def test_create_global_rule(client: TestClient) -> None:
    channel = _create_channel(client)
    resp = client.post("/notifications/rules", json={"channel_id": channel["id"]})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["registration_id"] is None and body["schedule_id"] is None
    assert body["flow_name"] is None and body["schedule_name"] is None


def test_create_rule_rejects_both_scopes(
    client: TestClient, registration: FlowRegistration, schedule: FlowSchedule
) -> None:
    channel = _create_channel(client)
    resp = client.post(
        "/notifications/rules",
        json={
            "channel_id": channel["id"],
            "registration_id": registration.id,
            "schedule_id": schedule.id,
        },
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "Provide either schedule_id or registration_id, not both"


def test_create_rule_rejects_a_foreign_channel(client: TestClient, other_client: TestClient) -> None:
    theirs = _create_channel(other_client, name="Theirs")
    resp = client.post("/notifications/rules", json={"channel_id": theirs["id"]})
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "Channel not found"


def test_create_rule_rejects_unknown_scope_targets(client: TestClient) -> None:
    channel = _create_channel(client)
    missing_schedule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "schedule_id": 987654}
    )
    assert missing_schedule.status_code == 422
    assert missing_schedule.json()["detail"] == "Schedule not found"

    missing_flow = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "registration_id": 987654}
    )
    assert missing_flow.status_code == 422
    assert missing_flow.json()["detail"] == "Flow not found"


def test_list_rules_filters(
    client: TestClient, registration: FlowRegistration, schedule: FlowSchedule
) -> None:
    channel = _create_channel(client)
    global_rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()
    flow_rule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "registration_id": registration.id}
    ).json()
    schedule_rule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "schedule_id": schedule.id}
    ).json()

    all_ids = [r["id"] for r in client.get("/notifications/rules").json()]
    assert all_ids == [global_rule["id"], flow_rule["id"], schedule_rule["id"]]

    by_schedule = client.get("/notifications/rules", params={"schedule_id": schedule.id}).json()
    assert [r["id"] for r in by_schedule] == [schedule_rule["id"]]

    # The flow filter is flow-level only: the schedule's rule belongs to the schedule's list.
    by_flow = client.get("/notifications/rules", params={"registration_id": registration.id}).json()
    assert [r["id"] for r in by_flow] == [flow_rule["id"]]


def test_rules_are_scoped_to_their_owner(client: TestClient, other_client: TestClient) -> None:
    theirs_channel = _create_channel(other_client, name="Theirs")
    theirs_rule = other_client.post("/notifications/rules", json={"channel_id": theirs_channel["id"]}).json()

    assert client.get("/notifications/rules").json() == []
    assert client.put(f"/notifications/rules/{theirs_rule['id']}", json={"enabled": False}).status_code == 404
    assert client.delete(f"/notifications/rules/{theirs_rule['id']}").status_code == 404
    assert len(other_client.get("/notifications/rules").json()) == 1


def test_update_rule_toggles_and_switches_channel(client: TestClient) -> None:
    first = _create_channel(client, name="First")
    second = _create_channel(client, name="Second", url=OTHER_PUBLIC_URL)
    rule = client.post("/notifications/rules", json={"channel_id": first["id"]}).json()

    resp = client.put(
        f"/notifications/rules/{rule['id']}",
        json={"channel_id": second["id"], "on_failure": False, "on_success": True, "enabled": False},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["channel_id"] == second["id"]
    assert body["channel_name"] == "Second"
    assert body["on_failure"] is False
    assert body["on_success"] is True
    assert body["on_recovery"] is True  # untouched
    assert body["enabled"] is False


def test_update_rule_rejects_a_foreign_channel(client: TestClient, other_client: TestClient) -> None:
    mine = _create_channel(client, name="Mine")
    theirs = _create_channel(other_client, name="Theirs")
    rule = client.post("/notifications/rules", json={"channel_id": mine["id"]}).json()

    resp = client.put(f"/notifications/rules/{rule['id']}", json={"channel_id": theirs["id"]})
    assert resp.status_code == 422
    assert resp.json()["detail"] == "Channel not found"


def test_delete_rule(client: TestClient) -> None:
    channel = _create_channel(client)
    rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()

    assert client.delete(f"/notifications/rules/{rule['id']}").status_code == 204
    assert client.get("/notifications/rules").json() == []
    assert client.delete(f"/notifications/rules/{rule['id']}").status_code == 404


# ---------- history ----------


def _insert_outbox(rule_id: int, channel_id: int, run_id: int | None, **kwargs) -> int:
    with get_db_context() as db:
        row = NotificationOutbox(
            rule_id=rule_id,
            channel_id=channel_id,
            run_id=run_id,
            event_type=kwargs.pop("event_type", "run_failed"),
            payload_json="{}",
            status=kwargs.pop("status", "sent"),
            attempts=kwargs.pop("attempts", 1),
            **kwargs,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return row.id


@pytest.fixture
def flow_run() -> Iterator[FlowRun]:
    from datetime import datetime

    with get_db_context() as db:
        run = FlowRun(
            flow_name="Notify Flow",
            user_id=OWNER_ID,
            started_at=datetime(2026, 1, 1, 12, 0, 0),
            ended_at=datetime(2026, 1, 1, 12, 0, 30),
            success=False,
            run_type="scheduled",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        db.expunge(run)
    yield run
    with get_db_context() as db:
        db.query(FlowRun).filter(FlowRun.id == run.id).delete()
        db.commit()


def test_history_returns_rows_newest_first(client: TestClient, flow_run: FlowRun) -> None:
    channel = _create_channel(client)
    rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()

    older = _insert_outbox(rule["id"], channel["id"], flow_run.id, event_type="run_failed")
    newer = _insert_outbox(
        rule["id"], channel["id"], None, event_type="run_success", status="dead", attempts=5,
        last_error="boom",
    )

    body = client.get("/notifications/history").json()
    assert [item["id"] for item in body] == [newer, older]

    assert body[1]["run_id"] == flow_run.id
    assert body[1]["flow_name"] == "Notify Flow"
    assert body[1]["channel_name"] == "Ops Slack"
    assert body[1]["status"] == "sent"

    assert body[0]["run_id"] is None
    assert body[0]["flow_name"] is None
    assert body[0]["status"] == "dead"
    assert body[0]["attempts"] == 5
    assert body[0]["last_error"] == "boom"


def test_history_is_scoped_to_its_owner(client: TestClient, other_client: TestClient) -> None:
    theirs_channel = _create_channel(other_client, name="Theirs")
    theirs_rule = other_client.post("/notifications/rules", json={"channel_id": theirs_channel["id"]}).json()
    _insert_outbox(theirs_rule["id"], theirs_channel["id"], None)

    assert client.get("/notifications/history").json() == []
    assert len(other_client.get("/notifications/history").json()) == 1


def test_history_survives_a_deleted_rule_via_the_channel(client: TestClient) -> None:
    """Rows outlive their rule; the channel snapshot on the row is the owner path."""
    channel = _create_channel(client)
    rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()
    row_id = _insert_outbox(rule["id"], channel["id"], None)

    with get_db_context() as db:
        db.query(NotificationRule).filter(NotificationRule.id == rule["id"]).delete()
        db.commit()

    body = client.get("/notifications/history").json()
    assert [item["id"] for item in body] == [row_id]
    assert body[0]["channel_name"] == "Ops Slack"


def test_deleting_a_channel_deletes_its_history(client: TestClient) -> None:
    """History ownership resolves through the channel, so rows outliving it would be
    invisible — until a future channel reuses the SQLite rowid and adopts them."""
    channel = _create_channel(client)
    rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()
    _insert_outbox(rule["id"], channel["id"], None)

    assert client.delete(f"/notifications/channels/{channel['id']}").status_code == 204

    assert client.get("/notifications/history").json() == []
    with get_db_context() as db:
        assert db.query(NotificationOutbox).count() == 0


def test_history_limit_is_capped(client: TestClient) -> None:
    channel = _create_channel(client)
    rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()
    for i in range(3):
        _insert_outbox(rule["id"], channel["id"], None, event_type=f"run_failed_{i}")

    assert len(client.get("/notifications/history", params={"limit": 2}).json()) == 2
    assert client.get("/notifications/history", params={"limit": 201}).status_code == 422
    assert client.get("/notifications/history", params={"limit": 0}).status_code == 422


# ---------- referent deletion cleanup ----------
#
# Scoped rules must die with their schedule/flow: SQLite reuses rowids, so a stale
# rule would alert the old owner's webhook about whichever future schedule or flow
# is created with the same id.


def test_deleting_a_schedule_deletes_its_scoped_rules(
    client: TestClient, registration: FlowRegistration, schedule: FlowSchedule
) -> None:
    channel = _create_channel(client)
    schedule_rule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "schedule_id": schedule.id}
    ).json()
    flow_rule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "registration_id": registration.id}
    ).json()
    global_rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()

    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_schedule(schedule.id)

    remaining = [r["id"] for r in client.get("/notifications/rules").json()]
    assert schedule_rule["id"] not in remaining
    assert flow_rule["id"] in remaining
    assert global_rule["id"] in remaining


def test_deleting_a_flow_deletes_its_scoped_rules(
    client: TestClient, registration: FlowRegistration, schedule: FlowSchedule
) -> None:
    channel = _create_channel(client)
    schedule_rule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "schedule_id": schedule.id}
    ).json()
    flow_rule = client.post(
        "/notifications/rules", json={"channel_id": channel["id"], "registration_id": registration.id}
    ).json()
    global_rule = client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()

    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_flow(registration.id)

    remaining = [r["id"] for r in client.get("/notifications/rules").json()]
    assert schedule_rule["id"] not in remaining
    assert flow_rule["id"] not in remaining
    assert global_rule["id"] in remaining


def test_deleting_a_user_deletes_their_notification_config(authed_app: None) -> None:
    """A reused user rowid must not inherit channels (webhook credentials), live
    rules, or another owner's delivery history."""
    with get_db_context() as db:
        victim = User(
            username="notify_doomed_user",
            email="notify_doomed_user@example.com",
            full_name="Doomed",
            hashed_password="x",
            disabled=False,
            is_admin=False,
            must_change_password=False,
        )
        db.add(victim)
        db.commit()
        db.refresh(victim)
        victim_id = victim.id

    victim_client = _client_for(victim_id)
    channel = _create_channel(victim_client, name="Doomed Ops")
    rule = victim_client.post("/notifications/rules", json={"channel_id": channel["id"]}).json()
    _insert_outbox(rule["id"], channel["id"], None)

    main.app.dependency_overrides[get_current_admin_user] = lambda: PydanticUser(
        id=OWNER_ID, username=f"user_{OWNER_ID}", is_admin=True
    )
    try:
        resp = _client_for(OWNER_ID).delete(f"/auth/users/{victim_id}")
        assert resp.status_code == 200, resp.text
    finally:
        main.app.dependency_overrides.pop(get_current_admin_user, None)

    with get_db_context() as db:
        assert db.query(NotificationChannel).filter_by(owner_id=victim_id).count() == 0
        assert db.query(NotificationRule).filter_by(owner_id=victim_id).count() == 0
        assert db.query(NotificationOutbox).count() == 0


# ---------- wiring ----------


def test_every_notification_route_is_mounted_and_authenticated() -> None:
    """The router must be reachable under /notifications and never open."""
    paths = {route.path for route in main.app.routes if getattr(route, "path", "").startswith("/notifications")}
    assert paths == {
        "/notifications/channels",
        "/notifications/channels/test-url",
        "/notifications/channels/{channel_id}",
        "/notifications/channels/{channel_id}/test",
        "/notifications/rules",
        "/notifications/rules/{rule_id}",
        "/notifications/history",
    }

    # No dependency override here: an unauthenticated request must be refused.
    anonymous = TestClient(main.app)
    assert anonymous.get("/notifications/channels").status_code == 401
    assert anonymous.get("/notifications/rules").status_code == 401
    assert anonymous.get("/notifications/history").status_code == 401
    assert anonymous.post("/notifications/channels/test-url", json={}).status_code == 401
