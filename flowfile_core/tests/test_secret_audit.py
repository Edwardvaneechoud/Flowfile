"""End-to-end coverage for the secret-access audit trail.

The audit table is written to as a side effect of the CRUD endpoints; these
tests treat that side effect as the contract. Each test exercises one route,
then queries ``GET /secrets/secrets/audit`` to confirm the expected row
appears with the right status.
"""

import os
import sys
import uuid

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import SecretAccessEvent


def _get_client() -> TestClient:
    """Authenticated test client. Mirrors ``test_endpoints.get_test_client``."""
    with TestClient(main.app) as bootstrap:
        token = bootstrap.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


def _audit_rows(action: str | None = None, secret_name: str | None = None):
    """Direct DB query so the test doesn't rely on the audit endpoint itself."""
    with get_db_context() as db:
        q = db.query(SecretAccessEvent)
        if action is not None:
            q = q.filter(SecretAccessEvent.action == action)
        if secret_name is not None:
            q = q.filter(SecretAccessEvent.secret_name == secret_name)
        rows = q.order_by(SecretAccessEvent.id.desc()).all()
        for row in rows:
            db.expunge(row)
        return rows


def test_create_secret_emits_success_audit_row():
    client = _get_client()
    name = f"audit_test_{uuid.uuid4().hex[:8]}"

    try:
        r = client.post("/secrets/secrets", json={"name": name, "value": "v"})
        assert r.status_code == 200, r.text

        rows = _audit_rows(action="create", secret_name=name)
        assert len(rows) == 1
        assert rows[0].result_status == "success"
        assert rows[0].secret_id is not None
        assert rows[0].source == "api"
    finally:
        client.delete(f"/secrets/secrets/{name}")


def test_create_duplicate_secret_emits_error_audit_row():
    client = _get_client()
    name = f"audit_dup_{uuid.uuid4().hex[:8]}"

    try:
        client.post("/secrets/secrets", json={"name": name, "value": "v"})
        r = client.post("/secrets/secrets", json={"name": name, "value": "v"})
        assert r.status_code == 400

        rows = _audit_rows(action="create", secret_name=name)
        # one success row + one error row
        assert len(rows) == 2
        statuses = {row.result_status for row in rows}
        assert statuses == {"success", "error"}
        err_row = next(r for r in rows if r.result_status == "error")
        assert err_row.error == "duplicate_name"
    finally:
        client.delete(f"/secrets/secrets/{name}")


def test_list_secrets_emits_audit_row():
    client = _get_client()
    pre = len(_audit_rows(action="list"))

    r = client.get("/secrets/secrets")
    assert r.status_code == 200

    post = len(_audit_rows(action="list"))
    assert post == pre + 1


def test_read_missing_secret_emits_error_audit_row():
    client = _get_client()
    name = f"audit_missing_{uuid.uuid4().hex[:8]}"

    r = client.get(f"/secrets/secrets/{name}")
    assert r.status_code == 404

    rows = _audit_rows(action="read", secret_name=name)
    assert len(rows) == 1
    assert rows[0].result_status == "error"
    assert rows[0].error == "not_found"


def test_delete_secret_emits_success_audit_row():
    client = _get_client()
    name = f"audit_del_{uuid.uuid4().hex[:8]}"

    client.post("/secrets/secrets", json={"name": name, "value": "v"})
    r = client.delete(f"/secrets/secrets/{name}")
    assert r.status_code == 204

    rows = _audit_rows(action="delete", secret_name=name)
    assert len(rows) == 1
    assert rows[0].result_status == "success"


def test_audit_endpoint_returns_recent_events():
    client = _get_client()
    name = f"audit_ep_{uuid.uuid4().hex[:8]}"

    try:
        client.post("/secrets/secrets", json={"name": name, "value": "v"})

        r = client.get(
            "/secrets/secrets/audit",
            params={"secret_name": name, "limit": 10},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body) >= 1
        assert body[0]["secret_name"] == name
        assert body[0]["action"] == "create"
        assert body[0]["result_status"] == "success"
        assert "created_at" in body[0]
    finally:
        client.delete(f"/secrets/secrets/{name}")


def test_list_endpoint_survives_audit_write_failure(monkeypatch):
    """F3: a failing audit insert must not poison the session and 500 the read."""
    from sqlalchemy.orm import Session

    client = _get_client()

    real_flush = Session.flush

    def failing_flush(self, *args, **kwargs):
        if any(isinstance(obj, SecretAccessEvent) for obj in self.new):
            raise RuntimeError("simulated audit flush failure")
        return real_flush(self, *args, **kwargs)

    monkeypatch.setattr(Session, "flush", failing_flush)

    r = client.get("/secrets/secrets")
    assert r.status_code == 200, r.text


def test_audit_endpoint_filters_by_action():
    client = _get_client()
    name = f"audit_filter_{uuid.uuid4().hex[:8]}"

    try:
        client.post("/secrets/secrets", json={"name": name, "value": "v"})
        client.delete(f"/secrets/secrets/{name}")

        r = client.get(
            "/secrets/secrets/audit",
            params={"secret_name": name, "action": "delete", "limit": 10},
        )
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 1
        assert body[0]["action"] == "delete"
    finally:
        # Best-effort cleanup if the test failed before delete.
        client.delete(f"/secrets/secrets/{name}")
