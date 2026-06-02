"""Route tests for the GA4 service-account connection endpoints.

Run with:
    pytest flowfile_core/tests/test_ga_connections_routes.py -v
"""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from flowfile_core import main

_SA_KEY = json.dumps(
    {
        "type": "service_account",
        "client_email": "route-bot@proj.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
)


def get_test_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = get_test_client()


def _delete(name: str) -> None:
    client.delete("/ga_connections/ga_connection", params={"connection_name": name})


def test_create_service_account_connection_and_list():
    name = "ga-route-sa"
    _delete(name)

    resp = client.post(
        "/ga_connections/ga_connection/service_account",
        json={
            "connection_name": name,
            "service_account_key": _SA_KEY,
            "description": "route test",
            "default_property_id": "123456789",
        },
    )
    assert resp.status_code == 200, resp.text

    listing = client.get("/ga_connections/ga_connections").json()
    match = next(c for c in listing if c["connection_name"] == name)
    assert match["auth_method"] == "service_account"
    assert match["oauth_user_email"] == "route-bot@proj.iam.gserviceaccount.com"

    _delete(name)


def test_create_service_account_rejects_bad_json():
    name = "ga-route-bad"
    _delete(name)

    resp = client.post(
        "/ga_connections/ga_connection/service_account",
        json={"connection_name": name, "service_account_key": "{not json"},
    )
    assert resp.status_code == 422, resp.text


def test_test_endpoint_service_account_does_not_require_oauth_config():
    """The ``/test`` service-account branch must not call ``_resolve_oauth_config``
    (which 500s when no Google OAuth client is configured). A structurally-invalid
    key returns ``success=False``, not a 500 OAuth-config error."""
    name = "ga-route-test"
    _delete(name)
    client.post(
        "/ga_connections/ga_connection/service_account",
        json={
            "connection_name": name,
            "service_account_key": _SA_KEY,
            "default_property_id": "123456789",
        },
    )

    resp = client.post("/ga_connections/test", json={"connection_name": name})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is False  # the fake key can't authenticate
    assert "oauth" not in body["message"].lower()  # and it's not an OAuth-config error

    _delete(name)


# --- _test_service_account: the property-access (get_metadata) branch ---------
# The fake key above fails at from_service_account_info, so it never reaches the
# get_metadata property check. These tests mock the SDK so creds.refresh()
# succeeds and we can drive the three success/failure messages.


def _patch_sa_sdk(monkeypatch, *, get_metadata_raises: bool):
    import google.analytics.data_v1beta as data_mod
    import google.oauth2.service_account as sa_mod

    class _FakeCreds:
        def refresh(self, _request):  # no-op: pretend the token minted fine
            pass

    class _FakeClient:
        def __init__(self, **_):
            pass

        def get_metadata(self, name):  # noqa: ANN001
            if get_metadata_raises:
                raise RuntimeError("403 PERMISSION_DENIED")
            return object()

    monkeypatch.setattr(
        sa_mod.Credentials,
        "from_service_account_info",
        staticmethod(lambda info, scopes=None: _FakeCreds()),
    )
    monkeypatch.setattr(data_mod, "BetaAnalyticsDataClient", _FakeClient)


def test_test_service_account_property_access_ok(monkeypatch):
    _patch_sa_sdk(monkeypatch, get_metadata_raises=False)
    from flowfile_core.routes.ga_connections import _test_service_account

    resp = _test_service_account(_SA_KEY, "123456789")
    assert resp.success is True
    assert "123456789" in resp.message


def test_test_service_account_no_property_mints_token_only(monkeypatch):
    _patch_sa_sdk(monkeypatch, get_metadata_raises=False)
    from flowfile_core.routes.ga_connections import _test_service_account

    resp = _test_service_account(_SA_KEY, None)
    assert resp.success is True
    assert "default property" in resp.message.lower()


def test_test_service_account_no_viewer_access_fails(monkeypatch):
    _patch_sa_sdk(monkeypatch, get_metadata_raises=True)
    from flowfile_core.routes.ga_connections import _test_service_account

    resp = _test_service_account(_SA_KEY, "123456789")
    assert resp.success is False
    assert "Viewer" in resp.message
    assert "123456789" in resp.message
