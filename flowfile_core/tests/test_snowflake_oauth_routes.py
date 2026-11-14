"""The Snowflake browser sign-in and discover routes, against a real local HTTP server standing in for Snowflake.

Snowflake's token endpoint is a ``ThreadingHTTPServer`` on a free port (``snowflake_auth._base_url`` points there);
the browser's return is a real GET to the sign-in's own loopback listener, at the ``redirect_uri`` of the authorize
URL. ``SnowflakeDialect.discover`` is replaced because no account is available to the suite.
"""

import base64
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_jwt_secret
from flowfile_core.configs.settings import ALGORITHM
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager import snowflake_signin
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_database_connection,
)
from flowfile_core.routes import snowflake as snowflake_routes
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared import snowflake_auth
from shared.db_dialects import SnowflakeDialect

ACCOUNT = "myorg-myaccount"
HOST = "myorg-myaccount.snowflakecomputing.com"
NAME = "sf_oauth_conn"


def _client() -> TestClient:
    with TestClient(main.app) as auth_client:
        token = auth_client.post("/auth/token").json()["access_token"]
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


client = _client()


class _TokenStub:
    """A real HTTP server standing in for Snowflake's ``/oauth/token-request``."""

    def __init__(self):
        self.requests: list[dict] = []
        self.status = 200
        self.body: dict | str = {
            "access_token": "acc-1",
            "refresh_token": "ref-1",
            "expires_in": 600,
            "username": "ME@EXAMPLE.COM",
        }
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                form = {k: v[0] for k, v in parse_qs(self.rfile.read(length).decode()).items()}
                stub.requests.append({"path": self.path, "headers": dict(self.headers), "form": form})
                payload = stub.body if isinstance(stub.body, str) else json.dumps(stub.body)
                self.send_response(stub.status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(payload.encode())

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub(monkeypatch):
    server = _TokenStub()
    monkeypatch.setattr(snowflake_auth, "_base_url", lambda host: server.base_url)
    monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: True)
    snowflake_auth.forget_access_tokens()
    snowflake_signin.forget_all()
    _cleanup()
    yield server
    _cleanup()
    snowflake_signin.forget_all()
    snowflake_auth.forget_access_tokens()
    server.close()


@pytest.fixture
def discovered(monkeypatch):
    """Replace ``SnowflakeDialect.discover``; records the URIs it was given."""
    uris: list[str] = []

    def discover(self, uri):
        uris.append(uri)
        return {
            "user": "ME@EXAMPLE.COM",
            "role": "ANALYST",
            "warehouse": "COMPUTE_WH",
            "database": "ANALYTICS",
            "warehouses": ["COMPUTE_WH", "BIG_WH"],
            "databases": ["ANALYTICS", "RAW"],
            "roles": ["ANALYST", "PUBLIC"],
        }

    monkeypatch.setattr(SnowflakeDialect, "discover", discover)
    return uris


def _cleanup():
    with get_db_context() as db:
        if get_database_connection(db, NAME, 1) is not None:
            delete_database_connection(db, NAME, 1)


def _start(role: str | None = "ANALYST", account: str = ACCOUNT) -> tuple[str, dict]:
    response = client.post("/snowflake/oauth/start", json={"account": account, "role": role})
    assert response.status_code == 200, response.text
    body = response.json()
    query = {k: v[0] for k, v in parse_qs(urlsplit(body["auth_url"]).query).items()}
    return body["flow_id"], query


def _callback(query: dict, path: str = "/", **params) -> httpx.Response:
    """What the browser does when Snowflake redirects it: a GET to the sign-in's own listener."""
    return httpx.get(query["redirect_uri"] + path, params=params, timeout=10)


def _refused(query: dict, within: float = 3.0) -> bool:
    """Whether the sign-in's listener stops accepting connections within ``within`` seconds."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            httpx.get(query["redirect_uri"] + "/favicon.ico", timeout=1)
        except httpx.ConnectError:
            return True
        time.sleep(0.05)
    return False


def _signed_in(role: str | None = "ANALYST") -> str:
    flow_id, query = _start(role)
    response = _callback(query, code="the-code", state=query["state"])
    assert response.status_code == 200, response.text
    assert client.get(f"/snowflake/oauth/status/{flow_id}").json()["status"] == "ok"
    return flow_id


def _save(method: str = "post", **overrides):
    body = {
        "connection_name": NAME,
        "database_type": "snowflake",
        "username": "",
        "password": "",
        "host": ACCOUNT,
        "database": "ANALYTICS",
        "auth_method": "oauth",
        "role": "ANALYST",
        "warehouse": "COMPUTE_WH",
    } | overrides
    return getattr(client, method)("/db_connection_lib", json=body)


def _stored() -> tuple[str, str]:
    with get_db_context() as db:
        row = get_database_connection(db, NAME, 1)
        secret = db.query(Secret).filter(Secret.id == row.password_id).one()
        return row.username, decrypt_secret(secret.encrypted_value).get_secret_value()


def test_start_opens_a_loopback_listener_for_the_redirect(stub):
    flow_id, query = _start()
    assert query["client_id"] == "LOCAL_APPLICATION"
    assert query["response_type"] == "code"
    redirect = urlsplit(query["redirect_uri"])
    assert (redirect.scheme, redirect.hostname, redirect.path, redirect.query) == ("http", "127.0.0.1", "", "")
    assert redirect.port and query["redirect_uri"] == f"http://127.0.0.1:{redirect.port}"
    assert snowflake_signin.get(flow_id).redirect_uri == query["redirect_uri"]
    assert query["scope"] == "refresh_token session:role:ANALYST"
    assert query["code_challenge_method"] == "S256"
    flow = snowflake_signin.get(flow_id)
    expected = base64.urlsafe_b64encode(hashlib.sha256(flow.verifier.encode()).digest()).rstrip(b"=").decode()
    assert query["code_challenge"] == expected
    assert flow.verifier not in json.dumps(query), "the verifier never enters a URL"
    state = jwt.decode(query["state"], get_jwt_secret(), algorithms=[ALGORITHM])
    assert state["type"] == "snowflake_oauth_state" and state["flow_id"] == flow_id
    assert (flow.host, flow.role, flow.user_id) == (HOST, "ANALYST", 1)
    assert client.get(f"/snowflake/oauth/status/{flow_id}").json() == {
        "status": "pending",
        "message": None,
        "account_host": HOST,
        "username": None,
        "role": "ANALYST",
        "warehouse": None,
        "database": None,
        "warehouses": [],
        "databases": [],
    }
    other_id, other = _start()
    assert other["redirect_uri"] != query["redirect_uri"], "each sign-in has a listener of its own"


def test_core_has_no_callback_route():
    from fastapi.routing import iter_route_contexts

    paths = {route.path for route in iter_route_contexts(main.app.routes)}
    assert "/snowflake/oauth/callback" not in paths
    assert "/snowflake/oauth/start" in paths


def test_start_refuses_a_bad_account(stub):
    response = client.post("/snowflake/oauth/start", json={"account": "https://evil.example.com/x"})
    assert response.status_code == 422
    assert "is not a Snowflake account" in response.json()["detail"]
    assert snowflake_signin._closers == {}, "no listener is opened for a refused start"


def test_a_failed_bind_is_a_422(stub, monkeypatch):
    from flowfile_core.flowfile.database_connection_manager import snowflake_callback

    def refuse(*args, **kwargs):
        raise OSError("no ports")

    monkeypatch.setattr(snowflake_callback._ListenerServer, "__init__", refuse)
    response = client.post("/snowflake/oauth/start", json={"account": ACCOUNT})
    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Flowfile could not open a local port for the Snowflake sign-in (OSError). Try again."
    )


def test_sign_in_then_save_stores_the_refresh_token_once(stub, discovered):
    flow_id, query = _start()
    response = _callback(query, code="the-code", state=query["state"])
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Signed in to Snowflake. You can close this tab and return to Flowfile." in response.text
    assert _refused(query), "the listener closes once it has handled the callback"

    (request,) = stub.requests
    assert request["path"] == "/oauth/token-request"
    assert request["form"] == {
        "grant_type": "authorization_code",
        "code": "the-code",
        "redirect_uri": query["redirect_uri"],
        "code_verifier": snowflake_signin.get(flow_id).verifier,
    }
    (uri,) = discovered
    assert uri.startswith(f"snowflake://ME%40EXAMPLE.COM:ref-1@{HOST}:443?")
    assert "auth=oauth" in uri and "role=ANALYST" in uri

    status = client.get(f"/snowflake/oauth/status/{flow_id}").json()
    assert status == {
        "status": "ok",
        "message": None,
        "account_host": HOST,
        "username": "ME@EXAMPLE.COM",
        "role": "ANALYST",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "warehouses": ["COMPUTE_WH", "BIG_WH"],
        "databases": ["ANALYTICS", "RAW"],
    }
    assert "ref-1" not in json.dumps(status) and "acc-1" not in json.dumps(status)

    response = _save(oauth_flow_id=flow_id)
    assert response.status_code == 200, response.text
    assert _stored() == ("ME@EXAMPLE.COM", "ref-1")
    with get_db_context() as db:
        row = get_database_connection(db, NAME, 1)
        assert (row.host, row.auth_method, row.role, row.port) == (HOST, "oauth", "ANALYST", None)

    _cleanup()
    again = _save(oauth_flow_id=flow_id)
    assert again.status_code == 422
    assert again.json()["detail"] == "This Snowflake sign-in has expired. Sign in again."
    assert client.get(f"/snowflake/oauth/status/{flow_id}").status_code == 404


def test_a_stray_request_does_not_consume_the_sign_in(stub, discovered):
    flow_id, query = _start()
    for path, params in (("/favicon.ico", {}), ("/", {}), ("/", {"code": "c"}), ("/", {"state": query["state"]})):
        assert _callback(query, path, **params).status_code == 404
    assert client.get(f"/snowflake/oauth/status/{flow_id}").json()["status"] == "pending"
    assert stub.requests == []
    assert _callback(query, code="the-code", state=query["state"]).status_code == 200
    assert client.get(f"/snowflake/oauth/status/{flow_id}").json()["status"] == "ok"


def test_a_sign_in_for_another_account_or_role_is_not_claimed(stub, discovered):
    flow_id = _signed_in()
    for overrides in ({"role": "SYSADMIN"}, {"host": "otherorg-other"}):
        response = _save(oauth_flow_id=flow_id, **overrides)
        assert response.status_code == 422
        assert "account or role changed" in response.json()["detail"]
    assert _save(oauth_flow_id=flow_id).status_code == 200, "a refused claim leaves the sign-in claimable"


def test_keeping_and_replacing_a_saved_sign_in(stub, discovered):
    assert _save(oauth_flow_id=_signed_in()).status_code == 200

    kept = _save("put", warehouse="BIG_WH")
    assert kept.status_code == 200, kept.text
    assert _stored()[1] == "ref-1"

    for overrides in ({"role": "SYSADMIN"}, {"host": "otherorg-other"}):
        response = _save("put", **overrides)
        assert response.status_code == 422
        assert response.json()["detail"] == (
            "The account or role changed, so the saved sign-in no longer applies. Sign in with Snowflake again."
        )

    stub.body = {"access_token": "acc-2", "refresh_token": "ref-2", "expires_in": 600, "username": "ME@EXAMPLE.COM"}
    replaced = _save("put", role="SYSADMIN", oauth_flow_id=_signed_in(role="SYSADMIN"))
    assert replaced.status_code == 200, replaced.text
    assert _stored()[1] == "ref-2"


def test_another_users_sign_in_is_unknown(stub, discovered):
    other = snowflake_signin.start(user_id=987654, host=HOST, role=None, verifier="v", redirect_uri="r")
    assert client.get(f"/snowflake/oauth/status/{other.flow_id}").status_code == 404
    snowflake_signin.finish(other.flow_id, status="ok", refresh_token="theirs")
    response = _save(oauth_flow_id=other.flow_id, role=None)
    assert response.status_code == 422
    assert snowflake_signin.get(other.flow_id) is not None, "a refused claim takes nothing"


def _state(**claims) -> str:
    payload = {"type": "snowflake_oauth_state", "exp": int(time.time()) + 60} | claims
    return jwt.encode(payload, get_jwt_secret(), algorithm=ALGORITHM)


def test_a_bad_state_is_refused_with_a_page_and_keeps_the_listener(stub, discovered):
    flow_id, query = _start()
    other_id, _ = _start()
    tampered = query["state"][:-2] + ("AA" if not query["state"].endswith("AA") else "BB")
    cases = {
        "tampered": tampered,
        "expired": _state(flow_id=flow_id, exp=int(time.time()) - 5),
        "wrong type": _state(flow_id=flow_id, type="ga_oauth_state"),
        "no flow": _state(),
        "another sign-in": _state(flow_id=other_id),
    }
    for label, state in cases.items():
        response = _callback(query, code="c", state=state)
        assert response.status_code == 400, label
        assert response.headers["content-type"].startswith("text/html")
        assert "Snowflake sign-in failed" in response.text
    assert stub.requests == [], "no code was exchanged"
    for flow in (flow_id, other_id):
        assert client.get(f"/snowflake/oauth/status/{flow}").json()["status"] == "pending"
    assert _callback(query, code="the-code", state=query["state"]).status_code == 200


def test_a_denied_sign_in_is_an_error_and_is_not_overwritten(stub, discovered):
    from flowfile_core.flowfile.database_connection_manager.snowflake_callback import complete_sign_in

    flow_id, query = _start()
    response = _callback(query, error="access_denied", state=query["state"])
    assert response.status_code == 200
    assert "The sign-in was cancelled in Snowflake." in response.text
    status = client.get(f"/snowflake/oauth/status/{flow_id}").json()
    assert status["status"] == "error"
    assert status["message"] == "The sign-in was cancelled in Snowflake. Start again in Flowfile."
    assert _refused(query)

    late = complete_sign_in("late-code", query["state"], None)
    assert late.ok is False and late.finished is True
    assert stub.requests == []
    assert client.get(f"/snowflake/oauth/status/{flow_id}").json()["status"] == "error"


def test_the_callback_escapes_what_it_echoes(stub):
    flow_id, query = _start()
    response = _callback(query, error="<script>alert(1)</script>", state=query["state"])
    assert "<script>alert(1)</script>" not in response.text
    assert "&lt;script&gt;" in response.text


def test_a_finished_sign_in_is_not_overwritten_by_a_second_callback(stub, discovered):
    from flowfile_core.flowfile.database_connection_manager.snowflake_callback import complete_sign_in

    flow_id, query = _start()
    _callback(query, code="the-code", state=query["state"])
    assert _refused(query), "a second callback cannot even reach the closed listener"
    stub.body = {"error": "invalid_grant", "message": "Code already used"}
    second = complete_sign_in("the-code", query["state"], None)
    assert (second.ok, second.status_code, second.finished) == (True, 200, True)
    assert len(stub.requests) == 1
    assert client.get(f"/snowflake/oauth/status/{flow_id}").json()["status"] == "ok"


def test_no_refresh_token_is_an_error(stub, discovered):
    stub.body = {"access_token": "acc-1", "expires_in": 600, "username": "ME"}
    flow_id, query = _start()
    response = _callback(query, code="c", state=query["state"])
    assert "does not issue refresh tokens" in response.text
    status = client.get(f"/snowflake/oauth/status/{flow_id}").json()
    assert status["status"] == "error"
    assert status["message"] == (
        "This Snowflake account does not issue refresh tokens for the built-in sign-in, so Flowfile cannot keep the "
        "connection. Use an access token or a key-pair."
    )
    assert discovered == []
    assert _save(oauth_flow_id=flow_id).status_code == 422


def test_invalid_client_is_reported_in_plain_words(stub):
    stub.status = 400
    stub.body = {"error": "invalid_client", "message": "Client not found"}
    flow_id, query = _start()
    response = _callback(query, code="c", state=query["state"])
    assert response.status_code == 200
    status = client.get(f"/snowflake/oauth/status/{flow_id}").json()
    assert status["status"] == "error"
    assert "does not offer the built-in sign-in" in status["message"]
    assert "Use an access token or a key-pair" in status["message"]


def test_a_discover_failure_does_not_fail_the_sign_in(stub, monkeypatch):
    def discover(self, uri):
        raise ValueError("No warehouse is selected.")

    monkeypatch.setattr(SnowflakeDialect, "discover", discover)
    flow_id, query = _start()
    _callback(query, code="c", state=query["state"])
    status = client.get(f"/snowflake/oauth/status/{flow_id}").json()
    assert status["status"] == "ok"
    assert status["username"] == "ME@EXAMPLE.COM"
    assert status["warehouses"] == [] and status["databases"] == []
    assert "No warehouse is selected." in status["message"]
    assert _save(oauth_flow_id=flow_id).status_code == 200


def test_an_expired_sign_in_closes_its_listener(stub, discovered, monkeypatch):
    flow_id, query = _start()
    assert _callback(query, "/favicon.ico").status_code == 404, "listening before expiry"
    real_now = snowflake_signin._now
    monkeypatch.setattr(snowflake_signin, "_now", lambda: real_now() + snowflake_signin.TTL_SECONDS + 1)
    assert _refused(query), "the listener notices the expiry on its own"
    assert client.get(f"/snowflake/oauth/status/{flow_id}").status_code == 404
    assert stub.requests == []


def test_forget_all_and_claim_close_listeners(stub, discovered):
    _, first = _start()
    _, second = _start()
    snowflake_signin.forget_all()
    assert _refused(first) and _refused(second)
    assert snowflake_signin._closers == {}

    flow_id, query = _start()
    claimed = _save(oauth_flow_id=flow_id)
    assert claimed.status_code == 422, "not signed in yet"
    assert _callback(query, "/favicon.ico").status_code == 404, "a refused claim leaves the listener open"
    snowflake_signin.forget(flow_id)
    assert _refused(query)


def test_browser_sign_in_is_refused_on_a_multi_user_server(monkeypatch):
    user = SimpleNamespace(id=1)
    body = snowflake_routes.SnowflakeOAuthStartRequest(account=ACCOUNT)
    for mode in ("docker", "server"):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        with pytest.raises(HTTPException) as caught:
            snowflake_routes.oauth_start(body, current_user=user)
        assert caught.value.status_code == 404
        assert snowflake_signin._closers == {}, "no listener is opened on a multi-user server"

        from flowfile_core.routes.routes import DatabaseConnectionInput, _snowflake_save_inputs

        monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: True)
        with pytest.raises(HTTPException) as caught:
            _snowflake_save_inputs(
                DatabaseConnectionInput(
                    connection_name=NAME, database_type="snowflake", username="", password="", auth_method="oauth"
                )
            )
        assert caught.value.status_code == 422
    monkeypatch.setenv("FLOWFILE_MODE", "package")
    monkeypatch.setenv("JWT_SECRET_KEY", "package-mode-test-secret")
    try:
        assert snowflake_routes.oauth_start(body, current_user=user)["flow_id"]
    finally:
        snowflake_signin.forget_all()


# ---------- POST /snowflake/discover ----------


def _discover(**overrides):
    body = {"account": ACCOUNT, "auth_method": "access_token", "username": "ME@EXAMPLE.COM", "secret": "pat-1"}
    return client.post("/snowflake/discover", json=body | overrides)


def test_discover_with_an_access_token(stub, discovered):
    response = _discover(role="ANALYST")
    assert response.status_code == 200, response.text
    assert response.json() == {
        "account_host": HOST,
        "username": "ME@EXAMPLE.COM",
        "role": "ANALYST",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "warehouses": ["COMPUTE_WH", "BIG_WH"],
        "databases": ["ANALYTICS", "RAW"],
        "roles": ["ANALYST", "PUBLIC"],
    }
    assert discovered == [f"snowflake://ME%40EXAMPLE.COM:pat-1@{HOST}:443?auth=access_token&role=ANALYST"]


def test_discover_with_an_encrypted_key(stub, discovered):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.BestAvailableEncryption(b"pw")
    ).decode()
    response = _discover(auth_method="key_pair", secret=pem, private_key_passphrase="pw")
    assert response.status_code == 200, response.text
    assert "auth=key_pair" in discovered[0]
    wrong = _discover(auth_method="key_pair", secret=pem, private_key_passphrase="nope")
    assert wrong.status_code == 422
    assert wrong.json()["detail"] == "The passphrase does not open this private key."


def test_discover_refusals(stub, discovered, monkeypatch):
    assert _discover(account="https://evil.example.com").status_code == 422
    blank = _discover(secret="")
    assert blank.status_code == 422 and blank.json()["detail"] == "Paste a Snowflake access token."
    assert _discover(auth_method="oauth").status_code == 422

    from shared.db_dialects.snowflake import SnowflakeError

    def failing(self, uri):
        raise SnowflakeError("Snowflake did not accept the username or the credential on this connection.")

    monkeypatch.setattr(SnowflakeDialect, "discover", failing)
    response = _discover()
    assert response.status_code == 422
    assert response.json()["detail"].startswith("Snowflake did not accept the username")

    monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: False)
    response = _discover()
    assert response.status_code == 422 and "flowfile[snowflake]" in response.json()["detail"]


def test_discover_with_a_stored_secret_uses_the_stored_account(stub, discovered):
    saved = _save(auth_method="access_token", username="ME@EXAMPLE.COM", password="stored-pat", role="ANALYST")
    assert saved.status_code == 200, saved.text
    response = _discover(account="attacker-account", secret="", connection_name=NAME, role="PUBLIC", username="")
    assert response.status_code == 200, response.text
    assert response.json()["account_host"] == HOST
    assert discovered == [f"snowflake://ME%40EXAMPLE.COM:stored-pat@{HOST}:443?auth=access_token&role=PUBLIC"]

    unknown = _discover(secret="", connection_name="no_such_connection")
    assert unknown.status_code == 422
