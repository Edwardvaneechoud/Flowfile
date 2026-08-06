import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

import pytest

from shared.snowflake_oauth import (
    SnowflakeOAuthError,
    derive_snowflake_endpoints,
    exchange_authorization_code,
    refresh_access_token,
)


class _TokenEndpointHandler(BaseHTTPRequestHandler):
    """Mock token endpoint. The test controls behavior via server.responses (a list of
    (status, payload) tuples popped per request) and records requests in server.requests."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = parse_qs(self.rfile.read(length).decode())
        self.server.requests.append(
            {
                "path": self.path,
                "body": {k: v[0] for k, v in body.items()},
                "authorization": self.headers.get("Authorization"),
            }
        )
        status, payload = self.server.responses.pop(0)
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


@pytest.fixture()
def token_server():
    server = HTTPServer(("127.0.0.1", 0), _TokenEndpointHandler)
    server.requests = []
    server.responses = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _endpoint(server) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}/oauth/token-request"


def test_derive_snowflake_endpoints():
    authorize, token = derive_snowflake_endpoints("myorg-myaccount")
    assert authorize == "https://myorg-myaccount.snowflakecomputing.com/oauth/authorize"
    assert token == "https://myorg-myaccount.snowflakecomputing.com/oauth/token-request"


def test_refresh_access_token_success(token_server):
    token_server.responses.append((200, {"access_token": "at-1", "expires_in": "600", "refresh_token": "rt-2"}))
    result = refresh_access_token(_endpoint(token_server), "client", "secret", "rt-1")
    assert result.access_token == "at-1"
    assert result.expires_in == 600
    assert result.refresh_token == "rt-2"
    request = token_server.requests[0]
    assert request["body"] == {"grant_type": "refresh_token", "refresh_token": "rt-1"}
    expected = base64.b64encode(b"client:secret").decode()
    assert request["authorization"] == f"Basic {expected}"


def test_refresh_without_rotation_returns_none(token_server):
    token_server.responses.append((200, {"access_token": "at-1", "expires_in": 599}))
    result = refresh_access_token(_endpoint(token_server), "client", "secret", "rt-1")
    assert result.refresh_token is None


def test_exchange_authorization_code(token_server):
    token_server.responses.append((200, {"access_token": "at-1", "refresh_token": "rt-1"}))
    result = exchange_authorization_code(
        _endpoint(token_server), "client", "secret", "the-code", "http://localhost:63578/cb"
    )
    assert result.access_token == "at-1"
    assert result.refresh_token == "rt-1"
    assert result.expires_in is None
    assert token_server.requests[0]["body"] == {
        "grant_type": "authorization_code",
        "code": "the-code",
        "redirect_uri": "http://localhost:63578/cb",
    }


def test_invalid_grant_flags_reauthentication(token_server):
    token_server.responses.append((400, {"error": "invalid_grant", "error_description": "expired"}))
    with pytest.raises(SnowflakeOAuthError) as exc_info:
        refresh_access_token(_endpoint(token_server), "client", "secret", "rt-old")
    assert exc_info.value.error == "invalid_grant"
    assert exc_info.value.requires_reauthentication
    assert "expired" in str(exc_info.value)


def test_server_error_is_not_reauthentication(token_server):
    token_server.responses.append((503, b"upstream down"))
    with pytest.raises(SnowflakeOAuthError) as exc_info:
        refresh_access_token(_endpoint(token_server), "client", "secret", "rt-1")
    assert exc_info.value.status_code == 503
    assert not exc_info.value.requires_reauthentication


def test_missing_access_token_raises(token_server):
    token_server.responses.append((200, {"token_type": "bearer"}))
    with pytest.raises(SnowflakeOAuthError, match="missing access_token"):
        refresh_access_token(_endpoint(token_server), "client", "secret", "rt-1")


def test_unreachable_endpoint_raises():
    with pytest.raises(SnowflakeOAuthError, match="Could not reach"):
        refresh_access_token("http://127.0.0.1:1/token", "client", "secret", "rt-1")
