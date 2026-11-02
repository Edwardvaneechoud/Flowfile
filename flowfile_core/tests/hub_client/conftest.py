"""A fake hub behind ``httpx.MockTransport``, and a secure store of the test's own.

The fake answers the routes a desktop uses: ``/hub/info``, core's ``/auth/token`` and ``/auth/refresh`` (form
fields, rotating refresh tokens), ``POST /hub/devices``, which upserts on the account and the device name as the
hub does, ``GET /hub/compute-nodes`` (whatever ``nodes`` holds) and ``POST /hub/node-tokens``, which signs a ``run``
token for an enabled node and refuses as the hub does. Its tokens are real JWTs with an ``exp`` claim, signed with
a key only the fake knows.
"""

import json
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs

import httpx
import jwt
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.hub_client import client, session

_KEY = "a signing key only the fake hub knows"
HUB_URL = "https://hub.example.test"
INFO = {"org_name": "Acme Data", "hub_version": "0.3.0", "core_version": "0.24.0", "vend_mode": "static"}
NODE_TOKEN_KID = "hub-key-1"


def compute_node(node_id: int, name: str, *, enabled: bool = True, worker_url: str | None = None) -> dict:
    """One node as the hub lists it; the worker URL defaults to ``https://<name>-worker.example.test:63579``."""
    return {
        "id": node_id,
        "name": name,
        "kind": "flowfile",
        "core_url": f"https://{name}-core.example.test:63578",
        "worker_url": worker_url or f"https://{name}-worker.example.test:63579",
        "ui_url": None,
        "enabled": enabled,
        "is_default": node_id == 1,
    }


class FakeHub:
    def __init__(self, accounts: dict[str, str] | None = None, access_ttl: int = 900):
        self.accounts = accounts or {"ana": "s3cret"}
        self.access_ttl = access_ttl
        self.info: dict = dict(INFO)
        self.version_refusal: str | None = None
        self.down = False
        self.devices: dict[tuple[str, str], int] = {}
        self.live_refresh: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []
        self.nodes: list[dict] = []
        self.nodes_failure: int | None = None
        self.node_token_ttl = 3600
        self.minted: list[dict] = []
        self._issued = 0

    def http(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def _token(self, user: str, kind: str, ttl: int) -> str:
        self._issued += 1
        claims = {"sub": user, "type": kind, "n": self._issued, "exp": int(time.time()) + ttl}
        return jwt.encode(claims, _KEY, algorithm="HS256")

    def _pair(self, user: str) -> dict:
        refresh = self._token(user, "refresh", 7 * 24 * 3600)
        self.live_refresh[refresh] = user
        return {
            "access_token": self._token(user, "access", self.access_ttl),
            "refresh_token": refresh,
            "token_type": "bearer",
        }

    def _bearer_user(self, request: httpx.Request) -> str | None:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        try:
            claims = jwt.decode(token, _KEY, algorithms=["HS256"])
        except jwt.PyJWTError:
            return None
        return claims["sub"] if claims.get("type") == "access" else None

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if self.down:
            raise httpx.ConnectError("connection refused", request=request)
        path = request.url.path
        if request.method == "GET" and path == "/hub/info":
            return httpx.Response(200, json=self.info)
        if request.method == "POST" and path == "/auth/token":
            form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
            if self.accounts.get(form.get("username")) != form.get("password"):
                return httpx.Response(401, json={"detail": "Incorrect username or password"})
            return httpx.Response(200, json=self._pair(form["username"]))
        if request.method == "POST" and path == "/auth/refresh":
            form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
            user = self.live_refresh.pop(form.get("refresh_token", ""), None)
            if user is None:
                return httpx.Response(401, json={"detail": "Invalid refresh token"})
            return httpx.Response(200, json=self._pair(user))
        if request.method == "POST" and path == "/hub/devices":
            user = self._bearer_user(request)
            if user is None:
                return httpx.Response(401, json={"detail": "Could not validate credentials"})
            if self.version_refusal is not None:
                return httpx.Response(426, json={"detail": self.version_refusal})
            body = httpx.Response(200, content=request.content).json()
            key = (user, body["name"])
            device_id = self.devices.setdefault(key, len(self.devices) + 1)
            return httpx.Response(
                200,
                json={
                    "id": device_id,
                    "name": body["name"],
                    "client_version": body["client_version"],
                    "last_seen_at": "2026-11-01T12:00:00Z",
                },
            )
        if request.method == "GET" and path == "/hub/compute-nodes":
            if self._bearer_user(request) is None:
                return httpx.Response(401, json={"detail": "Could not validate credentials"})
            if self.nodes_failure is not None:
                return httpx.Response(self.nodes_failure, json={"detail": "compute nodes unavailable"})
            return httpx.Response(200, json=self.nodes)
        if request.method == "POST" and path == "/hub/node-tokens":
            return self._mint(request)
        return httpx.Response(404, json={"detail": "Not Found"})

    def _mint(self, request: httpx.Request) -> httpx.Response:
        user = self._bearer_user(request)
        if user is None:
            return httpx.Response(401, json={"detail": "Could not validate credentials"})
        body = json.loads(request.content)
        node = next((n for n in self.nodes if n["id"] == body["node_id"]), None)
        if node is None:
            return httpx.Response(404, json={"detail": "no such compute node"})
        if not node["enabled"]:
            return httpx.Response(403, json={"detail": f"node {node['name']} is disabled"})
        scope = body.get("scope", "run")
        if scope != "run":
            return httpx.Response(403, json={"detail": "dispatch tokens are the hub's own"})
        now = int(time.time())
        claims = {
            "iss": HUB_URL,
            "sub": user,
            "uid": 1,
            "aud": node["name"],
            "scope": scope,
            "jti": uuid.uuid4().hex,
            "iat": now,
            "exp": now + self.node_token_ttl,
        }
        token = jwt.encode(claims, _KEY, algorithm="HS256", headers={"kid": NODE_TOKEN_KID})
        self.minted.append({"user": user, "node_id": node["id"], "scope": scope})
        iso = lambda t: datetime.fromtimestamp(t, timezone.utc).isoformat()  # noqa: E731
        return httpx.Response(
            201,
            json={
                "token": token,
                "node_id": node["id"],
                "node_name": node["name"],
                "scope": scope,
                "kid": NODE_TOKEN_KID,
                "issued_at": iso(now),
                "expires_at": iso(now + self.node_token_ttl),
                "issuer": HUB_URL,
            },
        )


@pytest.fixture
def hub(monkeypatch) -> FakeHub:
    """A fake hub that every client function reaches through the module's http seam."""
    fake = FakeHub()
    monkeypatch.setattr(client, "_module_http", fake.http())
    return fake


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A secure store in the test's tmp dir, and no access token cached from an earlier test."""
    path = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(path))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(path))
    # SecureStorage resolves its path once, at import — rebuild the module singleton.
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    return path
