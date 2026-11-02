"""A fake hub behind ``httpx.MockTransport``, and a secure store of the test's own.

The fake answers the routes a desktop uses: ``/hub/info``, core's ``/auth/token`` and ``/auth/refresh`` (form
fields, rotating refresh tokens), ``POST /hub/devices``, which upserts on the account and the device name as the
hub does, ``GET /hub/compute-nodes`` (whatever ``nodes`` holds) and ``POST /hub/node-tokens``, which signs a ``run``
token for an enabled node and refuses as the hub does. Its tokens are real JWTs with an ``exp`` claim, signed with
a key only the fake knows. ``FakeHubWithCatalog`` adds the hub's catalog namespaces and tables, and the credentials
the hub vends to a run over them.
"""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import jwt
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.hub_client import client, session
from flowfile_core.schemas.catalog_schema import CatalogTableOut, NamespaceOut

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


_STAMP = "2026-11-01T12:00:00Z"
_LOCAL_ONLY = {"source", "hub_id", "hub_namespace_id"}


class FakeHubWithCatalog(FakeHub):
    """The connection fake plus core's catalog routes the hub mounts, and ``POST /hub/vend`` as the hub answers it.

    ``namespaces`` and ``tables`` hold the hub's rows as core 0.24.0 dumps ``NamespaceOut`` and ``CatalogTableOut``;
    ``catalog_calls`` records each catalog request as ``(path, query string)``. ``GET /catalog/tables/{id}`` answers a
    row of ``tables`` (404 for another id) and ``POST /catalog/tables/from-data`` appends one (409 on a name taken in
    the namespace), each body kept in ``registered``. A vend checks ``grants`` (``"use"`` or ``"manage"`` per
    namespace id) and answers one static credential per root ``storage_uri``, its options ``vend_options`` of that
    root; each body is kept in ``vend_calls``. ``vend_failure`` set answers that status instead.
    """

    def __init__(self):
        super().__init__()
        self.namespaces: list[dict] = []
        self.tables: list[dict] = []
        self.catalog_calls: list[tuple[str, str]] = []
        self.grants: dict[int, str] = {}
        self.vend_options: dict[str, dict] = {}
        self.vend_calls: list[dict] = []
        self.registered: list[dict] = []
        self.vend_failure: int | None = None
        self._vends = 0

    def seed_catalog(self, name: str, **fields) -> dict:
        """A root namespace (a catalog) on the hub."""
        return self._add_namespace(name, None, 0, fields)

    def seed_schema(self, catalog: dict, name: str, **fields) -> dict:
        """A schema under ``catalog`` on the hub."""
        return self._add_namespace(name, catalog["id"], 1, fields)

    def seed_table(self, namespace: dict, name: str, **fields) -> dict:
        """A table in ``namespace`` on the hub, with the fields the hub fills for a physical Delta table."""
        row = CatalogTableOut(
            id=len(self.tables) + 101,
            name=name,
            namespace_id=namespace["id"],
            namespace_name=namespace["name"],
            owner_id=1,
            created_at=_STAMP,
            updated_at=_STAMP,
            **fields,
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)
        self.tables.append(row)
        return row

    def _add_namespace(self, name: str, parent_id: int | None, level: int, fields: dict) -> dict:
        row = NamespaceOut(
            id=len(self.namespaces) + 1,
            name=name,
            parent_id=parent_id,
            level=level,
            owner_id=1,
            created_at=_STAMP,
            updated_at=_STAMP,
            **fields,
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)
        self.namespaces.append(row)
        return row

    def _root(self, namespace_id: int) -> dict | None:
        namespace = next((ns for ns in self.namespaces if ns["id"] == namespace_id), None)
        while namespace is not None and namespace["parent_id"] is not None:
            namespace = next(ns for ns in self.namespaces if ns["id"] == namespace["parent_id"])
        return namespace

    def _vend(self, body: dict) -> httpx.Response:
        self.vend_calls.append(body)
        if self.vend_failure is not None:
            return httpx.Response(self.vend_failure, json={"detail": "vending is not configured on this hub"})
        for ns in body["read_namespace_ids"]:
            if self.grants.get(ns) not in ("use", "manage"):
                return httpx.Response(403, json={"detail": f"no use grant on namespace {ns}"})
        for ns in body["write_namespace_ids"]:
            if self.grants.get(ns) != "manage":
                return httpx.Response(403, json={"detail": f"no manage grant on namespace {ns}"})
        by_base: dict[str, dict] = {}
        for ns, write in [(n, False) for n in body["read_namespace_ids"]] + [
            (n, True) for n in body["write_namespace_ids"]
        ]:
            root = self._root(ns)
            if root is None or not root.get("storage_uri"):
                return httpx.Response(422, json={"detail": f"namespace {ns} lives on local disk and cannot be vended"})
            base = root["storage_uri"].rstrip("/")
            grant = by_base.setdefault(
                base, {"options": self.vend_options.get(root["storage_uri"], {}), "write": False}
            )
            grant["write"] = grant["write"] or write
        if not by_base:
            return httpx.Response(422, json={"detail": "nothing to vend"})
        self._vends += 1
        expires = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
        credentials = [
            {
                "base_uri": base,
                "storage_options": grant["options"],
                "expires_at": expires,
                "mode": "static",
                "read_prefixes": [base],
                "write_prefixes": [base] if grant["write"] else [],
            }
            for base, grant in by_base.items()
        ]
        return httpx.Response(200, json={"vend_id": self._vends, "credentials": credentials})

    def _register(self, body: dict) -> httpx.Response:
        self.registered.append(body)
        if any(t["name"] == body["name"] and t["namespace_id"] == body["namespace_id"] for t in self.tables):
            return httpx.Response(409, json={"detail": f"Catalog table '{body['name']}' already exists"})
        namespace = next(ns for ns in self.namespaces if ns["id"] == body["namespace_id"])
        row = self.seed_table(
            namespace,
            body["name"],
            file_path=body["table_path"],
            is_remote_storage=True,
            schema_columns=body.get("schema_columns") or [],
            row_count=body.get("row_count"),
            column_count=body.get("column_count"),
            size_bytes=body.get("size_bytes"),
        )
        return httpx.Response(201, json=row)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        catalog_read = request.method == "GET" and (
            path in ("/catalog/namespaces", "/catalog/tables") or path.startswith("/catalog/tables/")
        )
        catalog_write = request.method == "POST" and path in ("/hub/vend", "/catalog/tables/from-data")
        if not (catalog_read or catalog_write):
            return super().handle(request)
        self.calls.append((request.method, path))
        self.catalog_calls.append((path, request.url.query.decode()))
        if self.down:
            raise httpx.ConnectError("connection refused", request=request)
        if self._bearer_user(request) is None:
            return httpx.Response(401, json={"detail": "Could not validate credentials"})
        if path == "/hub/vend":
            return self._vend(json.loads(request.content))
        if path == "/catalog/tables/from-data":
            return self._register(json.loads(request.content))
        if path.startswith("/catalog/tables/"):
            table_id = int(path.rsplit("/", 1)[1])
            table = next((t for t in self.tables if t["id"] == table_id), None)
            if table is None:
                return httpx.Response(404, json={"detail": "Catalog table not found"})
            return httpx.Response(200, json=table)
        if path == "/catalog/namespaces":
            parent = request.url.params.get("parent_id")
            parent_id = None if parent is None else int(parent)
            return httpx.Response(200, json=[ns for ns in self.namespaces if ns["parent_id"] == parent_id])
        namespace = request.url.params.get("namespace_id")
        return httpx.Response(
            200, json=[t for t in self.tables if namespace is None or t["namespace_id"] == int(namespace)]
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
