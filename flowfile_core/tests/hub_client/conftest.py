"""A fake hub behind ``httpx.MockTransport``, and a secure store of the test's own.

The fake answers the routes a desktop uses: ``/hub/info``, core's ``/auth/token`` and ``/auth/refresh`` (form
fields, rotating refresh tokens), ``POST /hub/devices``, which upserts on the account and the device name as the
hub does, ``GET /hub/compute-nodes`` (whatever ``nodes`` holds) and ``POST /hub/node-tokens``, which signs a ``run``
token for an enabled node and refuses as the hub does; and the one a team node asks, ``GET /hub/exchange-store``
(``exchange_store``, a 404 while it is ``None``). Its tokens are real JWTs with an ``exp`` claim, signed with a key
only the fake knows. ``FakeHubWithCatalog`` adds the hub's catalog namespaces and tables, the credentials the
hub vends to a run over them and the run reports it receives; ``SealingHubCatalog`` seals what it vends to the nodes a
run names. ``serve_fake`` serves a fake over real HTTP on a
loopback port, for tests in which nothing may stand between the client and the wire.
"""

import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote

import httpx
import jwt
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client import secrets as hub_secrets
from flowfile_core.schemas.catalog_schema import (
    ActiveFlowRun,
    CatalogTableOut,
    FlowRegistrationOut,
    FlowRunOut,
    FlowScheduleOut,
    NamespaceOut,
    NamespaceTree,
)

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
        self.exchange_store: dict | None = None
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
        if request.method == "GET" and path == "/hub/exchange-store":
            if not request.headers.get("Authorization", "").startswith("Bearer "):
                return httpx.Response(401, json={"detail": "Not authenticated"})
            if self.exchange_store is None:
                return httpx.Response(404, json={"detail": "no exchange store is set"})
            return httpx.Response(200, json=self.exchange_store)
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
NEVER_RUNS = "The hub never runs, schedules or computes; run this on a compute node."
CANCEL_REFUSED = "The server that runs this flow could not be told to stop it."
_LOCAL_ONLY = {"source", "hub_id", "hub_namespace_id"}


class FakeHubWithCatalog(FakeHub):
    """The connection fake plus core's catalog routes the hub mounts, and ``POST /hub/vend`` as the hub answers it.

    ``namespaces`` and ``tables`` hold the hub's rows as core 0.24.0 dumps ``NamespaceOut`` and ``CatalogTableOut``;
    ``catalog_calls`` records each catalog request as ``(path, query string)``. ``GET /catalog/tables/{id}`` answers a
    row of ``tables`` (404 for another id), ``GET /catalog/tables/resolve`` the row whose ``qualified_name`` is ``q``
    (404 for none) and ``POST /catalog/tables/from-data`` appends one (409 on a name taken in the namespace), each
    body kept in ``registered``. ``GET /hub/flows`` answers ``flows``, the published flows as
    the hub lists them (``seed_flow``; ``flows_failure`` set answers that status instead). A vend checks ``grants``
    (``"use"`` or ``"manage"`` per namespace id) and answers one static credential per root ``storage_uri``, its options ``vend_options`` of that
    root; each body is kept in ``vend_calls``. ``vend_failure`` set answers that status instead. With ``says_sentences``
    a refused grant is answered as a hub that says what to do does (team-spaces TS10), naming the schema; otherwise
    as core's bare reason. ``POST /hub/runs``
    keeps each run report in ``reports`` and answers 201; ``GET /catalog/scheduler/status`` answers
    ``scheduler_status``, the hub's own scheduler.
    """

    def __init__(self):
        super().__init__()
        self.namespaces: list[dict] = []
        self.tables: list[dict] = []
        self.flows: list[dict] = []
        self.flows_failure: int | None = None
        self.catalog_calls: list[tuple[str, str]] = []
        self.grants: dict[int, str] = {}
        self.vend_options: dict[str, dict] = {}
        self.vend_calls: list[dict] = []
        self.registered: list[dict] = []
        self.refreshed: list[tuple[int, dict]] = []
        self.reports: list[dict] = []
        self.vend_failure: int | None = None
        self.says_sentences = False
        self._vends = 0
        self.tree_answer: list | None = None
        self.runs: list[dict] = []
        self.schedules: list[dict] = []
        self.scheduler_status: dict = {"active": True, "holder_id": "hub", "heartbeat_at": _STAMP}
        self.cursors: dict[int, dict[str, dict]] = {}

    def seed_catalog(self, name: str, **fields) -> dict:
        """A root namespace (a catalog) on the hub."""
        return self._add_namespace(name, None, 0, fields)

    def seed_schema(self, catalog: dict, name: str, **fields) -> dict:
        """A schema under ``catalog`` on the hub."""
        return self._add_namespace(name, catalog["id"], 1, fields)

    def seed_table(self, namespace: dict, name: str, **fields) -> dict:
        """A table in ``namespace`` on the hub, with the fields the hub fills for a physical Delta table, its
        ``full_table_name`` (``schema.table``) and ``qualified_name`` (``catalog.schema.table``) as core names them."""
        row = CatalogTableOut(
            id=len(self.tables) + 101,
            name=name,
            namespace_id=namespace["id"],
            namespace_name=namespace["name"],
            full_table_name=f"{namespace['name']}.{name}",
            qualified_name=f"{self._full_name(namespace['id'])}.{name}",
            owner_id=1,
            created_at=_STAMP,
            updated_at=_STAMP,
            **fields,
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)
        self.tables.append(row)
        return row

    def seed_flow(
        self,
        namespace: dict,
        name: str,
        *,
        latest_version: int | None = 1,
        author_name: str | None = "bob",
        owner_id: int = 1,
        flow_uuid: str | None = None,
    ) -> dict:
        """A flow registered in ``namespace`` on the hub, as ``GET /hub/flows`` lists it; ``latest_version`` ``None``
        is one registered but never published."""
        row = {
            "registration_id": len(self.flows) + 201,
            "flow_uuid": flow_uuid or str(uuid.uuid5(uuid.NAMESPACE_URL, f"{namespace['id']}/{name}")),
            "name": name,
            "namespace_id": namespace["id"],
            "owner_id": owner_id,
            "latest_version": latest_version,
            "author_name": author_name if latest_version is not None else None,
            "published_at": _STAMP if latest_version is not None else None,
        }
        self.flows.append(row)
        return row

    def seed_schedule(self, flow: dict, server: str = "bmax", **fields) -> dict:
        """A schedule of ``flow`` as the hub's mount of core's schedule routes dumps its ``flow_schedules`` row
        (``FlowScheduleOut``): the flow by its registration id, the server as ``backend`` ``worker:<name>``."""
        row = FlowScheduleOut(
            **{
                "id": len(self.schedules) + 7,
                "registration_id": flow["registration_id"],
                "owner_id": flow["owner_id"],
                "enabled": True,
                "schedule_type": "interval",
                "interval_seconds": 3600,
                "backend": f"worker:{server}",
                "created_at": _STAMP,
                "updated_at": _STAMP,
                **fields,
            }
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)
        self.schedules.append(row)
        return row

    def seed_run(self, flow: dict, *, ended: bool = True, **fields) -> dict:
        """A run of ``flow`` as the hub records one it sent or was reported (``FlowRunOut``); ``ended`` False is one
        still running, which ``GET /catalog/active-runs`` lists."""
        row = FlowRunOut(
            **{
                "id": len(self.runs) + 5,
                "registration_id": flow["registration_id"],
                "flow_uuid": flow["flow_uuid"],
                "flow_name": flow["name"],
                "flow_path": f"hub://flows/{flow['flow_uuid']}",
                "user_id": flow["owner_id"],
                "started_at": _STAMP,
                "ended_at": _STAMP if ended else None,
                "success": True if ended else None,
                "run_type": "scheduled",
                **fields,
            }
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)
        self.runs.append(row)
        return row

    def _runs_page(self, params) -> dict:
        """``GET /catalog/runs`` as core's service pages it: filtered by ``registration_id`` and ``schedule_id``,
        cut by ``offset`` and ``limit``, with the counts of the filtered runs."""
        runs = [
            run
            for run in self.runs
            if all(params.get(key) in (None, str(run.get(key))) for key in ("registration_id", "schedule_id"))
        ]
        offset, limit = int(params.get("offset", 0)), int(params.get("limit", 50))
        ended = [run for run in runs if run.get("ended_at")]
        return {
            "items": runs[offset : offset + limit],
            "total": len(runs),
            "total_success": len([run for run in ended if run.get("success")]),
            "total_failed": len([run for run in ended if not run.get("success")]),
            "total_running": len(runs) - len(ended),
        }

    def _active_runs(self) -> list[dict]:
        return [
            ActiveFlowRun(**{key: run[key] for key in ActiveFlowRun.model_fields if key in run}).model_dump(
                mode="json", exclude=_LOCAL_ONLY
            )
            for run in self.runs
            if not run.get("ended_at")
        ]

    def _add_namespace(self, name: str, parent_id: int | None, level: int, fields: dict) -> dict:
        row = NamespaceOut(
            id=len(self.namespaces) + 1,
            name=name,
            parent_id=parent_id,
            level=level,
            created_at=_STAMP,
            updated_at=_STAMP,
            **{"owner_id": 1, **fields},
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)
        self.namespaces.append(row)
        return row

    @staticmethod
    def flow_row(f: dict) -> dict:
        """A flow as the hub's catalog service lists it (uuid, newest version and its author), in the tree and on
        ``GET /catalog/flows``."""
        return FlowRegistrationOut(
            id=f["registration_id"],
            name=f["name"],
            flow_path=f"hub://flows/{f['flow_uuid']}",
            namespace_id=f["namespace_id"],
            owner_id=f["owner_id"],
            created_at=_STAMP,
            updated_at=_STAMP,
            flow_uuid=f["flow_uuid"],
            latest_version=f["latest_version"],
            author_name=f["author_name"],
        ).model_dump(mode="json", exclude=_LOCAL_ONLY)

    def tree(self) -> list[dict]:
        """What ``GET /catalog/namespaces/tree`` answers: core's ``NamespaceTree`` of ``namespaces``, each with its
        ``tables`` and its ``flows`` (``flow_row``)."""

        def node(namespace: dict) -> dict:
            flows = [self.flow_row(f) for f in self.flows if f["namespace_id"] == namespace["id"]]
            row = NamespaceTree(**namespace).model_dump(mode="json", exclude=_LOCAL_ONLY)
            row["children"] = [node(ns) for ns in self.namespaces if ns["parent_id"] == namespace["id"]]
            row["tables"] = [t for t in self.tables if t["namespace_id"] == namespace["id"]]
            row["flows"] = flows
            return row

        return [node(ns) for ns in self.namespaces if ns["parent_id"] is None]

    def _catalog_row(self, method: str, path: str, body, params=None) -> httpx.Response:
        """Core's own routes over one row, as the hub's catalog answers them: a table read, renamed or deleted, a
        table favourite, the run history of every flow or of one registration or schedule, and the runs under way."""
        params = params or {}
        if path == "/catalog/runs":
            return httpx.Response(200, json=self._runs_page(params))
        if path == "/catalog/active-runs":
            return httpx.Response(200, json=self._active_runs())
        parts = path.split("/")
        if parts[2] == "runs" and parts[4:] == ["cancel"]:
            return self._cancel(parts[3])
        if parts[2] == "schedules":
            return self._schedule_row(method, parts[3:], body, params.get("registration_id"))
        if parts[2] == "flows":
            flow = next((f for f in self.flows if str(f["registration_id"]) == parts[3]), None)
            if flow is None:
                return httpx.Response(404, json={"detail": "Flow not found"})
            if parts[4:] == ["run"]:
                return httpx.Response(403, json={"detail": NEVER_RUNS})
            if parts[4:] == ["favorite"] and method == "DELETE":
                return httpx.Response(204)
            if parts[4:] == ["favorite"]:
                row = {"id": 3, "user_id": 1, "registration_id": flow["registration_id"], "created_at": _STAMP}
                return httpx.Response(201, json=row)
            if not parts[4:] and method == "DELETE":
                self.flows.remove(flow)
                return httpx.Response(204)
            if not parts[4:] and method == "PUT":
                flow.update({k: v for k, v in body.items() if v is not None})
            return httpx.Response(200, json={"id": flow["registration_id"], **flow})
        table = next((t for t in self.tables if str(t["id"]) == parts[3]), None) if parts[2] == "tables" else None
        if table is None:
            return httpx.Response(404, json={"detail": "Catalog table not found"})
        if parts[4:] == ["favorite"]:
            return httpx.Response(201, json={"id": 9, "user_id": 1, "table_id": table["id"], "created_at": _STAMP})
        if parts[4:5] == ["cdc"]:
            return self._cursor_route(method, table, parts[5:], body)
        if method == "DELETE":
            self.tables.remove(table)
            return httpx.Response(204)
        if method == "PUT":
            table.update(body)
        if parts[4:] == ["refresh"]:
            self.refreshed.append((table["id"], body))
            facts = [body.get(name) for name in ("schema_columns", "row_count", "size_bytes")]
            if body.get("table_path") is not None or None in facts:
                return httpx.Response(403, json={"detail": "The hub opens, writes and removes no file on its disk"})
            floor = body.get("cdc_enabled_version")
            table.update(
                {name: value for name, value in body.items() if value is not None and name != "cdc_enabled_version"}
            )
            if floor is not None:
                table.update(cdc_enabled=True, cdc_enabled_version=table.get("cdc_enabled_version") or floor)
        return httpx.Response(200, json=table)

    def _cursor_route(self, method: str, table: dict, rest: list[str], body) -> httpx.Response:
        """Core's change-cursor routes over ``cursors`` as the hub serves them: the status lists the table's cursors
        and reads no head (the hub reads no storage), a commit upserts one, a reset moves one to a version and a
        delete forgets one; a missing cursor is core's 404."""
        kept = self.cursors.setdefault(table["id"], {})
        if not rest:
            listed = [{**c, "pending_commits": None} for _, c in sorted(kept.items())]
            status = {"cdc_enabled_version": table.get("cdc_enabled_version"), "current_version": None}
            return httpx.Response(200, json={"cdc_enabled": bool(table.get("cdc_enabled")), **status, "cursors": listed})
        key = unquote(rest[1]) if method == "DELETE" else body["consumer_key"]
        if rest == ["cursors"]:
            fields = {k: v for k, v in body.items() if v is not None}
            kept[key] = {**kept.get(key, {"owner_id": 1, "last_commit_timestamp": None}), **fields}
            kept[key]["updated_at"] = _STAMP
            return httpx.Response(200, json=kept[key])
        if key not in kept:
            return httpx.Response(404, json={"detail": f"No change cursor '{key}' on table id={table['id']}"})
        if method == "DELETE":
            del kept[key]
            return httpx.Response(204)
        kept[key]["last_version"] = int(body["to"])
        return httpx.Response(200, json=kept[key])

    def _cancel(self, run_id: str) -> httpx.Response:
        """``POST /catalog/runs/{id}/cancel`` as the hub answers a run it sent: 204 for one under way, the hub's
        sentence (422) for one already ended, 404 for no such run."""
        run = next((r for r in self.runs if str(r["id"]) == run_id), None)
        if run is None:
            return httpx.Response(404, json={"detail": "Run not found"})
        if run["ended_at"] is not None:
            return httpx.Response(422, json={"detail": CANCEL_REFUSED})
        run["ended_at"] = _STAMP
        return httpx.Response(204)

    def _schedule_row(self, method: str, rest: list[str], body, registration_id: str | None = None) -> httpx.Response:
        """Core's schedule routes as the hub answers them from its own schedules: every flow's listed, or those of
        ``registration_id``, paused or resumed by ``enabled`` and removed; one is never made
        (``POST /catalog/schedules``) or run now here."""
        if method == "POST":
            return httpx.Response(403, json={"detail": NEVER_RUNS})
        if not rest:
            listed = [s for s in self.schedules if registration_id in (None, str(s["registration_id"]))]
            return httpx.Response(200, json=listed)
        schedule = next((s for s in self.schedules if str(s["id"]) == rest[0]), None)
        if schedule is None:
            return httpx.Response(404, json={"detail": "Schedule not found"})
        if method == "DELETE":
            self.schedules.remove(schedule)
            return httpx.Response(204)
        if method == "PUT":
            schedule.update(body)
        return httpx.Response(200, json=schedule)

    def _root(self, namespace_id: int) -> dict | None:
        namespace = next((ns for ns in self.namespaces if ns["id"] == namespace_id), None)
        while namespace is not None and namespace["parent_id"] is not None:
            namespace = next(ns for ns in self.namespaces if ns["id"] == namespace["parent_id"])
        return namespace

    def _full_name(self, namespace_id: int) -> str:
        namespace = next(ns for ns in self.namespaces if ns["id"] == namespace_id)
        root = self._root(namespace_id)
        return namespace["name"] if root is namespace else f"{root['name']}.{namespace['name']}"

    def _vend(self, body: dict) -> httpx.Response:
        self.vend_calls.append(body)
        if self.vend_failure is not None:
            return httpx.Response(self.vend_failure, json={"detail": "vending is not configured on this hub"})
        for ns in body["read_namespace_ids"]:
            if self.grants.get(ns) not in ("use", "manage"):
                if self.says_sentences:
                    sentence = f"You cannot read {self._full_name(ns)}; ask an admin or your team's manager to share it"
                    return httpx.Response(403, json={"detail": sentence})
                return httpx.Response(403, json={"detail": f"no use grant on namespace {ns}"})
        for ns in body["write_namespace_ids"]:
            if self.grants.get(ns) != "manage":
                if self.says_sentences:
                    sentence = (
                        f"You can read {self._full_name(ns)} but not write to it; "
                        "ask an admin or your team's manager for write access"
                    )
                    return httpx.Response(403, json={"detail": sentence})
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
        catalog_read = path.startswith("/catalog/") or (request.method == "GET" and path == "/hub/flows")
        catalog_write = request.method == "POST" and path in ("/hub/vend", "/catalog/tables/from-data", "/hub/runs")
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
        if path == "/hub/runs":
            self.reports.append(json.loads(request.content))
            return httpx.Response(201, json={"run_id": self.reports[-1].get("run_id")})
        if path == "/catalog/tables/from-data":
            return self._register(json.loads(request.content))
        if path == "/catalog/tables/resolve":
            reference = request.url.params.get("q")
            table = next((t for t in self.tables if t["qualified_name"] == reference), None)
            if table is None:
                return httpx.Response(404, json={"detail": "Catalog table not found"})
            return httpx.Response(200, json={"table": table, "warnings": []})
        if path == "/catalog/scheduler/status":
            return httpx.Response(200, json=self.scheduler_status)
        if path == "/catalog/namespaces/tree":
            return httpx.Response(200, json=self.tree() if self.tree_answer is None else self.tree_answer)
        if path == "/catalog/flows":
            return httpx.Response(200, json=[self.flow_row(f) for f in self.flows])
        rows = ("/catalog/tables/", "/catalog/flows/", "/catalog/schedules")
        cancel = path.startswith("/catalog/runs/") and path.endswith("/cancel")
        if path.startswith(rows) or cancel or path in ("/catalog/runs", "/catalog/active-runs"):
            body = json.loads(request.content) if request.content else None
            return self._catalog_row(request.method, path, body, request.url.params)
        if path == "/catalog/namespaces":
            parent = request.url.params.get("parent_id")
            parent_id = None if parent is None else int(parent)
            return httpx.Response(200, json=[ns for ns in self.namespaces if ns["parent_id"] == parent_id])
        namespace = request.url.params.get("namespace_id")
        if path == "/hub/flows":
            if self.flows_failure is not None:
                return httpx.Response(self.flows_failure, json={"detail": "Not Found"})
            return httpx.Response(
                200, json=[f for f in self.flows if namespace is None or f["namespace_id"] == int(namespace)]
            )
        return httpx.Response(
            200, json=[t for t in self.tables if namespace is None or t["namespace_id"] == int(namespace)]
        )


class SealingHubCatalog(FakeHubWithCatalog):
    """``FakeHubWithCatalog`` whose vend takes ``exchange`` and ``targets`` as the hub's contract has them.

    ``exchange`` adds a credential for ``exchange_store``'s URI, its options ``vend_options`` of that URI (a 422 with
    no store). With ``targets`` each credential's ``storage_options`` is empty and its ``sealed`` maps every target to
    a token ``signer`` signs and seals to that node's ``enc`` key in ``enc_keys`` (a node with none is the hub's 422,
    one in ``refused_nodes`` its 403), with ``issuer`` as ``iss``; without targets ``sealed`` is empty. Storage tokens
    carry ``kind: "storage"``. ``secrets`` names (TS18) are answered from ``secret_values``, each sealed to every target
    with ``kind: "secret"`` (``[]`` when none is asked); names without targets are the hub's 422 and a name it does not
    hold its 403 sentence, worded as TS18's hub words them. ``connections`` (``kind`` and ``name``) are answered from
    ``stored_connections``, each ``(kind, name)`` to its settings and the names of its secret fields, sealed to every
    target as the hub's vend seals them. The hub lists ``secret_values`` at ``GET /secrets/secrets`` and
    ``stored_connections`` at each kind's list route, no value and no secret, as the hub mounts core's routes.
    """

    LIST_PATHS = {
        "/cloud_connections/cloud_connections": "cloud",
        "/kafka/connections": "kafka",
        "/db_connection_lib": "database",
    }

    def __init__(self, signer, issuer: str):
        super().__init__()
        self.signer = signer
        self.issuer = issuer
        self.enc_keys: dict[str, dict] = {}
        self.refused_nodes: set[str] = set()
        self.secret_values: dict[str, str] = {}
        self.stored_connections: dict[tuple[str, str], tuple[dict, tuple[str, ...]]] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method != "GET" or (path != "/secrets/secrets" and path not in self.LIST_PATHS):
            return super().handle(request)
        self.calls.append((request.method, path))
        if self._bearer_user(request) is None:
            return httpx.Response(401, json={"detail": "Could not validate credentials"})
        if path == "/secrets/secrets":
            rows = [{"id": i, "name": name, "value": "**********"} for i, name in enumerate(self.secret_values, 1)]
            return httpx.Response(200, json=rows)
        kind = self.LIST_PATHS[path]
        rows = [
            {"id": i, "connection_name": name, **{k: v for k, v in settings.items() if k not in secret}}
            for i, ((stored_kind, name), (settings, secret)) in enumerate(self.stored_connections.items(), 1)
            if stored_kind == kind
        ]
        return httpx.Response(200, json=rows)

    def _vend(self, body: dict) -> httpx.Response:
        targets = body.get("targets") or []
        names = body.get("secrets") or []
        refs = [(ref["kind"], ref["name"]) for ref in body.get("connections") or []]
        if refs and not targets:
            unsealed = "Connections are vended only sealed to the servers that run them"
            return httpx.Response(422, json={"detail": unsealed})
        for ref in refs:
            if ref not in self.stored_connections:
                return httpx.Response(403, json={"detail": f"You have no connection named {ref[1]} on the hub"})
        if names and not targets:
            return httpx.Response(
                422,
                json={
                    "detail": "Secrets are vended only sealed to the servers that run the step; name them in targets"
                },
            )
        for name in names:
            if name not in self.secret_values:
                sentence = (
                    f"You have no secret named {name} on the hub; add it in Storage and connections → Secrets, "
                    "or in a team's Secrets"
                )
                self.vend_calls.append(body)
                return httpx.Response(403, json={"detail": sentence})
        for name in targets:
            if name in self.refused_nodes:
                return httpx.Response(403, json={"detail": f"You may not run on the server {name}"})
            if name not in self.enc_keys:
                return httpx.Response(422, json={"detail": f"The server {name} has no keys on this hub"})
        if body["read_namespace_ids"] or body["write_namespace_ids"]:
            response = super()._vend(body)
            if response.status_code != 200:
                return response
            answer = response.json()
        else:
            self.vend_calls.append(body)
            self._vends += 1
            answer = {"vend_id": self._vends, "credentials": []}
        if body.get("exchange"):
            if self.exchange_store is None:
                return httpx.Response(422, json={"detail": "The organisation has no exchange store"})
            uri = self.exchange_store["uri"]
            expires = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
            answer["credentials"].append(
                {
                    "base_uri": uri,
                    "storage_options": dict(self.vend_options.get(uri, {})),
                    "expires_at": expires,
                    "mode": "static",
                    "read_prefixes": [uri],
                    "write_prefixes": [uri],
                }
            )
        for credential in answer["credentials"]:
            options = credential["storage_options"]
            credential["sealed"] = {
                name: self.signer.sealed(
                    self.enc_keys[name],
                    node=name,
                    iss=self.issuer,
                    base_uri=credential["base_uri"],
                    storage_options=options,
                    run_id=body["run_id"],
                    vend_id=answer["vend_id"],
                )
                for name in targets
            }
            if targets:
                credential["storage_options"] = {}
        answer["secrets"] = []
        if names:
            answer["secrets"] = [
                {
                    "name": name,
                    "sealed": {
                        node: self.signer.sealed_secret(
                            self.enc_keys[node],
                            node=node,
                            iss=self.issuer,
                            name=name,
                            value=self.secret_values[name],
                            run_id=body["run_id"],
                            vend_id=answer["vend_id"],
                        )
                        for node in targets
                    },
                }
                for name in names
            ]
        answer["connections"] = [
            {
                "kind": kind,
                "name": name,
                "secret_fields": list(self.stored_connections[(kind, name)][1]),
                "sealed": {
                    node: self.signer.sealed_connection(
                        self.enc_keys[node],
                        node=node,
                        iss=self.issuer,
                        connection_kind=kind,
                        name=name,
                        settings=self.stored_connections[(kind, name)][0],
                        run_id=body["run_id"],
                        vend_id=answer["vend_id"],
                    )
                    for node in targets
                },
            }
            for kind, name in dict.fromkeys(refs)
        ]
        return httpx.Response(200, json=answer)


_HOP_HEADERS = {"content-length", "transfer-encoding", "connection", "content-encoding"}


@pytest.fixture
def serve_fake() -> Iterator[Callable[[FakeHub], str]]:
    """``serve_fake(fake)`` serves ``fake`` over HTTP on a free loopback port and answers its base URL.

    Each request is handed to ``fake.handle`` as the ``httpx.Request`` it was on the wire (method, path, query,
    headers, body) and its ``httpx.Response`` is written back, so the hub client and the run report reach the fake
    with their own ``httpx`` clients. A fake that is ``down`` closes the connection without an answer. The servers
    are shut down when the test ends.
    """
    servers: list[ThreadingHTTPServer] = []

    def serve(fake: FakeHub) -> str:
        class Handler(BaseHTTPRequestHandler):
            def _answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                request = httpx.Request(
                    self.command,
                    f"http://{self.headers['Host']}{self.path}",
                    headers=list(self.headers.items()),
                    content=self.rfile.read(length) if length else b"",
                )
                try:
                    response = fake.handle(request)
                except httpx.TransportError:
                    self.close_connection = True
                    return
                body = response.read()
                self.send_response(response.status_code)
                for name, value in response.headers.items():
                    if name.lower() not in _HOP_HEADERS:
                        self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _answer

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield serve
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture
def hub(monkeypatch) -> FakeHub:
    """A fake hub that every client function reaches through the module's http seam."""
    fake = FakeHub()
    monkeypatch.setattr(client, "_module_http", fake.http())
    return fake


def sign_in_through_hub(fake: FakeHub, user_id: int, username: str = "ana", password: str = "s3cret") -> None:
    """Keep a sign-in through ``fake`` as the session of ``user_id``, as a server joined with keys of its own keeps one:
    ``kind: "delegation"``, its access token one the fake issued, cached until it expires, so no refresh is asked."""
    from flowfile_core.hub_client.delegation import Delegation

    url = getattr(fake, "url", HUB_URL)
    tokens = client.sign_in(url, username, password)
    granted = Delegation(
        access_token=tokens["access_token"],
        expires_in=fake.access_ttl,
        refresh_token=tokens["refresh_token"],
        refresh_expires_in=7 * 24 * 3600,
        user={"uid": 1, "username": username, "full_name": None, "node_admin": False},
    )
    session.save_delegation(user_id, url, granted)


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A secure store in the test's tmp dir, and no access token or hub list cached from an earlier test."""
    path = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(path))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(path))
    # SecureStorage resolves its path once, at import — rebuild the module singleton.
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    monkeypatch.setattr(hub_secrets, "_listed", {})
    return path
