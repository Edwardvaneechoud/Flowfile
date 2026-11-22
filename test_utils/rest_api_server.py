"""A real HTTP server for the REST API reader tests that records every request it receives."""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

SECURE_TOKEN = "e2e-secret-token"

KEYSET_RECORDS = [{"_id": f"id{i:02d}", "value": i} for i in range(1, 8)]


@dataclass
class RecordedRequest:
    method: str
    path: str
    query: dict[str, str]
    headers: dict[str, str]
    body: Any
    raw_body: bytes


@dataclass
class Reply:
    status: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    text: str | None = None


Route = Callable[[RecordedRequest, "ApiServer"], Reply]


def _keyset_page(request: RecordedRequest, *, echo_last: bool) -> Reply:
    """The records after ``data_id`` in the JSON body; ``echo_last`` repeats the last record past the end."""
    body = request.body or {}
    limit = int(body.get("limit", 3))
    after = body.get("data_id")
    ids = [r["_id"] for r in KEYSET_RECORDS]
    start = ids.index(after) + 1 if after in ids else 0
    page = KEYSET_RECORDS[start : start + limit]
    if not page and echo_last:
        page = KEYSET_RECORDS[-1:]
    return Reply(payload={"data": page})


def _cursor(request: RecordedRequest, _server: ApiServer) -> Reply:
    nxt = {None: ("c2", 1), "c2": ("c3", 2), "c3": (None, 3)}
    next_cursor, rid = nxt.get(request.query.get("cursor"), (None, 99))
    return Reply(payload={"rows": [{"id": rid}], "next": next_cursor})


def _offset(request: RecordedRequest, _server: ApiServer) -> Reply:
    rows = [{"id": i} for i in range(5)]
    offset = int(request.query.get("offset", "0"))
    limit = int(request.query.get("limit", "100"))
    return Reply(payload=rows[offset : offset + limit])


def _page(request: RecordedRequest, _server: ApiServer) -> Reply:
    page = int(request.query.get("page", "1"))
    return Reply(payload=[{"id": page}] if page <= 3 else [])


def _secure(request: RecordedRequest, server: ApiServer) -> Reply:
    server.seen_authorization = request.headers.get("Authorization")
    if server.seen_authorization == f"Bearer {SECURE_TOKEN}":
        return Reply(payload=[{"id": 1, "name": "secret"}])
    return Reply(401, {"error": "unauthorized"})


def _flaky(_request: RecordedRequest, server: ApiServer) -> Reply:
    server.calls["flaky"] = server.calls.get("flaky", 0) + 1
    if server.calls["flaky"] == 1:
        return Reply(429, {"error": "slow down"}, {"Retry-After": "0"})
    return Reply(payload=[{"id": 1}])


def _fail_once(_request: RecordedRequest, server: ApiServer) -> Reply:
    server.calls["fail-once"] = server.calls.get("fail-once", 0) + 1
    if server.calls["fail-once"] == 1:
        return Reply(503, {"error": "unavailable"})
    return Reply(payload=[{"id": 1}])


DEFAULT_ROUTES: dict[str, Route] = {
    "/items": lambda _r, _s: Reply(payload=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]),
    "/wrapped": lambda _r, _s: Reply(
        payload={"data": {"items": [{"id": 1, "nested": {"city": "ams"}}, {"id": 2, "nested": {"city": "rtm"}}]}}
    ),
    "/offset": _offset,
    "/page": _page,
    "/cursor": _cursor,
    "/secure": _secure,
    "/flaky": _flaky,
    "/fail-once": _fail_once,
    "/keyset": lambda r, _s: _keyset_page(r, echo_last=False),
    "/keyset-echo": lambda r, _s: _keyset_page(r, echo_last=True),
}


class ApiServer:
    def __init__(self, httpd: ThreadingHTTPServer):
        host, port = httpd.server_address[:2]
        self.base_url = f"http://{host}:{port}"
        self.routes: dict[str, Route] = dict(DEFAULT_ROUTES)
        self.requests: list[RecordedRequest] = []
        self.calls: dict[str, int] = {}
        self.seen_authorization: str | None = None
        self._lock = threading.Lock()

    @property
    def request_count(self) -> int:
        return len(self.requests)

    def url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def route(self, path: str, handler: Route) -> str:
        self.routes[path] = handler
        return self.url(path)

    def requests_to(self, path: str) -> list[RecordedRequest]:
        return [r for r in self.requests if r.path == path]

    def reset(self) -> None:
        with self._lock:
            self.requests.clear()
            self.calls.clear()
            self.seen_authorization = None

    def _record(self, request: RecordedRequest) -> None:
        with self._lock:
            self.requests.append(request)


def _handler_for(server: ApiServer) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def _serve(self) -> None:
            parts = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = None
            request = RecordedRequest(
                method=self.command,
                path=parts.path,
                query={k: v[0] for k, v in parse_qs(parts.query).items()},
                headers=dict(self.headers.items()),
                body=body,
                raw_body=raw,
            )
            server._record(request)
            handler = server.routes.get(parts.path)
            reply = handler(request, server) if handler else Reply(404, {"error": "not found"})
            if reply.text is not None:
                data, content_type = reply.text.encode(), "text/plain"
            else:
                data, content_type = json.dumps(reply.payload).encode(), "application/json"
            self.send_response(reply.status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            for k, v in reply.headers.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        do_GET = _serve  # noqa: N815 - BaseHTTPRequestHandler API
        do_POST = _serve  # noqa: N815

    return _Handler


@contextlib.contextmanager
def start_api_server() -> Iterator[ApiServer]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    server = ApiServer(httpd)
    httpd.RequestHandlerClass = _handler_for(server)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def closed_port_url() -> str:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}/"
