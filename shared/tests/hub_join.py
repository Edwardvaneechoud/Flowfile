"""A hub that answers joins for the tests: ``POST /hub/nodes/join`` as the hub's contract has it, served from a thread.

``mint()`` hands out an opaque join token; the join route checks a body as the hub's model does (a 422 in FastAPI's
shape), refuses a token it did not mint, an expired one and a used one with the hub's sentences, and answers the
others once. Given a ``HubSigner`` it also serves that signer's keys at ``/.well-known/jwks.json`` and names the
signer's URL as the issuer, so one URL is the whole hub for a node that joins it and then verifies its tokens. Import
the fixture into a test module: ``from shared.tests.hub_join import join_hub  # noqa: F401``.
"""

import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from shared.tests.hub_jwks import JWKS_PATH, HubSigner

JOIN_PATH = "/hub/nodes/join"
NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$"
URL_PATTERN = r"^https?://"
NOT_VALID = "this join token is not valid"
EXPIRED = "this join token has expired; create a new one on the hub's Compute page"
USED = "this join token has been used; create a new one on the hub's Compute page"


def _validation_errors(body) -> list[dict]:
    """The errors FastAPI's ``NodeJoin`` model would answer for ``body``, ``input`` included as FastAPI sends it."""
    if not isinstance(body, dict):
        return [{"type": "model_attributes_type", "loc": ["body"], "msg": "Input should be a valid dictionary"}]
    errors = []
    fields = (("token", None, True), ("name", NAME_PATTERN, True), ("worker_url", URL_PATTERN, True))
    for field, pattern, required in (*fields, ("core_url", URL_PATTERN, False)):
        loc = ["body", field]
        if field not in body or (not required and body[field] is None):
            if required:
                errors.append({"type": "missing", "loc": loc, "msg": "Field required", "input": body})
            continue
        value = body[field]
        if not isinstance(value, str):
            errors.append({"type": "string_type", "loc": loc, "msg": "Input should be a valid string", "input": value})
        elif pattern is not None and re.search(pattern, value) is None:
            errors.append(
                {
                    "type": "string_pattern_mismatch",
                    "loc": loc,
                    "msg": f"String should match pattern '{pattern}'",
                    "input": value,
                    "ctx": {"pattern": pattern},
                }
            )
    return errors


class FakeJoinHub:
    """Answers ``POST /hub/nodes/join`` for the tokens it minted; ``calls`` keeps each request's path and body."""

    def __init__(self, signer: HubSigner | None = None):
        self.signer = signer
        self.calls: list[tuple[str, dict | None]] = []
        self._grants: dict[str, dict] = {}
        self._node_ids: dict[str, int] = {}
        self._lock = threading.Lock()
        hub = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                hub.calls.append((self.path, None))
                if hub.signer is None or self.path != JWKS_PATH:
                    self._answer(404, {"detail": "Not Found"})
                    return
                self._answer(200, {"keys": [key.jwk() for key in hub.signer.served]})

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length)
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = None
                hub.calls.append((self.path, body))
                if self.path != JOIN_PATH:
                    self._answer(404, {"detail": "Not Found"})
                    return
                self._answer(*hub.answer(body))

            def _answer(self, status: int, payload: dict):
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def issuer(self) -> str:
        """The ``iss`` the hub's tokens carry: the signer's URL when there is one, else this hub's own."""
        return self.signer.url if self.signer is not None else self.url

    def mint(self, enable_on_join: bool = False, expired: bool = False) -> str:
        """A join token this hub accepts once, unless it is minted ``expired``."""
        token = f"join-{uuid.uuid4().hex}"
        with self._lock:
            self._grants[token] = {"enable_on_join": enable_on_join, "expired": expired, "used": False}
        return token

    def answer(self, body) -> tuple[int, dict]:
        """The status and body the join route answers ``body`` with."""
        errors = _validation_errors(body)
        if errors:
            return 422, {"detail": errors}
        with self._lock:
            grant = self._grants.get(body["token"])
            if grant is None:
                return 401, {"detail": NOT_VALID}
            if grant["expired"]:
                return 401, {"detail": EXPIRED}
            if grant["used"]:
                return 401, {"detail": USED}
            grant["used"] = True
            node_id = self._node_ids.setdefault(body["name"], len(self._node_ids) + 1)
        return 200, self.joined(body, grant, node_id)

    def joined(self, body: dict, grant: dict, node_id: int) -> dict:
        """The ``NodeJoined`` answer to an accepted join; a test overrides it for a hub that answers otherwise."""
        return {"node_id": node_id, "name": body["name"], "enabled": grant["enable_on_join"], "issuer": self.issuer}

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def join_hub():
    """A running ``FakeJoinHub`` without a signer, shut down after the test."""
    hub = FakeJoinHub()
    try:
        yield hub
    finally:
        hub.close()
