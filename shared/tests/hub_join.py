"""A hub that answers joins for the tests: ``POST /hub/nodes/join`` as the hub's contract has it, served from a thread.

``mint()`` hands out an opaque join token (``code=True``: a short code as a hub's Compute page shows one, ``K7QX-M2PD``,
which a node posts as given); the join route checks a body as the hub's model does, the editor's
``ui_url`` included (a 422 in FastAPI's shape), refuses a token it did not mint, an expired one and a used one with
the hub's sentences, and answers the others once. Given a ``HubSigner`` it also serves that signer's keys at
``/.well-known/jwks.json`` and names the signer's URL as the issuer, so one URL is the whole hub for a node that joins
it and then verifies its tokens. ``POST /hub/nodes/heartbeat`` takes a beat from a node that joined with keys and is
enabled, checking its node assertion against the ``sig`` key the join sent, as the hub's contract says: the ``kid``, the
signature, the audience (the issuer and the route's path), at most 60 seconds between ``iat`` and ``exp``, 30 seconds
of leeway, a ``jti`` used once, and the body. ``POST /hub/token`` exchanges a sign-in token the signer signed
(``sign_in_token``) for a delegation to the node whose assertion comes with it, and refreshes one with its rotating
refresh token; ``POST /hub/token/revoke`` ends one. Both take the form fields and answer the OAuth errors of the
hub's contract, after checking the assertion as the heartbeat does, with the route's own path as its audience. Import
the fixture into a test module: ``from shared.tests.hub_join import join_hub  # noqa: F401``.
"""

import json
import re
import secrets
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import jwt
import pytest

from shared.tests.hub_jwks import JWKS_PATH, HubSigner

JOIN_PATH = "/hub/nodes/join"
HEARTBEAT_PATH = "/hub/nodes/heartbeat"
TOKEN_PATH = "/hub/token"
REVOKE_PATH = "/hub/token/revoke"
TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
REFRESH_GRANT = "refresh_token"
JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"
JWT_BEARER = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
ACCESS_SECONDS = 900
DELEGATION_SECONDS = 12 * 3600
ASSERTION_CLAIMS = ["iss", "sub", "aud", "iat", "exp", "jti"]
NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$"
URL_PATTERN = r"^https?://"
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
NOT_VALID = "this join token is not valid"
EXPIRED = "this join token has expired; create a new one on the hub's Compute page"
USED = "this join token has been used; create a new one on the hub's Compute page"


def _validation_errors(body) -> list[dict]:
    """The errors FastAPI's ``NodeJoin`` model would answer for ``body``, ``input`` included as FastAPI sends it."""
    if not isinstance(body, dict):
        return [{"type": "model_attributes_type", "loc": ["body"], "msg": "Input should be a valid dictionary"}]
    errors = []
    fields = (("token", None, True), ("name", NAME_PATTERN, True), ("worker_url", URL_PATTERN, True))
    optional = (("core_url", URL_PATTERN, False), ("ui_url", URL_PATTERN, False))
    for field, pattern, required in (*fields, *optional):
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


def _form(content_type: str | None, raw: bytes) -> dict | None:
    """The form fields of a urlencoded body, each field's first value; ``None`` for any other body."""
    if not (content_type or "").startswith("application/x-www-form-urlencoded"):
        return None
    return {name: values[0] for name, values in parse_qs(raw.decode("utf-8")).items()}


def _oauth(error: str, description: str) -> dict:
    return {"error": error, "error_description": description}


class FakeJoinHub:
    """Answers ``POST /hub/nodes/join`` for the tokens it minted, and the heartbeats and token requests of the nodes
    that joined.

    ``calls`` keeps each request's path and body (a token request's form fields); ``beats`` each heartbeat taken, its
    assertion's verified ``header`` and ``claims`` and its ``body``; ``token_requests`` each token request answered
    200, its ``grant_type`` and its assertion's ``header`` and ``claims``. ``people`` holds what the hub knows of each
    person a sign-in token was made for, by user id, and answers as their ``user``; a test changes it to model a
    person whose name or rights changed. A user id in ``refused_people`` may not use any node now. Setting
    ``refuse_next_refresh`` ends the delegation the next refresh names, as the hub ends one by itself ("Sign out
    everywhere", the node removed): that refresh is ``invalid_grant`` and its token is refused from then on.
    ``sign_out_everywhere`` ends every delegation of one person at once.
    """

    def __init__(self, signer: HubSigner | None = None):
        self.signer = signer
        self.calls: list[tuple[str, dict | None]] = []
        self.beats: list[dict] = []
        self.token_requests: list[dict] = []
        self.people: dict[int, dict] = {}
        self.refused_people: set[int] = set()
        self.refuse_next_refresh = False
        self._delegations: dict[str, dict] = {}
        self._spent_sign_ins: set[str] = set()
        self._grants: dict[str, dict] = {}
        self._node_ids: dict[str, int] = {}
        self._nodes: dict[str, dict] = {}
        self._jtis: set[str] = set()
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
                if self.path in (TOKEN_PATH, REVOKE_PATH):
                    form = _form(self.headers.get("Content-Type"), raw)
                    hub.calls.append((self.path, form))
                    self._answer(*(hub.token(form) if self.path == TOKEN_PATH else hub.revoke(form)))
                    return
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = None
                hub.calls.append((self.path, body))
                if self.path == JOIN_PATH:
                    self._answer(*hub.answer(body))
                elif self.path == HEARTBEAT_PATH:
                    self._answer(*hub.beat(self.headers.get("Authorization"), body))
                else:
                    self._answer(404, {"detail": "Not Found"})

            def _answer(self, status: int, payload: dict | None):
                self.send_response(status)
                if payload is None:
                    self.end_headers()
                    return
                data = json.dumps(payload).encode()
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

    def mint(self, enable_on_join: bool = False, expired: bool = False, code: bool = False) -> str:
        """A join token this hub accepts once, unless it is minted ``expired``; with ``code``, a short code."""
        if code:
            characters = "".join(secrets.choice(CODE_ALPHABET) for _ in range(8))
            token = f"{characters[:4]}-{characters[4:]}"
        else:
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
            self._nodes[body["name"]] = {"keys": body.get("keys"), "enabled": grant["enable_on_join"]}
        return 200, self.joined(body, grant, node_id)

    def joined(self, body: dict, grant: dict, node_id: int) -> dict:
        """The ``NodeJoined`` answer to an accepted join; a test overrides it for a hub that answers otherwise."""
        return {"node_id": node_id, "name": body["name"], "enabled": grant["enable_on_join"], "issuer": self.issuer}

    def set_enabled(self, name: str, enabled: bool) -> None:
        """Enable or disable the joined node ``name``, as an admin does on the hub's Compute page."""
        with self._lock:
            self._nodes[name]["enabled"] = enabled

    def check_assertion(self, assertion: str, path: str) -> tuple[str | None, dict | None]:
        """A node assertion sent to ``path`` checked as the hub's contract says: ``(None, verified)`` when it holds,
        ``verified`` naming the ``node`` and the assertion's ``header`` and ``claims``, else the sentence refusing it.
        """
        try:
            header = jwt.get_unverified_header(assertion)
            issuer = str(jwt.decode(assertion, options={"verify_signature": False}).get("iss", ""))
        except jwt.InvalidTokenError:
            return "this is not a node assertion", None
        name = issuer.removeprefix("node:")
        with self._lock:
            node = self._nodes.get(name) if issuer.startswith("node:") else None
        if node is None or not node["keys"]:
            return "this node is not known to the hub or has no keys", None
        if not node["enabled"]:
            return "this node is disabled on the hub", None
        sig = node["keys"]["sig"]
        if header.get("kid") != sig["kid"]:
            return "this assertion is not signed with the node's key", None
        try:
            claims = jwt.decode(
                assertion,
                jwt.PyJWK(sig),
                algorithms=["ES256"],
                audience=self.issuer + path,
                issuer=issuer,
                leeway=30,
                options={"require": ASSERTION_CLAIMS},
            )
        except jwt.InvalidTokenError as exc:
            return f"this node assertion is refused ({type(exc).__name__})", None
        if claims["sub"] != issuer or claims["exp"] - claims["iat"] > 60:
            return "this node assertion is refused", None
        with self._lock:
            if claims["jti"] in self._jtis:
                return "this node assertion was used already", None
            self._jtis.add(claims["jti"])
        return None, {"node": name, "header": header, "claims": claims}

    def beat(self, authorization: str | None, body) -> tuple[int, dict | None]:
        """The status and body the heartbeat route answers: 204 for a beat taken, else 401 or 422 with a sentence."""
        scheme, _, assertion = (authorization or "").partition(" ")
        if scheme != "Bearer" or not assertion:
            return 401, {"detail": "a heartbeat needs a node assertion"}
        refusal, verified = self.check_assertion(assertion, HEARTBEAT_PATH)
        if refusal is not None:
            return 401, {"detail": refusal}
        header, claims = verified["header"], verified["claims"]
        if not (
            isinstance(body, dict)
            and set(body) == {"version", "process"}
            and isinstance(body["version"], str)
            and body["process"] in ("core", "worker")
        ):
            return 422, {"detail": [{"type": "value_error", "loc": ["body"], "msg": "not a heartbeat"}]}
        with self._lock:
            self.beats.append({"header": header, "claims": claims, "body": body})
        return 204, None

    def sent_beats(self) -> list[dict | None]:
        """The body of every heartbeat sent to this hub, taken or not."""
        return [body for path, body in list(self.calls) if path == HEARTBEAT_PATH]

    def wait_for_beats(self, count: int, timeout: float = 10.0) -> list[dict | None]:
        """``sent_beats()`` once ``count`` heartbeats were sent, or once ``timeout`` seconds have passed."""
        deadline = time.monotonic() + timeout
        while len(self.sent_beats()) < count and time.monotonic() < deadline:
            time.sleep(0.02)
        return self.sent_beats()

    def sign_in_token(
        self,
        node: str,
        *,
        uid: int,
        username: str,
        full_name: str | None = None,
        node_admin: bool = False,
        ttl: int = 120,
        **claims,
    ) -> str:
        """A sign-in token for ``node`` as the hub's Open page hands it to the browser, signed with the signer's key.

        The person is kept in ``people`` as the hub knows them; ``claims`` adds or overrides the token's own claims.
        """
        if self.signer is None:
            raise RuntimeError("a hub without a signer signs no sign-in tokens")
        self.people[uid] = {"uid": uid, "username": username, "full_name": full_name, "node_admin": node_admin}
        sign_in = {"may_act": {"sub": f"node:{node}"}, "name": full_name, "node_admin": node_admin, **claims}
        return self.signer.token(scope="signin", sub=username, uid=uid, aud=node, ttl=ttl, **sign_in)

    def token(self, form: dict | None) -> tuple[int, dict]:
        """The status and body ``POST /hub/token`` answers: a delegation, or the contract's OAuth error."""
        form = form or {}
        grant_type = form.get("grant_type")
        if grant_type not in (TOKEN_EXCHANGE, REFRESH_GRANT):
            return 400, _oauth("unsupported_grant_type", f"the grant type {grant_type!r} is not supported here")
        if form.get("client_assertion_type") != JWT_BEARER or not form.get("client_assertion"):
            return 401, _oauth("invalid_client", "a token request needs a node assertion")
        refusal, verified = self.check_assertion(form["client_assertion"], TOKEN_PATH)
        if refusal is not None:
            return 401, _oauth("invalid_client", refusal)
        if grant_type == TOKEN_EXCHANGE:
            refusal, delegation = self._sign_in(verified["node"], form)
        else:
            refusal, delegation = self._refresh(verified["node"], form.get("refresh_token") or "")
        if refusal is not None:
            return 400, _oauth("invalid_grant", refusal)
        with self._lock:
            self.token_requests.append({"grant_type": grant_type, **verified})
        return 200, self._delegate(delegation)

    def _sign_in(self, node: str, form: dict) -> tuple[str | None, dict | None]:
        if form.get("subject_token_type") != JWT_TOKEN_TYPE:
            return "the subject token must be a JWT", None
        token = form.get("subject_token") or ""
        try:
            kid = jwt.get_unverified_header(token).get("kid")
            key = next((key for key in self.signer.served if key.kid == kid), None) if self.signer else None
            if key is None:
                return "this sign-in token is not signed by this hub", None
            claims = jwt.decode(
                token,
                key.private.public_key(),
                algorithms=["ES256"],
                audience=node,
                issuer=self.issuer,
                leeway=30,
                options={"require": ["exp", "iat", "sub", "uid", "aud", "scope", "jti"]},
            )
        except jwt.InvalidAudienceError:
            return "this sign-in token is for another node", None
        except jwt.InvalidTokenError as exc:
            return f"this sign-in token is refused ({type(exc).__name__})", None
        if claims["scope"] != "signin" or claims.get("may_act") != {"sub": f"node:{node}"}:
            return "this is not a sign-in token for this node", None
        with self._lock:
            if claims["jti"] in self._spent_sign_ins:
                return "this sign-in token has been used", None
            self._spent_sign_ins.add(claims["jti"])
        if claims["uid"] in self.refused_people:
            return "this person may not use this node now", None
        person = {
            "uid": claims["uid"],
            "username": claims["sub"],
            "full_name": claims.get("name"),
            "node_admin": claims.get("node_admin") is True,
        }
        return None, {"node": node, "uid": claims["uid"], "person": person, "signed_in_at": time.time()}

    def _refresh(self, node: str, refresh_token: str) -> tuple[str | None, dict | None]:
        with self._lock:
            delegation = self._delegations.get(refresh_token)
            if delegation is None or delegation["node"] != node:
                return "this refresh token is not known to the hub for this node", None
            del self._delegations[refresh_token]
            if self.refuse_next_refresh:
                self.refuse_next_refresh = False
                return "this sign-in was ended on the hub", None
        if time.time() - delegation["signed_in_at"] >= DELEGATION_SECONDS:
            return "this sign-in has expired", None
        if delegation["uid"] in self.refused_people:
            return "this person may not use this node now", None
        return None, delegation

    def _delegate(self, delegation: dict) -> dict:
        refresh_token = f"hub-refresh-{uuid.uuid4().hex}"
        with self._lock:
            self._delegations[refresh_token] = delegation
        user = dict(self.people.get(delegation["uid"]) or delegation["person"])
        return {
            "access_token": f"hub-access-{uuid.uuid4().hex}",
            "issued_token_type": ACCESS_TOKEN_TYPE,
            "token_type": "Bearer",
            "expires_in": ACCESS_SECONDS,
            "refresh_token": refresh_token,
            "refresh_expires_in": int(delegation["signed_in_at"] + DELEGATION_SECONDS - time.time()),
            "scope": "workspace",
            "user": user,
        }

    def revoke(self, form: dict | None) -> tuple[int, dict]:
        """The status and body ``POST /hub/token/revoke`` answers: ``{}`` whether or not the token was alive."""
        form = form or {}
        if form.get("client_assertion_type") != JWT_BEARER or not form.get("client_assertion"):
            return 401, _oauth("invalid_client", "a revocation needs a node assertion")
        refusal, verified = self.check_assertion(form["client_assertion"], REVOKE_PATH)
        if refusal is not None:
            return 401, _oauth("invalid_client", refusal)
        with self._lock:
            delegation = self._delegations.get(form.get("token") or "")
            if delegation is not None and delegation["node"] == verified["node"]:
                del self._delegations[form["token"]]
        return 200, {}

    def sign_out_everywhere(self, uid: int) -> None:
        """End every delegation of the person ``uid`` on every node, as "Sign out everywhere" on the hub does: each of
        their refresh tokens is refused from then on.
        """
        with self._lock:
            for refresh_token in [token for token, held in self._delegations.items() if held["uid"] == uid]:
                del self._delegations[refresh_token]

    def live_refresh_tokens(self) -> list[str]:
        """The refresh tokens the hub would take now, for any node."""
        with self._lock:
            return list(self._delegations)

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
