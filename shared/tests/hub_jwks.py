"""A hub that signs tokens for the tests: an EC P-256 key, its JWKS served from a thread, and ``token()``.

Core and the worker verify hub tokens against the keys at ``<FLOWFILE_HUB_URL>/.well-known/jwks.json``. The
``hub_signer`` fixture serves such a document from a free port on this machine, points the verifier at it through the
environment and signs the tokens a test sends, so every test runs the real verifier against real keys with nothing of
PyJWT replaced. Import the fixture into a test module: ``from shared.tests.hub_jwks import hub_signer  # noqa: F401``.
"""

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from shared import hub_tokens

NODE_NAME = "team"
JWKS_PATH = "/.well-known/jwks.json"


class SigningKey:
    """One EC P-256 key with a ``kid``, as the hub publishes it."""

    def __init__(self, kid: str | None = None):
        self.kid = kid or uuid.uuid4().hex[:8]
        self.private = ec.generate_private_key(ec.SECP256R1())

    def jwk(self) -> dict:
        public = jwt.algorithms.ECAlgorithm.to_jwk(self.private.public_key(), as_dict=True)
        return {**public, "kid": self.kid, "use": "sig", "alg": "ES256"}


class HubSigner:
    """Serves a JWKS of ``served`` keys and signs tokens with ``key``; ``rotate()`` replaces both."""

    def __init__(self):
        self.key = SigningKey()
        self.served: list[SigningKey] = [self.key]
        self.fetches = 0
        signer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != JWKS_PATH:
                    self.send_response(404)
                    self.end_headers()
                    return
                signer.fetches += 1
                body = json.dumps({"keys": [key.jwk() for key in signer.served]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def rotate(self) -> SigningKey:
        """Sign with a new key from now on and serve only that one, as a hub that retired the old key does."""
        self.key = SigningKey()
        self.served = [self.key]
        return self.key

    def token(
        self,
        *,
        scope: str = "run",
        sub: str = "ana",
        uid: int | None = 7,
        aud: str = NODE_NAME,
        iss: str | None = None,
        ttl: int = 300,
        key: SigningKey | None = None,
        drop: tuple[str, ...] = (),
        algorithm: str = "ES256",
        **extra,
    ) -> str:
        """A token as the hub signs it; ``drop`` leaves claims out, ``extra`` adds or overrides them."""
        now = int(time.time())
        claims = {
            "iss": iss if iss is not None else self.url,
            "sub": sub,
            "aud": aud,
            "scope": scope,
            "jti": uuid.uuid4().hex,
            "iat": now,
            "exp": now + ttl,
        }
        if uid is not None:
            claims["uid"] = uid
        claims.update(extra)
        for name in drop:
            claims.pop(name, None)
        key = key or self.key
        return jwt.encode(claims, key.private, algorithm=algorithm, headers={"kid": key.kid})

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def hub_signer(monkeypatch):
    """A running ``HubSigner`` that this process verifies against: ``FLOWFILE_HUB_URL`` and ``FLOWFILE_NODE_NAME`` set."""
    signer = HubSigner()
    monkeypatch.setenv("FLOWFILE_HUB_URL", signer.url)
    monkeypatch.setenv("FLOWFILE_NODE_NAME", NODE_NAME)
    monkeypatch.delenv("FLOWFILE_HUB_ISSUER", raising=False)
    # A fresh key client per test: a reused port would otherwise serve the previous test's cached keys.
    monkeypatch.setattr(hub_tokens, "_client", None)
    try:
        yield signer
    finally:
        signer.close()
