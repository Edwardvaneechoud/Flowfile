"""A hub that signs tokens for the tests: an EC P-256 key, its JWKS served from a thread, and ``token()``.

Core and the worker verify hub tokens against the keys at ``<FLOWFILE_HUB_URL>/.well-known/jwks.json``. The
``hub_signer`` fixture serves such a document from a free port on this machine, points the verifier at it through the
environment and signs the tokens a test sends, so every test runs the real verifier against real keys with nothing of
PyJWT replaced. Import the fixture into a test module: ``from shared.tests.hub_jwks import hub_signer  # noqa: F401``.
"""

import base64
import json
import os
import threading
import time
import uuid
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, x25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from shared import hub_node, hub_node_key, hub_sealed, hub_tokens

NODE_NAME = "team"
JWKS_PATH = "/.well-known/jwks.json"


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def seal(plaintext: str, enc_jwk: Mapping[str, str]) -> str:
    """``plaintext`` sealed to the node whose X25519 public JWK is ``enc_jwk``, as the hub's vend seals a JWS.

    The hub's half of ``shared.hub_sealed``: a compact JWE, ``ECDH-ES`` with an ephemeral X25519 ``epk`` and
    ``A256GCM``, the content key derived as the node derives it (``hub_sealed._content_key``).
    """
    peer = x25519.X25519PublicKey.from_public_bytes(hub_node_key.unb64url(enc_jwk["x"]))
    ephemeral = x25519.X25519PrivateKey.generate()
    epk = ephemeral.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    header = {
        "alg": hub_sealed.ALG,
        "enc": hub_sealed.ENC,
        "kid": enc_jwk["kid"],
        "epk": {"kty": "OKP", "crv": "X25519", "x": _b64url(epk)},
    }
    protected = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    iv = os.urandom(12)
    key = hub_sealed._content_key(ephemeral.exchange(peer))
    sealed = AESGCM(key).encrypt(iv, plaintext.encode("utf-8"), protected.encode())
    return ".".join((protected, "", _b64url(iv), _b64url(sealed[:-16]), _b64url(sealed[-16:])))


class SigningKey:
    """One EC P-256 key with a ``kid``, as the hub publishes it."""

    def __init__(self, kid: str | None = None):
        self.kid = kid or uuid.uuid4().hex[:8]
        self.private = ec.generate_private_key(ec.SECP256R1())

    def jwk(self) -> dict:
        public = jwt.algorithms.ECAlgorithm.to_jwk(self.private.public_key(), as_dict=True)
        return {**public, "kid": self.kid, "use": "sig", "alg": "ES256"}


class HubSigner:
    """Serves a JWKS of ``served`` keys and signs tokens with ``key``."""

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

    def sealed(
        self,
        enc_jwk: dict,
        *,
        node: str = NODE_NAME,
        base_uri: str = "s3://bucket/root",
        storage_options: dict | None = None,
        run_id: str = "run-1",
        vend_id: int = 1,
        ttl: int = 900,
        iss: str | None = None,
        key: SigningKey | None = None,
        drop: tuple[str, ...] = (),
        **extra,
    ) -> str:
        """A credential as the hub's vend seals it to the node ``node`` whose X25519 public JWK is ``enc_jwk``.

        The JWS is signed with ``key`` (the served key by default, header ``alg`` and ``kid`` only) and sealed with
        ``seal``; ``drop`` leaves claims out, ``extra`` adds or overrides them.
        """
        now = int(time.time())
        claims = {
            "iss": iss if iss is not None else self.url,
            "aud": f"node:{node}",
            "run_id": run_id,
            "vend_id": vend_id,
            "iat": now,
            "exp": now + ttl,
            "kind": "storage",
            "base_uri": base_uri,
            "storage_options": storage_options or {},
        }
        return self._seal(claims, enc_jwk, key, drop, extra)

    def sealed_secret(
        self,
        enc_jwk: dict,
        *,
        name: str,
        value: str,
        node: str = NODE_NAME,
        run_id: str = "run-1",
        vend_id: int = 1,
        ttl: int = 900,
        iss: str | None = None,
        key: SigningKey | None = None,
        drop: tuple[str, ...] = (),
        **extra,
    ) -> str:
        """The secret ``name`` with ``value`` as the hub's vend seals it to the node ``node``, ``kind`` ``"secret"``."""
        now = int(time.time())
        claims = {
            "iss": iss if iss is not None else self.url,
            "aud": f"node:{node}",
            "run_id": run_id,
            "vend_id": vend_id,
            "iat": now,
            "exp": now + ttl,
            "kind": "secret",
            "name": name,
            "value": value,
        }
        return self._seal(claims, enc_jwk, key, drop, extra)

    def _seal(self, claims: dict, enc_jwk: dict, key: SigningKey | None, drop: tuple[str, ...], extra: dict) -> str:
        claims.update(extra)
        for name in drop:
            claims.pop(name, None)
        key = key or self.key
        signed = jwt.encode(claims, key.private, algorithm="ES256", headers={"kid": key.kid, "typ": None})
        return seal(signed, enc_jwk)

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def hub_signer(monkeypatch, tmp_path):
    """A running ``HubSigner`` that this process verifies against: ``FLOWFILE_HUB_URL`` and ``FLOWFILE_NODE_NAME`` set."""
    signer = HubSigner()
    monkeypatch.setenv("FLOWFILE_HUB_URL", signer.url)
    monkeypatch.setenv("FLOWFILE_NODE_NAME", NODE_NAME)
    monkeypatch.delenv("FLOWFILE_HUB_ISSUER", raising=False)
    # No hub_node.json from this machine's storage dir: the tests set the node up through the variables alone.
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / hub_node.FILE_NAME)
    # A fresh key client per test: a reused port would otherwise serve the previous test's cached keys.
    monkeypatch.setattr(hub_tokens, "_client", None)
    try:
        yield signer
    finally:
        signer.close()
