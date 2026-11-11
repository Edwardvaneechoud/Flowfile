"""This node's own keys: ``sig`` (EC P-256) signs what it sends its hub, ``enc`` (X25519) opens what the hub seals.

Made at the first join and reused by every later one, so a node that joins again presents the same identity. The
private halves stay in ``hub_node_key.json`` (mode 0600) and are never logged or sent. A ``kid`` is the first 16 hex
of SHA-256 over the public key's DER SubjectPublicKeyInfo, the hub's rule for its own keys. Nothing runs at import.
"""

import hashlib
import json
import os
import time
import uuid
from pathlib import Path

import jwt
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, x25519
from jwt.algorithms import ECAlgorithm

from shared import hub_node
from shared.sealed import b64url, unb64url

FILE_NAME = "hub_node_key.json"
ASSERTION_SECONDS = 60


def path() -> Path:
    return hub_node.path().with_name(FILE_NAME)


def _kid(public_key: ec.EllipticCurvePublicKey | x25519.X25519PublicKey) -> str:
    der = public_key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()[:16]


class NodeKeys:
    __slots__ = ("_sig", "_enc", "sig_kid", "enc_kid")

    def __init__(self, sig: ec.EllipticCurvePrivateKey, enc: x25519.X25519PrivateKey):
        self._sig = sig
        self._enc = enc
        self.sig_kid = _kid(sig.public_key())
        self.enc_kid = _kid(enc.public_key())

    def __repr__(self) -> str:
        return f"NodeKeys(sig={self.sig_kid!r}, enc={self.enc_kid!r})"

    def public_jwks(self) -> dict:
        point = ECAlgorithm.to_jwk(self._sig.public_key(), as_dict=True)
        raw = self._enc.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        return {
            "sig": {
                "kty": "EC",
                "crv": "P-256",
                "x": point["x"],
                "y": point["y"],
                "kid": self.sig_kid,
                "use": "sig",
                "alg": "ES256",
            },
            "enc": {"kty": "OKP", "crv": "X25519", "x": b64url(raw), "kid": self.enc_kid, "use": "enc"},
        }

    def agree(self, peer: bytes) -> bytes:
        return self._enc.exchange(x25519.X25519PublicKey.from_public_bytes(peer))

    def assertion(self, name: str, audience: str) -> str:
        """A JWT for the hub route ``audience`` (issuer + path), with a fresh ``jti`` so the hub takes it once."""
        now = int(time.time())
        subject = f"node:{name}"
        claims = {
            "iss": subject,
            "sub": subject,
            "aud": audience,
            "iat": now,
            "exp": now + ASSERTION_SECONDS,
            "jti": str(uuid.uuid4()),
        }
        return jwt.encode(claims, self._sig, algorithm="ES256", headers={"kid": self.sig_kid, "typ": "JWT"})


def _read(target: Path) -> NodeKeys:
    data = json.loads(target.read_text(encoding="utf-8"))
    if not (isinstance(data, dict) and isinstance(data.get("sig"), str) and isinstance(data.get("enc"), str)):
        raise ValueError(f"{target} does not hold a node's keys")
    try:
        sig = serialization.load_pem_private_key(data["sig"].encode("utf-8"), password=None)
        enc = x25519.X25519PrivateKey.from_private_bytes(unb64url(data["enc"]))
    except (TypeError, UnsupportedAlgorithm):
        raise ValueError(f"{target} does not hold a node's keys") from None
    if not (isinstance(sig, ec.EllipticCurvePrivateKey) and isinstance(sig.curve, ec.SECP256R1)):
        raise ValueError(f"{target} does not hold a node's keys")
    return NodeKeys(sig, enc)


def _write(target: Path, text: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.unlink(missing_ok=True)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def load() -> NodeKeys | None:
    """The keys, or ``None``; never makes them, since keys the hub was never sent would be useless."""
    try:
        return _read(path())
    except FileNotFoundError:
        return None


def load_or_create() -> NodeKeys:
    """The keys, made now when there is no file; a file that holds no keys is ``ValueError`` and left as it is, so a
    node's key is never replaced unseen."""
    target = path()
    try:
        return _read(target)
    except FileNotFoundError:
        pass
    sig = ec.generate_private_key(ec.SECP256R1())
    enc = x25519.X25519PrivateKey.generate()
    pem = sig.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode("ascii")
    raw = enc.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
    _write(target, json.dumps({"sig": pem, "enc": b64url(raw)}, indent=2) + "\n")
    return NodeKeys(sig, enc)
