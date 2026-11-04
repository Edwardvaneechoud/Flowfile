"""This node's own keys on a hub: two key pairs made at its first join and kept beside ``hub_node.json``.

A node that joins a hub holds none of the organisation's keys, so it proves who it is with keys of its own. At its
first join it makes an EC P-256 key pair, ``sig``, that signs what the node sends its hub, and an X25519 key pair,
``enc``, for what the hub hands this node that only it may open. The private halves are kept in ``hub_node_key.json``
in the storage dir, beside ``hub_node.json``, readable by this node's user only, and every later join from that
storage dir reuses them, so a node that joins again presents the same identity. Only the public halves leave the
machine, as JWKs in the join; the private halves are never logged, printed or sent.

The file is ``{"sig": <PEM, PKCS8>, "enc": <base64url of the 32 raw private bytes>}``. Each key's ``kid`` is the first
16 hex characters of SHA-256 over its public key's DER SubjectPublicKeyInfo, the rule the hub uses for its own signing
keys. Nothing runs at import: ``load_or_create()``, which the join calls, makes the keys.
"""

import base64
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

FILE_NAME = "hub_node_key.json"
ASSERTION_SECONDS = 60
"""How long a node assertion holds: its ``exp`` is this many seconds after its ``iat``, the most a hub takes."""


def path() -> Path:
    """``hub_node_key.json`` in the storage dir, beside ``hub_node.json``."""
    return hub_node.path().with_name(FILE_NAME)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64url(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _kid(public_key: ec.EllipticCurvePublicKey | x25519.X25519PublicKey) -> str:
    der = public_key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()[:16]


class NodeKeys:
    """This node's two key pairs. What leaves it is the public halves and what ``sig`` signs, nothing else."""

    __slots__ = ("_sig", "_enc", "sig_kid", "enc_kid")

    def __init__(self, sig: ec.EllipticCurvePrivateKey, enc: x25519.X25519PrivateKey):
        self._sig = sig
        self._enc = enc
        self.sig_kid = _kid(sig.public_key())
        self.enc_kid = _kid(enc.public_key())

    def __repr__(self) -> str:
        return f"NodeKeys(sig={self.sig_kid!r}, enc={self.enc_kid!r})"

    def public_jwks(self) -> dict:
        """The public halves as a join sends them: ``{"sig": <EC P-256 JWK>, "enc": <X25519 JWK>}``."""
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
            "enc": {"kty": "OKP", "crv": "X25519", "x": _b64url(raw), "kid": self.enc_kid, "use": "enc"},
        }

    def agree(self, peer: bytes) -> bytes:
        """The X25519 shared secret of ``enc`` with the 32 raw bytes of a ``peer`` public key, for what is sealed to
        this node; the private half stays here."""
        return self._enc.exchange(x25519.X25519PublicKey.from_public_bytes(peer))

    def assertion(self, name: str, audience: str) -> str:
        """A node assertion: a JWT signed with ``sig`` saying that ``name``, this node on its hub, calls ``audience``.

        ``audience`` is the URL of the hub route it goes to, the hub's issuer followed by the route's path. It holds
        for ``ASSERTION_SECONDS`` and carries a fresh ``jti``, so the hub takes it once.
        """
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
        enc = x25519.X25519PrivateKey.from_private_bytes(_unb64url(data["enc"]))
    except (TypeError, UnsupportedAlgorithm):
        raise ValueError(f"{target} does not hold a node's keys") from None
    if not (isinstance(sig, ec.EllipticCurvePrivateKey) and isinstance(sig.curve, ec.SECP256R1)):
        raise ValueError(f"{target} does not hold a node's keys")
    return NodeKeys(sig, enc)


def _write(target: Path, text: str) -> None:
    """Write ``text`` whole, created readable and writable by this user only; a failed write leaves no part of it."""
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
    """This node's keys from the file, or ``None`` when it has none; never makes them.

    For what a joined node signs after its join, which must not give it keys the hub was never sent. ``OSError`` and
    ``ValueError`` as ``load_or_create``.
    """
    try:
        return _read(path())
    except FileNotFoundError:
        return None


def load_or_create() -> NodeKeys:
    """This node's keys from the file, or both key pairs made now and the file written whole with mode 0600.

    Every later call, in this process or another on the same storage dir, returns the same keys. ``OSError`` when the
    file cannot be read or written; ``ValueError`` when it does not hold a node's keys, and the file is then left as
    it is, so a node's key is never replaced unseen.
    """
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
    _write(target, json.dumps({"sig": pem, "enc": _b64url(raw)}, indent=2) + "\n")
    return NodeKeys(sig, enc)
