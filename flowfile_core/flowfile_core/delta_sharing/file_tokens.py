"""Signed, short-lived URLs for the files of a table on local disk, which no object store can presign.

A token is ``<base64url(payload)>.<base64url(hmac)>``; the payload names the share table row, the file's path
relative to the table directory, a digest of that directory and the expiry. The HMAC key is derived from the
master key with HKDF, so a token minted by one install is worthless on another and nothing is stored.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from flowfile_core.auth.secrets import get_master_key

_INFO = b"delta-sharing-files"
_SALT = b"flowfile-delta-sharing-v1"


@dataclass(frozen=True)
class FileGrant:
    """What a valid token allows: one file of one shared table, until ``expires_at`` (epoch seconds)."""

    share_table_id: int
    rel_path: str
    table_digest: str
    expires_at: int


def _key() -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=_SALT, info=_INFO).derive(get_master_key().encode())


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def table_digest(table_path: str) -> str:
    """A short digest of a table's storage path, which a token is bound to (SQLite reuses rowids)."""
    return hashlib.sha256(table_path.encode()).hexdigest()[:24]


def mint(share_table_id: int, rel_path: str, table_path: str, expires_at: int) -> str:
    payload = json.dumps(
        {"t": share_table_id, "p": rel_path, "d": table_digest(table_path), "e": int(expires_at)},
        separators=(",", ":"),
    ).encode()
    signature = hmac.new(_key(), payload, hashlib.sha256).digest()
    return f"{_b64(payload)}.{_b64(signature)}"


def verify(token: str, *, now: float | None = None) -> FileGrant | None:
    """The grant a token carries, or ``None`` when it is malformed, forged or expired."""
    try:
        payload_part, signature_part = token.split(".", 1)
        payload = _unb64(payload_part)
        signature = _unb64(signature_part)
    except (ValueError, TypeError):
        return None
    expected = hmac.new(_key(), payload, hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        data = json.loads(payload)
        grant = FileGrant(
            share_table_id=int(data["t"]),
            rel_path=str(data["p"]),
            table_digest=str(data["d"]),
            expires_at=int(data["e"]),
        )
    except (ValueError, KeyError, TypeError):
        return None
    if grant.expires_at < (time.time() if now is None else now):
        return None
    return grant
