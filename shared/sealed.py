"""Credentials an issuer seals to this server: opened here, with the server's own key, and nowhere else.

A server with a master key of its own cannot open another core's ``$ffsec$`` ciphertext, so an issuer seals each
credential to each server that runs a step with it: a JWE (ECDH-ES over X25519, A256GCM; RFC 7518 §4.6) around a JWS
the issuer signed for ``node:<name>``. A run carries the whole map of server name to token (``sealed``) and each
process opens only its own server's entry. The module that knows the issuer sets ``server`` and ``sealer``.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from collections.abc import Callable, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from shared.cloud_credential_provider import CredentialResolutionError, split_credentials

if TYPE_CHECKING:
    import polars as pl

ALG = "ECDH-ES"
ENC = "A256GCM"
KEY_BITS = 256
SIGNED_CLAIMS = ["iss", "aud", "iat", "exp"]
REQUIRED_CLAIMS = [*SIGNED_CLAIMS, "storage_options"]
SECRET_CLAIMS = [*SIGNED_CLAIMS, "kind", "name", "value"]
CONNECTION_CLAIMS = [*SIGNED_CLAIMS, "kind", "connection_kind", "name", "settings"]
SEALED_SECRET_PREFIX = "$ffseal$1$"


server: Callable[[], tuple[str | None, Any, str | None, str | None, bool]] | None = None
"""This server as its issuer knows it: its name there, its keys (``enc_kid``, ``agree``), where the issuer's keys are,
the ``iss`` to expect, and whether it joined the issuer with keys of its own."""
sealer = "issuer"


def b64url(data: bytes) -> str:
    """``data`` as JOSE writes bytes: base64url without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def unb64url(text: str) -> bytes:
    """The bytes of the unpadded base64url ``text``; ``ValueError`` when it is not one."""
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


class SealedCredentialRefused(CredentialResolutionError):
    """A sealed credential this server cannot open; ``reason`` names why, never the token or what it holds."""

    def __init__(self, reason: str):
        super().__init__(f"Could not open the credential the {sealer} sealed to this server: {reason}")
        self.reason = reason


def _content_key(shared_secret: bytes) -> bytes:
    """The A256GCM key ECDH-ES derives from ``shared_secret``: Concat KDF over SHA-256, RFC 7518 §4.6.2."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.concatkdf import ConcatKDFHash

    def counted(data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + data

    other_info = counted(ENC.encode("ascii")) + counted(b"") + counted(b"") + struct.pack(">I", KEY_BITS)
    return ConcatKDFHash(algorithm=hashes.SHA256(), length=KEY_BITS // 8, otherinfo=other_info).derive(shared_secret)


def _decrypt(token: str, keys: Any) -> str:
    """The plaintext of the JWE ``token`` sealed to ``keys``' ``enc`` key.

    ``cryptography`` is imported at use: every spawned worker child imports this module and most open nothing.
    """
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    parts = token.split(".") if isinstance(token, str) else []
    if len(parts) != 5:
        raise SealedCredentialRefused("it is not a sealed token")
    protected, encrypted_key, iv, ciphertext, tag = parts
    try:
        header = json.loads(unb64url(protected))
        epk = header["epk"]
        peer = unb64url(epk["x"])
        iv_bytes, sealed = unb64url(iv), unb64url(ciphertext) + unb64url(tag)
    except (ValueError, TypeError, KeyError):
        raise SealedCredentialRefused("its header does not read") from None
    if header.get("alg") != ALG or header.get("enc") != ENC or encrypted_key:
        raise SealedCredentialRefused(f"it is sealed as {header.get('alg')}/{header.get('enc')}, not {ALG}/{ENC}")
    if header.get("zip") is not None or header.get("crit") is not None:
        raise SealedCredentialRefused("its header asks for what this node does not do")
    if header.get("kid") != keys.enc_kid:
        raise SealedCredentialRefused("it is sealed to another key than this node's")
    if not (isinstance(epk, dict) and epk.get("kty") == "OKP" and epk.get("crv") == "X25519" and len(peer) == 32):
        raise SealedCredentialRefused("its ephemeral key is not an X25519 key")
    try:
        return AESGCM(_content_key(keys.agree(peer))).decrypt(iv_bytes, sealed, protected.encode()).decode("utf-8")
    except (InvalidTag, ValueError):
        raise SealedCredentialRefused("it does not decrypt with this node's key") from None


def _claims(sealed: Mapping[str, str], required: list[str]) -> dict[str, Any]:
    """The claims of this server's entry of ``sealed``: decrypted with its key, verified against its issuer's keys."""
    from shared.issuer_tokens import TokenRefused, decode

    if server is None:
        raise SealedCredentialRefused("nothing here says who seals to this server")
    name, keys, jwks_url, issuer, _ = server()
    token = sealed.get(name) if name else None
    if token is None:
        names = ", ".join(sorted(sealed)) or "no server"
        raise SealedCredentialRefused(f"the {sealer} sealed it to {names}, not to this server ({name or 'no name'})")
    if not jwks_url or keys is None:
        raise SealedCredentialRefused(f"this server has not joined a {sealer} with keys of its own")
    try:
        return decode(_decrypt(token, keys), jwks_url, f"node:{name}", issuer, required)
    except TokenRefused as refused:
        raise SealedCredentialRefused(refused.reason) from None


def _open(sealed: Mapping[str, str]) -> tuple[dict[str, str], int]:
    """This node's entry of ``sealed``, opened and verified: the storage options and the ``exp`` they hold until.

    Raises:
        SealedCredentialRefused: No entry or issuer for this node, or the token does not open, verify or hold a
            storage credential.
    """
    claims = _claims(sealed, REQUIRED_CLAIMS)
    if claims.get("kind", "storage") != "storage":
        raise SealedCredentialRefused("it holds a secret, not a storage credential")
    options = claims["storage_options"]
    if not (isinstance(options, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in options.items())):
        raise SealedCredentialRefused("its claims are malformed")
    return dict(options), int(claims["exp"])


def sealed_secret(name: str, sealed: Mapping[str, str]) -> str:
    """The secret ``name`` sealed in ``sealed``, as a string that takes a ``$ffsec$`` ciphertext's place; nothing in
    clear."""
    body = json.dumps({"name": name, "sealed": dict(sealed)}, separators=(",", ":")).encode("utf-8")
    return SEALED_SECRET_PREFIX + b64url(body)


def is_sealed_secret(value: object) -> bool:
    """Whether ``value`` is a ``sealed_secret`` string."""
    return isinstance(value, str) and value.startswith(SEALED_SECRET_PREFIX)


def sealed_connection_secret(kind: str, name: str, field: str, sealed: Mapping[str, str]) -> str:
    """The secret ``field`` of the ``kind`` connection ``name`` sealed in ``sealed``, as ``sealed_secret`` forms it."""
    body = {"name": name, "connection_kind": kind, "field": field, "sealed": dict(sealed)}
    return SEALED_SECRET_PREFIX + b64url(json.dumps(body, separators=(",", ":")).encode("utf-8"))


def open_connection(sealed: Mapping[str, str], kind: str, name: str) -> dict[str, Any]:
    """The whole settings, secrets in plain, of the ``kind`` connection ``name`` in this node's entry of ``sealed``.

    Raises:
        SealedCredentialRefused: The map has no entry for this node, the token does not open or verify, or it holds
            another kind of credential or another connection than ``kind`` ``name``.
    """
    claims = _claims(sealed, CONNECTION_CLAIMS)
    if claims["kind"] != "connection":
        raise SealedCredentialRefused(f"it holds a {claims['kind']} credential, not a connection")
    if claims["connection_kind"] != kind or claims["name"] != name or not isinstance(claims["settings"], dict):
        raise SealedCredentialRefused(f'it does not hold the {kind} connection "{name}"')
    return dict(claims["settings"])


def open_secret(value: str) -> str:
    """The value of the ``sealed_secret`` or ``sealed_connection_secret`` string ``value``, opened here.

    Raises:
        SealedCredentialRefused: The string does not read, or this node's entry does not open, verify or hold the
            secret it names.
    """
    try:
        body = json.loads(unb64url(value.removeprefix(SEALED_SECRET_PREFIX)))
        name, sealed = body["name"], body["sealed"]
    except (ValueError, TypeError, KeyError):
        raise SealedCredentialRefused("it is not a sealed secret") from None
    if not isinstance(name, str) or not isinstance(sealed, dict):
        raise SealedCredentialRefused("it is not a sealed secret")
    if "field" in body:
        kind, field = body.get("connection_kind"), body["field"]
        if not isinstance(kind, str) or not isinstance(field, str):
            raise SealedCredentialRefused("it is not a sealed secret")
        secret = open_connection(sealed, kind, name).get(field)
        if not isinstance(secret, str):
            raise SealedCredentialRefused(f'the {kind} connection "{name}" holds no {field}')
        return secret
    claims = _claims(sealed, SECRET_CLAIMS)
    if claims["kind"] != "secret":
        raise SealedCredentialRefused(f"it holds a {claims['kind']} credential, not a secret")
    if claims["name"] != name or not isinstance(claims["value"], str):
        raise SealedCredentialRefused(f'it does not hold the secret "{name}"')
    return claims["value"]


def can_open() -> bool:
    """Whether this process opens what an issuer seals to its server: it joined one with keys of its own."""
    return server is not None and server()[4]


def sealed_storage_options(sealed: Mapping[str, str]) -> dict[str, str]:
    """The whole storage options of this node's entry of ``sealed``, secrets included, for work that runs here."""
    return _open(sealed)[0]


class SealedCredentialProvider:
    """A Polars ``credential_provider`` whose only state is a vend's ``sealed`` map, opened where the plan executes."""

    def __init__(self, sealed: Mapping[str, str]):
        self.sealed = dict(sealed)
        # Polars caches credentials by the pickled provider; the nonce keeps one entry per plan.
        self.nonce = os.urandom(8).hex()

    def __call__(self) -> tuple[dict[str, str], int | None]:
        options, expires_at = _open(self.sealed)
        return split_credentials(options)[1], expires_at

    def __repr__(self) -> str:
        return f"SealedCredentialProvider(nodes={sorted(self.sealed)!r})"


class SealedConnectionProvider:
    """A Polars ``credential_provider`` holding a connection's name and its ``sealed`` map, opened where it executes."""

    def __init__(self, name: str, sealed: Mapping[str, str]):
        self.name = name
        self.sealed = dict(sealed)
        # Polars caches credentials by the pickled provider; the nonce keeps one entry per plan.
        self.nonce = os.urandom(8).hex()

    def __call__(self) -> tuple[dict[str, str], int | None]:
        from shared.cloud_credential_provider import connection_credentials

        return connection_credentials(open_connection(self.sealed, "cloud", self.name))

    def __repr__(self) -> str:
        return f"SealedConnectionProvider({self.name!r}, nodes={sorted(self.sealed)!r})"


def sealed_scan_kwargs(sealed: Mapping[str, str]) -> dict[str, Any]:
    """What a sink or scan built here takes for ``sealed``: its plain options and a provider holding only the map, so
    the plan carries no secret."""
    options, credentials = split_credentials(sealed_storage_options(sealed))
    kwargs: dict[str, Any] = {"storage_options": options}
    if credentials:
        kwargs["credential_provider"] = SealedCredentialProvider(sealed)
    return kwargs


class SealedDeltaSource:
    """A Polars Python scan reading the Delta table at ``path`` with a ``sealed`` map, opened where it runs.

    It is the scan function itself, not a closure wrapped by ``register_io_source``: Polars cloudpickles a closure and
    refuses it under another Python patch version, while a module's class pickles by reference.
    """

    def __init__(self, path: str, sealed: Mapping[str, str]):
        self.path = path
        self.sealed = dict(sealed)

    def __call__(
        self, with_columns: list[str] | None, predicate: bytes | None, n_rows: int | None, batch_size: int | None
    ) -> tuple[Iterator[pl.DataFrame], bool]:
        import polars as pl

        del batch_size
        try:
            expression = pl.Expr.deserialize(predicate) if predicate else None
        except Exception:
            return self._frames(with_columns, None, n_rows), False
        return self._frames(with_columns, expression, n_rows), True

    def _frames(self, with_columns, predicate, n_rows) -> Iterator[pl.DataFrame]:
        import polars as pl

        frame = pl.scan_delta(self.path, storage_options=sealed_storage_options(self.sealed))
        if predicate is not None:
            frame = frame.filter(predicate)
        if n_rows is not None:
            frame = frame.head(n_rows)
        if with_columns is not None:
            frame = frame.select(with_columns)
        yield frame.collect()

    def __repr__(self) -> str:
        return f"SealedDeltaSource({self.path!r}, nodes={sorted(self.sealed)!r})"


def scan_delta_sealed(path: str, sealed: Mapping[str, str], schema: Mapping[str, Any]) -> pl.LazyFrame:
    """A lazy scan of the Delta table at ``path`` with columns ``schema``, opened with ``sealed`` where it runs.

    A Polars scan takes only keys from a provider, never the endpoint or region, so this is a Python scan whose plan
    names the table and the map, nothing secret.
    """
    import polars as pl

    return pl.LazyFrame._scan_python_function(dict(schema), SealedDeltaSource(path, sealed))
