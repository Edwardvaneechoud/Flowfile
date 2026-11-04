"""Credentials a hub seals to this node: opened here, with the node's own key, and nowhere else.

A server that joined its hub with keys of its own has its own master key and catalog database, so a credential core
encrypted (``$ffsec$``) or a connection core stored names nothing there. A vend that names its target nodes therefore
seals each credential to each node that runs a step with it: a JWE compact token to the node's ``enc`` key (``alg``
ECDH-ES with an ephemeral X25519 ``epk``, Concat KDF with empty ``apu`` and ``apv``, ``enc`` A256GCM, the protected
header as the AAD; RFC 7518 §4.6), whose plaintext is a JWS the hub signed with its node-token key, with the claims
``iss``, ``aud`` (``node:<name>``), ``run_id``, ``vend_id``, ``iat``, ``exp`` and ``kind``: ``"storage"`` (or no
``kind``) with ``base_uri`` and ``storage_options``, ``"secret"`` with a secret's ``name`` and ``value``.

A run carries the vend's whole map of node name to token, ``sealed``; whichever process opens it takes this node's
entry, decrypts it with ``hub_node_key``'s ``enc`` key and verifies the JWS as ``shared.hub_tokens.decode`` verifies
any token the hub signs: against the hub's JWKS, its audience, issuer and expiry. Core hands the map on and opens only
its own node's entry. ``SealedCredentialProvider`` is a Polars ``credential_provider`` holding only the map, so a plan
carries no secret; ``sealed_scan_kwargs`` gives a scan built here its plain options beside it,
``sealed_storage_options`` the whole options for work that runs here, and ``scan_delta_sealed`` reads a Delta table as
a Python IO source, for a plan built where the options are not known. A secret travels as ``sealed_secret``'s string in
place of a ``$ffsec$`` ciphertext, and ``open_secret`` gives its value where the step runs. Nothing runs at import.
"""

from __future__ import annotations

import json
import os
import struct
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.concatkdf import ConcatKDFHash

from shared import hub_node, hub_node_key, hub_tokens
from shared.cloud_credential_provider import CredentialResolutionError, split_credentials

if TYPE_CHECKING:
    import polars as pl

ALG = "ECDH-ES"
ENC = "A256GCM"
KEY_BITS = 256
SIGNED_CLAIMS = ["iss", "aud", "iat", "exp", "run_id", "vend_id"]
REQUIRED_CLAIMS = [*SIGNED_CLAIMS, "base_uri", "storage_options"]
SECRET_CLAIMS = [*SIGNED_CLAIMS, "kind", "name", "value"]
SEALED_SECRET_PREFIX = "$ffseal$1$"


class SealedCredentialRefused(CredentialResolutionError):
    """A sealed credential this node cannot open; ``reason`` names why, never the token or what it holds."""

    def __init__(self, reason: str):
        super().__init__(f"Could not open the credential the hub sealed to this server: {reason}")
        self.reason = reason


@dataclass(frozen=True, repr=False)
class OpenedCredential:
    """What a sealed token holds once opened and verified: the storage root, its options, the vend and its expiry.

    ``storage_options`` are what Polars and deltalake take, secrets included; ``repr`` never shows them.
    """

    base_uri: str
    storage_options: dict[str, str]
    run_id: str
    vend_id: int
    expires_at: int

    def __repr__(self) -> str:
        return f"OpenedCredential(base_uri={self.base_uri!r}, vend_id={self.vend_id!r})"


def _content_key(shared_secret: bytes) -> bytes:
    """The A256GCM key ECDH-ES derives from ``shared_secret``: Concat KDF over SHA-256, RFC 7518 §4.6.2.

    The OtherInfo is the AlgorithmID (the ``enc`` value, for direct key agreement), empty PartyUInfo and PartyVInfo,
    each with its 32-bit big-endian length, and SuppPubInfo, the key length in bits.
    """

    def counted(data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + data

    other_info = counted(ENC.encode("ascii")) + counted(b"") + counted(b"") + struct.pack(">I", KEY_BITS)
    return ConcatKDFHash(algorithm=hashes.SHA256(), length=KEY_BITS // 8, otherinfo=other_info).derive(shared_secret)


def _decrypt(token: str, keys: hub_node_key.NodeKeys) -> str:
    """The plaintext of the JWE ``token`` sealed to ``keys``' ``enc`` key."""
    parts = token.split(".") if isinstance(token, str) else []
    if len(parts) != 5:
        raise SealedCredentialRefused("it is not a sealed token")
    protected, encrypted_key, iv, ciphertext, tag = parts
    try:
        header = json.loads(hub_node_key.unb64url(protected))
        epk = header["epk"]
        peer = hub_node_key.unb64url(epk["x"])
        iv_bytes, sealed = hub_node_key.unb64url(iv), hub_node_key.unb64url(ciphertext) + hub_node_key.unb64url(tag)
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


def _claims(token: str, required: list[str]) -> dict[str, Any]:
    """The claims of ``token``, sealed to this node: decrypted with its key and verified against its hub's keys."""
    node = hub_node.resolved()
    try:
        keys = hub_node_key.load()
    except (OSError, ValueError):
        keys = None
    if not node.hub_url or not node.name or keys is None:
        raise SealedCredentialRefused("this server has not joined a hub with keys of its own")
    signed = _decrypt(token, keys)
    try:
        claims = hub_tokens.decode(
            signed, node.hub_url + hub_tokens.JWKS_PATH, f"node:{node.name}", node.issuer, required
        )
    except hub_tokens.HubTokenRefused as refused:
        raise SealedCredentialRefused(refused.reason) from None
    if not isinstance(claims["vend_id"], int):
        raise SealedCredentialRefused("its claims are malformed")
    return claims


def open_token(token: str) -> OpenedCredential:
    """The storage credential in ``token``, sealed to this node: decrypted with its key and verified against its hub's
    keys. A token with no ``kind`` is a storage credential, as the hub sealed them before it sealed secrets.

    Raises:
        SealedCredentialRefused: This node is not on a hub with keys of its own, or the token does not decrypt, its
            signature, audience (``node:<name>``), issuer or expiry does not hold, it holds a secret, or a claim is
            missing or malformed.
    """
    claims = _claims(token, REQUIRED_CLAIMS)
    if claims.get("kind", "storage") != "storage":
        raise SealedCredentialRefused("it holds a secret, not a storage credential")
    options = claims["storage_options"]
    if not (
        isinstance(options, dict)
        and all(isinstance(name, str) and isinstance(value, str) for name, value in options.items())
        and isinstance(claims["base_uri"], str)
    ):
        raise SealedCredentialRefused("its claims are malformed")
    return OpenedCredential(
        base_uri=claims["base_uri"].rstrip("/"),
        storage_options=dict(options),
        run_id=str(claims["run_id"]),
        vend_id=claims["vend_id"],
        expires_at=int(claims["exp"]),
    )


def _this_nodes_entry(sealed: Mapping[str, str]) -> str:
    name = hub_node.resolved().name
    token = sealed.get(name) if name else None
    if token is None:
        names = ", ".join(sorted(sealed)) or "no server"
        raise SealedCredentialRefused(f"the hub sealed it to {names}, not to this server ({name or 'no name'})")
    return token


def open_sealed(sealed: Mapping[str, str]) -> OpenedCredential:
    """This node's entry of a vend's ``sealed`` map, opened (``open_token``).

    Raises:
        SealedCredentialRefused: The map has no entry for this node's name, or ``open_token`` refuses it.
    """
    return open_token(_this_nodes_entry(sealed))


def sealed_secret(name: str, sealed: Mapping[str, str]) -> str:
    """The secret ``name`` a vend sealed to each node of ``sealed``, as a string that takes a ``$ffsec$`` ciphertext's
    place in what a step carries: ``SEALED_SECRET_PREFIX`` and the name and map as base64url JSON, nothing in clear."""
    body = json.dumps({"name": name, "sealed": dict(sealed)}, separators=(",", ":")).encode("utf-8")
    return SEALED_SECRET_PREFIX + hub_node_key.b64url(body)


def is_sealed_secret(value: object) -> bool:
    """Whether ``value`` is a ``sealed_secret`` string."""
    return isinstance(value, str) and value.startswith(SEALED_SECRET_PREFIX)


def open_secret(value: str) -> str:
    """The value of the ``sealed_secret`` string ``value``: this node's entry opened with its key and verified.

    Raises:
        SealedCredentialRefused: The string does not read, the map has no entry for this node, the token does not
            open or verify (``open_token``'s reasons), or it holds another kind or another secret than it names.
    """
    try:
        body = json.loads(hub_node_key.unb64url(value.removeprefix(SEALED_SECRET_PREFIX)))
        name, sealed = body["name"], body["sealed"]
    except (ValueError, TypeError, KeyError):
        raise SealedCredentialRefused("it is not a sealed secret") from None
    if not isinstance(name, str) or not isinstance(sealed, dict):
        raise SealedCredentialRefused("it is not a sealed secret")
    claims = _claims(_this_nodes_entry(sealed), SECRET_CLAIMS)
    if claims["kind"] != "secret":
        raise SealedCredentialRefused(f"it holds a {claims['kind']} credential, not a secret")
    if claims["name"] != name or not isinstance(claims["value"], str):
        raise SealedCredentialRefused(f'it does not hold the secret "{name}"')
    return claims["value"]


def can_open() -> bool:
    """Whether this process opens what a hub seals to its node: it has the hub's URL, its name there and its keys."""
    node = hub_node.resolved()
    if not node.hub_url or not node.name:
        return False
    try:
        return hub_node_key.load() is not None
    except (OSError, ValueError):
        return False


def sealed_storage_options(sealed: Mapping[str, str]) -> dict[str, str]:
    """The whole storage options of this node's entry of ``sealed``, secrets included, for work that runs here."""
    return open_sealed(sealed).storage_options


class SealedCredentialProvider:
    """A Polars ``credential_provider`` whose only state is a vend's ``sealed`` map, opened where the plan executes."""

    def __init__(self, sealed: Mapping[str, str]):
        self.sealed = dict(sealed)
        # Polars caches credentials by the pickled provider; the nonce keeps one entry per plan.
        self.nonce = os.urandom(8).hex()

    def __call__(self) -> tuple[dict[str, str], int | None]:
        opened = open_sealed(self.sealed)
        return split_credentials(opened.storage_options)[1], opened.expires_at

    def __repr__(self) -> str:
        return f"SealedCredentialProvider(nodes={sorted(self.sealed)!r})"


def sealed_scan_kwargs(sealed: Mapping[str, str]) -> dict[str, Any]:
    """What a sink or scan built here takes for ``sealed``: its plain options and a provider holding only the map.

    The map is opened here for the plain options (endpoint, region, plain HTTP), so the plan carries those and no
    secret, and the provider opens it again wherever the plan runs.
    """
    options, credentials = split_credentials(sealed_storage_options(sealed))
    kwargs: dict[str, Any] = {"storage_options": options}
    if credentials:
        kwargs["credential_provider"] = SealedCredentialProvider(sealed)
    return kwargs


class SealedDeltaSource:
    """A Polars IO source reading the Delta table at ``path`` with a vend's ``sealed`` map, opened where it runs."""

    def __init__(self, path: str, sealed: Mapping[str, str]):
        self.path = path
        self.sealed = dict(sealed)

    def __call__(self, with_columns, predicate, n_rows, batch_size) -> Iterator[pl.DataFrame]:
        import polars as pl

        del batch_size
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

    For a plan built in a process that does not hold the options (core, sending a read to another node): a Polars
    scan takes only keys from a provider, never the endpoint or region, so the read is a Python IO source instead, and
    its plan names the table and the map, nothing secret. Projection, predicate and row limit are pushed into it.
    """
    from polars.io.plugins import register_io_source

    return register_io_source(SealedDeltaSource(path, sealed), schema=dict(schema))
