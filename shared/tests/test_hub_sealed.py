"""A credential the hub sealed to this node: opened with the node's own key and checked against the hub's keys.

The hub is ``HubSigner`` (its JWKS served from a thread), the node ``team`` with keys made under the test's storage
dir, and each token is sealed as the hub's vend seals one (``HubSigner.sealed``), so the real JWE and JWS code runs
both ways with nothing replaced.
"""

import base64
import json
import pickle

import polars as pl
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.concatkdf import ConcatKDFHash

from shared import hub_node_key, hub_sealed
from shared.hub_sealed import SealedCredentialProvider, SealedCredentialRefused
from shared.tests import hub_jwks
from shared.tests.hub_jwks import NODE_NAME, HubSigner, SigningKey

hub_signer = hub_jwks.hub_signer

OPTIONS = {
    "aws_access_key_id": "AKIAVENDED",
    "aws_secret_access_key": "vended-secret",
    "aws_session_token": "vended-session",
    "aws_region": "eu-west-1",
    "endpoint_url": "http://minio.example:9000",
    "aws_allow_http": "true",
}


def unb64url(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


@pytest.fixture
def node(hub_signer):
    """This node's keys, made in the test's storage dir; the node is ``team`` on ``hub_signer``."""
    return hub_node_key.load_or_create()


def sealed_for(signer: HubSigner, keys, **claims) -> str:
    return signer.sealed(keys.public_jwks()["enc"], storage_options=dict(OPTIONS), **claims)


def test_a_token_sealed_to_this_node_opens_to_its_claims(hub_signer, node):
    token = sealed_for(hub_signer, node, base_uri="s3://bucket/sales/", run_id="r-9", vend_id=12)

    opened = hub_sealed.open_token(token)

    assert (opened.base_uri, opened.run_id, opened.vend_id) == ("s3://bucket/sales", "r-9", 12)
    assert opened.storage_options == OPTIONS
    assert "vended-secret" not in repr(opened)


def test_the_jwe_is_compact_ecdh_es_a256gcm_to_the_nodes_enc_key(hub_signer, node):
    token = sealed_for(hub_signer, node)
    protected, encrypted_key, iv, ciphertext, tag = token.split(".")
    header = json.loads(unb64url(protected))

    assert set(header) == {"alg", "enc", "kid", "epk"}
    assert (header["alg"], header["enc"], header["kid"]) == ("ECDH-ES", "A256GCM", node.enc_kid)
    assert set(header["epk"]) == {"kty", "crv", "x"} and header["epk"]["crv"] == "X25519"
    assert encrypted_key == "" and len(unb64url(iv)) == 12 and len(unb64url(tag)) == 16
    assert b"vended-secret" not in unb64url(ciphertext)


def test_the_content_key_is_concat_kdf_with_the_rfc_other_info(hub_signer, node):
    """Derived independently here, as RFC 7518 §4.6.2 spells it, the key decrypts what ``seal`` sealed."""
    token = sealed_for(hub_signer, node)
    protected, _, iv, ciphertext, tag = token.split(".")
    epk = unb64url(json.loads(unb64url(protected))["epk"]["x"])
    shared_secret = node.agree(epk)
    other_info = b"\x00\x00\x00\x07A256GCM" + b"\x00\x00\x00\x00" * 2 + b"\x00\x00\x01\x00"
    key = ConcatKDFHash(hashes.SHA256(), 32, other_info).derive(shared_secret)

    signed = AESGCM(key).decrypt(unb64url(iv), unb64url(ciphertext) + unb64url(tag), protected.encode()).decode()

    assert signed.count(".") == 2


def test_a_token_sealed_to_another_nodes_key_is_refused(hub_signer, node):
    with pytest.raises(SealedCredentialRefused, match="sealed to another key"):
        hub_sealed.open_token(hub_signer.sealed(_jwk(), storage_options=dict(OPTIONS)))


def test_a_token_whose_ciphertext_changed_does_not_decrypt(hub_signer, node):
    protected, key, iv, ciphertext, tag = sealed_for(hub_signer, node).split(".")
    flipped = bytearray(unb64url(ciphertext))
    flipped[0] ^= 1
    tampered = ".".join((protected, key, iv, base64.urlsafe_b64encode(bytes(flipped)).rstrip(b"=").decode(), tag))

    with pytest.raises(SealedCredentialRefused, match="does not decrypt"):
        hub_sealed.open_token(tampered)


@pytest.mark.parametrize(
    ("claims", "reason"),
    [
        ({"node": "other"}, "for another node than team"),
        ({"iss": "https://elsewhere.example"}, "another issuer"),
        ({"ttl": -120}, "expired"),
        ({"drop": ("vend_id",)}, "no claim vend_id"),
        ({"drop": ("storage_options",)}, "no claim storage_options"),
        ({"storage_options": {"aws_access_key_id": 7}}, "malformed"),
        ({"key": SigningKey(kid="unknown")}, "does not publish the key"),
    ],
    ids=["audience", "issuer", "expired", "vend_id", "options", "malformed", "unknown key"],
)
def test_a_token_whose_signed_claims_do_not_hold_is_refused(hub_signer, node, claims, reason):
    token = hub_signer.sealed(node.public_jwks()["enc"], **{"storage_options": dict(OPTIONS), **claims})

    with pytest.raises(SealedCredentialRefused, match=reason):
        hub_sealed.open_token(token)


def test_a_signature_by_a_key_of_the_same_kid_that_is_not_the_hubs_is_refused(hub_signer, node):
    forged = SigningKey(kid=hub_signer.key.kid)

    with pytest.raises(SealedCredentialRefused, match="signature does not hold"):
        hub_sealed.open_token(sealed_for(hub_signer, node, key=forged))


def test_a_node_without_keys_opens_nothing(hub_signer):
    token = hub_signer.sealed(_jwk())

    assert not hub_sealed.can_open()
    with pytest.raises(SealedCredentialRefused, match="has not joined a hub with keys"):
        hub_sealed.open_token(token)


def _jwk() -> dict:
    raw = x25519.X25519PrivateKey.generate().public_key().public_bytes_raw()
    return {"kty": "OKP", "crv": "X25519", "x": base64.urlsafe_b64encode(raw).rstrip(b"=").decode(), "kid": "k"}


def test_the_map_opens_this_nodes_own_entry_and_names_the_others_when_it_has_none(hub_signer, node):
    mine = sealed_for(hub_signer, node)

    assert hub_sealed.open_sealed({"worker": "x.y.z.w.v", NODE_NAME: mine}).storage_options == OPTIONS
    assert hub_sealed.can_open()
    with pytest.raises(SealedCredentialRefused, match=r"sealed it to core, worker, not to this server \(team\)"):
        hub_sealed.open_sealed({"worker": mine, "core": mine})


def test_the_provider_holds_only_the_map_and_answers_the_keys_and_expiry(hub_signer, node):
    sealed = {NODE_NAME: sealed_for(hub_signer, node, ttl=600)}
    provider = SealedCredentialProvider(sealed)

    credentials, expiry = provider()

    assert credentials == {
        key: OPTIONS[key] for key in ("aws_access_key_id", "aws_secret_access_key", "aws_session_token")
    }
    assert expiry == hub_sealed.open_sealed(sealed).expires_at
    assert b"vended-secret" not in pickle.dumps(provider)
    assert "vended" not in repr(provider)


def test_scan_kwargs_carry_the_plain_options_and_a_provider_never_the_secret(hub_signer, node):
    sealed = {NODE_NAME: sealed_for(hub_signer, node)}

    kwargs = hub_sealed.sealed_scan_kwargs(sealed)

    assert kwargs["storage_options"] == {
        "aws_region": "eu-west-1",
        "endpoint_url": "http://minio.example:9000",
        "aws_allow_http": "true",
    }
    assert isinstance(kwargs["credential_provider"], SealedCredentialProvider)
    plan = pl.scan_parquet("s3://bucket/root/x.parquet", **kwargs).serialize()
    assert b"vended-secret" not in plan and b"AKIAVENDED" not in plan


def test_a_sealed_delta_scan_names_the_table_and_the_map_and_no_secret(hub_signer, node):
    sealed = {NODE_NAME: sealed_for(hub_signer, node)}

    frame = hub_sealed.scan_delta_sealed("s3://bucket/root/orders", sealed, {"id": pl.Int64, "country": pl.String})

    assert frame.collect_schema() == pl.Schema({"id": pl.Int64, "country": pl.String})
    plan = frame.filter(pl.col("id") > 1).serialize()
    assert b"vended-secret" not in plan and b"AKIAVENDED" not in plan


def test_a_storage_token_without_a_kind_still_opens_as_storage(hub_signer, node):
    token = hub_signer.sealed(node.public_jwks()["enc"], storage_options=dict(OPTIONS), drop=("kind",))

    assert hub_sealed.open_token(token).storage_options == OPTIONS


def secret_for(signer: HubSigner, keys, name: str = "warehouse_pw", value: str = "s3cr3t", **claims) -> str:
    return signer.sealed_secret(keys.public_jwks()["enc"], name=name, value=value, **claims)


def test_a_sealed_secret_opens_to_its_value_on_the_node_it_was_sealed_to(hub_signer, node):
    value = hub_sealed.sealed_secret("warehouse_pw", {NODE_NAME: secret_for(hub_signer, node), "core": "a.b.c.d.e"})

    assert hub_sealed.is_sealed_secret(value) and not hub_sealed.is_sealed_secret("$ffsec$1$1$abc")
    assert "s3cr3t" not in value and "warehouse_pw" not in value
    assert hub_sealed.open_secret(value) == "s3cr3t"


def test_a_sealed_secret_is_not_a_storage_credential_and_a_storage_token_is_not_a_secret(hub_signer, node):
    secret = secret_for(hub_signer, node)
    storage = sealed_for(hub_signer, node)

    with pytest.raises(SealedCredentialRefused, match="no claim base_uri"):
        hub_sealed.open_token(secret)
    with pytest.raises(SealedCredentialRefused, match="it has no claim name"):
        hub_sealed.open_secret(hub_sealed.sealed_secret("warehouse_pw", {NODE_NAME: storage}))
    with pytest.raises(SealedCredentialRefused, match="holds a storage credential, not a secret"):
        hub_sealed.open_secret(hub_sealed.sealed_secret("x", {NODE_NAME: secret_for(hub_signer, node, kind="storage")}))


@pytest.mark.parametrize(
    ("claims", "reason"),
    [
        ({"node": "other"}, "for another node than team"),
        ({"iss": "https://elsewhere.example"}, "another issuer"),
        ({"ttl": -120}, "expired"),
        ({"name": "another"}, 'does not hold the secret "warehouse_pw"'),
        ({"drop": ("value",)}, "no claim value"),
    ],
    ids=["audience", "issuer", "expired", "other name", "no value"],
)
def test_a_sealed_secret_whose_claims_do_not_hold_is_refused(hub_signer, node, claims, reason):
    value = hub_sealed.sealed_secret("warehouse_pw", {NODE_NAME: secret_for(hub_signer, node, **claims)})

    with pytest.raises(SealedCredentialRefused, match=reason):
        hub_sealed.open_secret(value)


def test_a_sealed_secret_for_other_nodes_or_garbled_names_why(hub_signer, node):
    with pytest.raises(SealedCredentialRefused, match=r"sealed it to worker, not to this server \(team\)"):
        hub_sealed.open_secret(hub_sealed.sealed_secret("pw", {"worker": secret_for(hub_signer, node)}))
    with pytest.raises(SealedCredentialRefused, match="not a sealed secret"):
        hub_sealed.open_secret(hub_sealed.SEALED_SECRET_PREFIX + "not-json")
