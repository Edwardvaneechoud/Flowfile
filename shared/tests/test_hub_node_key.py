"""A node's own keys: made once per storage dir and kept for its user only, sent as public JWKs, and the assertion
the node signs with them."""

import base64
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
import uuid

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, x25519

from shared import hub_node, hub_node_key
from shared.hub_node_key import NodeKeys

AUDIENCE = "http://hub.example:63580/hub/nodes/heartbeat"
CLAIMS = ["iss", "sub", "aud", "iat", "exp", "jti"]
POSIX_MODES = pytest.mark.skipif(os.name == "nt", reason="Windows keeps no POSIX file modes")


@pytest.fixture(autouse=True)
def key_file(tmp_path, monkeypatch):
    """The key file in a storage dir of the test's own, beside where ``hub_node.json`` would be; absent at first."""
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    return tmp_path / "storage" / hub_node_key.FILE_NAME


def unb64url(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def kid_of(public_key) -> str:
    der = public_key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()[:16]


def kept(key_file) -> tuple[ec.EllipticCurvePrivateKey, x25519.X25519PrivateKey]:
    data = json.loads(key_file.read_text(encoding="utf-8"))
    sig = serialization.load_pem_private_key(data["sig"].encode(), password=None)
    return sig, x25519.X25519PrivateKey.from_private_bytes(unb64url(data["enc"]))


def other_keys() -> NodeKeys:
    return NodeKeys(ec.generate_private_key(ec.SECP256R1()), x25519.X25519PrivateKey.generate())


def test_the_key_file_is_beside_the_nodes_file(key_file):
    assert hub_node_key.path() == key_file == hub_node.path().with_name("hub_node_key.json")


def test_the_first_call_writes_a_pkcs8_p256_key_and_32_raw_x25519_bytes(key_file):
    keys = hub_node_key.load_or_create()

    data = json.loads(key_file.read_text(encoding="utf-8"))
    assert set(data) == {"sig", "enc"}
    assert data["sig"].startswith("-----BEGIN PRIVATE KEY-----\n")
    sig, enc = kept(key_file)
    assert isinstance(sig, ec.EllipticCurvePrivateKey) and isinstance(sig.curve, ec.SECP256R1)
    assert len(unb64url(data["enc"])) == 32 and "=" not in data["enc"]
    assert (keys.sig_kid, keys.enc_kid) == (kid_of(sig.public_key()), kid_of(enc.public_key()))
    assert not key_file.with_name(key_file.name + ".tmp").exists()


@POSIX_MODES
def test_the_key_file_is_readable_by_this_user_only(key_file):
    hub_node_key.load_or_create()

    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600


def test_a_second_call_returns_the_same_keys_and_leaves_the_file(key_file):
    first = hub_node_key.load_or_create()
    written = key_file.read_bytes()

    second = hub_node_key.load_or_create()

    assert second.public_jwks() == first.public_jwks()
    assert (second.sig_kid, second.enc_kid) == (first.sig_kid, first.enc_kid)
    assert key_file.read_bytes() == written


def test_a_node_started_again_reads_the_keys_from_its_storage_dir(key_file):
    made = hub_node_key.load_or_create().public_jwks()
    script = (
        "import json\n"
        "from shared import hub_node_key\n"
        "print(json.dumps({'path': str(hub_node_key.path()), 'keys': hub_node_key.load_or_create().public_jwks()}))\n"
    )
    env = {**os.environ, "FLOWFILE_STORAGE_DIR": str(key_file.parent)}

    done = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=60)

    assert done.returncode == 0, done.stderr
    again = json.loads(done.stdout.strip().splitlines()[-1])
    assert again == {"path": str(key_file), "keys": made}


def test_the_public_jwks_have_the_contracts_members_and_no_private_part():
    jwks = hub_node_key.load_or_create().public_jwks()

    assert set(jwks) == {"sig", "enc"}
    sig, enc = jwks["sig"], jwks["enc"]
    assert list(sig) == ["kty", "crv", "x", "y", "kid", "use", "alg"]
    assert (sig["kty"], sig["crv"], sig["use"], sig["alg"]) == ("EC", "P-256", "sig", "ES256")
    assert list(enc) == ["kty", "crv", "x", "kid", "use"]
    assert (enc["kty"], enc["crv"], enc["use"]) == ("OKP", "X25519", "enc")
    for jwk in (sig, enc):
        assert "d" not in jwk
        assert re.fullmatch(r"[0-9a-f]{16}", jwk["kid"])


def test_each_kid_is_the_sha256_of_its_public_keys_der_and_the_jwks_carry_the_kept_keys(key_file):
    jwks = hub_node_key.load_or_create().public_jwks()
    sig, enc = kept(key_file)

    sig_public = jwt.PyJWK(jwks["sig"]).key
    enc_public = x25519.X25519PublicKey.from_public_bytes(unb64url(jwks["enc"]["x"]))

    assert jwks["sig"]["kid"] == kid_of(sig_public) == kid_of(sig.public_key())
    assert jwks["enc"]["kid"] == kid_of(enc_public) == kid_of(enc.public_key())
    assert sig_public.public_numbers() == sig.public_key().public_numbers()


def test_an_assertion_verifies_with_the_public_jwk_and_carries_every_claim():
    keys = hub_node_key.load_or_create()
    before = int(time.time())

    token = keys.assertion("worker-1", AUDIENCE)

    assert jwt.get_unverified_header(token) == {"alg": "ES256", "kid": keys.sig_kid, "typ": "JWT"}
    claims = jwt.decode(
        token,
        jwt.PyJWK(keys.public_jwks()["sig"]),
        algorithms=["ES256"],
        audience=AUDIENCE,
        issuer="node:worker-1",
        options={"require": CLAIMS},
    )
    assert set(claims) == set(CLAIMS)
    assert claims["sub"] == "node:worker-1"
    assert isinstance(claims["iat"], int) and before <= claims["iat"] <= int(time.time())
    assert claims["exp"] - claims["iat"] == 60
    assert uuid.UUID(claims["jti"]).version == 4


def test_each_assertion_has_a_fresh_jti():
    keys = hub_node_key.load_or_create()

    tokens = [keys.assertion("worker-1", AUDIENCE) for _ in range(3)]

    jtis = {jwt.decode(token, options={"verify_signature": False})["jti"] for token in tokens}
    assert len(jtis) == 3


def test_an_assertion_does_not_verify_with_another_nodes_key():
    token = hub_node_key.load_or_create().assertion("worker-1", AUDIENCE)

    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(token, jwt.PyJWK(other_keys().public_jwks()["sig"]), algorithms=["ES256"], audience=AUDIENCE)


def test_the_repr_names_the_two_kids_only(key_file):
    keys = hub_node_key.load_or_create()
    data = json.loads(key_file.read_text(encoding="utf-8"))

    assert repr(keys) == str(keys) == f"NodeKeys(sig={keys.sig_kid!r}, enc={keys.enc_kid!r})"
    pem_body = data["sig"].splitlines()[1]
    assert pem_body not in repr(keys) and data["enc"] not in repr(keys)


@pytest.mark.parametrize(
    "text",
    [
        "{not json",
        "[]",
        json.dumps({"sig": "not a key", "enc": "AAAA"}),
        json.dumps({"enc": base64.urlsafe_b64encode(bytes(32)).decode()}),
        "p384",
        "short enc",
    ],
    ids=["not json", "not an object", "sig not a pem", "no sig", "a p-384 key", "enc of 16 bytes"],
)
def test_a_file_that_does_not_hold_a_nodes_keys_raises_and_is_left_as_it_is(key_file, text):
    if text in ("p384", "short enc"):
        hub_node_key.load_or_create()
        data = json.loads(key_file.read_text(encoding="utf-8"))
        if text == "p384":
            other = ec.generate_private_key(ec.SECP384R1())
            data["sig"] = other.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            ).decode()
        else:
            data["enc"] = data["enc"][:22]
        text = json.dumps(data)
    key_file.parent.mkdir(parents=True, exist_ok=True)
    key_file.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError):
        hub_node_key.load_or_create()

    assert key_file.read_text(encoding="utf-8") == text


@POSIX_MODES
@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root writes into a read-only directory")
def test_a_storage_dir_that_cannot_hold_the_file_raises_and_keeps_no_part_of_it(key_file):
    key_file.parent.mkdir(parents=True)
    key_file.parent.chmod(0o500)
    try:
        with pytest.raises(PermissionError):
            hub_node_key.load_or_create()
        assert os.listdir(key_file.parent) == []
    finally:
        key_file.parent.chmod(0o700)


def test_a_write_that_fails_half_way_leaves_no_private_key_behind(key_file, monkeypatch):
    def full_disk(source, target):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(hub_node_key.os, "replace", full_disk)

    with pytest.raises(OSError):
        hub_node_key.load_or_create()

    assert os.listdir(key_file.parent) == []
