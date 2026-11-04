"""The one verifier of tokens from a configured issuer: what it accepts, what it refuses and why, and when it is off."""

import json
import logging
import time

import pytest

from shared import hub_node, hub_tokens
from shared.hub_tokens import HubClaims, HubTokenRefused, verify
from shared.tests import hub_jwks
from shared.tests.hub_jwks import NODE_NAME, SigningKey

hub_signer = hub_jwks.hub_signer


def refusal(token: str) -> str:
    with pytest.raises(HubTokenRefused) as refused:
        verify(token)
    return refused.value.reason


def test_a_run_token_for_this_node_is_accepted_with_its_claims(hub_signer):
    token = hub_signer.token(scope="run", sub="ana", uid=7)

    claims = verify(token)

    assert isinstance(claims, HubClaims)
    assert (claims.subject, claims.user_id, claims.scope) == ("ana", 7, "run")


def test_a_dispatch_token_is_no_scope_this_node_knows(hub_signer):
    assert refusal(hub_signer.token(scope="dispatch", sub="hub", uid=None)) == "unknown scope"


def test_the_issuer_setting_names_the_iss_the_hub_signs_with(hub_signer, monkeypatch):
    signed_for_another_name = hub_signer.token(iss="http://localhost:63580")
    assert refusal(signed_for_another_name) == "wrong issuer"

    monkeypatch.setenv("FLOWFILE_HUB_ISSUER", "http://localhost:63580")

    assert verify(signed_for_another_name).scope == "run"
    assert refusal(hub_signer.token()) == "wrong issuer"


def test_a_token_for_another_node_is_refused(hub_signer):
    assert refusal(hub_signer.token(aud="other-node")) == "wrong audience"


def test_an_expired_token_is_refused_past_the_leeway(hub_signer):
    assert refusal(hub_signer.token(ttl=-120)) == "expired"
    assert verify(hub_signer.token(ttl=-10)).scope == "run"


def test_a_token_signed_with_a_key_the_hub_does_not_serve_is_refused(hub_signer):
    assert verify(hub_signer.token()).scope == "run"
    unserved = SigningKey()

    assert refusal(hub_signer.token(key=unserved)) == "unknown key"
    assert hub_signer.fetches >= 1


def test_a_token_with_a_served_kid_but_another_signature_is_refused(hub_signer):
    impostor = SigningKey(kid=hub_signer.key.kid)

    assert refusal(hub_signer.token(key=impostor)) == "bad signature"


def test_a_token_signed_with_a_shared_secret_is_refused_whatever_its_kid(hub_signer):
    import jwt

    claims = {
        "iss": hub_signer.url,
        "sub": "ana",
        "aud": NODE_NAME,
        "scope": "run",
        "jti": "x",
        "iat": int(time.time()),
        "exp": int(time.time()) + 60,
    }
    forged = jwt.encode(claims, "a shared secret", algorithm="HS256", headers={"kid": hub_signer.key.kid})

    assert refusal(forged) == "algorithm not allowed"


def test_an_unknown_scope_is_refused(hub_signer):
    assert refusal(hub_signer.token(scope="admin")) == "unknown scope"


@pytest.mark.parametrize("claim", ["exp", "iat", "sub", "scope", "jti"])
def test_a_missing_required_claim_is_refused_by_name(hub_signer, claim):
    assert refusal(hub_signer.token(drop=(claim,))) == f"missing claim {claim}"


def test_a_missing_audience_is_refused(hub_signer):
    assert refusal(hub_signer.token(drop=("aud",))) in ("missing claim aud", "wrong audience")


def test_a_string_that_is_not_a_jwt_is_not_a_hub_token_and_reads_no_settings(hub_signer, monkeypatch):
    def no_settings():
        raise AssertionError("the node's settings were read for a token that is not a JWT")

    monkeypatch.setattr(hub_node, "resolved", no_settings)

    assert verify("flowfile-test-internal-token") is None
    assert verify("") is None
    assert verify("one.dot") is None
    assert verify(None) is None


def test_hub_tokens_are_off_without_a_hub_url(hub_signer, monkeypatch):
    token = hub_signer.token()
    monkeypatch.delenv("FLOWFILE_HUB_URL")

    assert verify(token) is None


def test_a_hub_url_without_a_node_name_accepts_nothing_and_warns_once(hub_signer, monkeypatch, caplog):
    token = hub_signer.token()
    monkeypatch.delenv("FLOWFILE_NODE_NAME")
    monkeypatch.setattr(hub_tokens, "_warned_no_node_name", False)

    with caplog.at_level(logging.WARNING, logger="flowfile.hub_tokens"):
        assert verify(token) is None
        assert verify(token) is None

    warnings = [record for record in caplog.records if "FLOWFILE_NODE_NAME" in record.getMessage()]
    assert len(warnings) == 1


def test_the_key_client_follows_the_hub_url(hub_signer, monkeypatch):
    assert verify(hub_signer.token()).scope == "run"
    first_client = hub_tokens._client

    monkeypatch.setenv("FLOWFILE_HUB_URL", hub_signer.url + "/")
    assert verify(hub_signer.token()).scope == "run"
    assert hub_tokens._client is first_client, "a trailing slash is the same hub"

    monkeypatch.setenv("FLOWFILE_HUB_URL", "http://127.0.0.1:9/elsewhere")
    assert refusal(hub_signer.token(iss="http://127.0.0.1:9/elsewhere")) == "keys unavailable"
    assert hub_tokens._client is not first_client


def test_a_joined_node_takes_tokens_for_its_name(hub_signer, monkeypatch, tmp_path):
    monkeypatch.delenv("FLOWFILE_NODE_NAME")
    monkeypatch.delenv("FLOWFILE_HUB_URL")
    joined = tmp_path / "storage" / hub_node.FILE_NAME
    joined.parent.mkdir()
    joined.write_text(
        json.dumps(
            {
                "hub_url": hub_signer.url,
                "issuer": hub_signer.url,
                "name": "joined-node",
                "node_id": 4,
                "joined_at": "2026-11-03T09:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(hub_node, "path", lambda: joined)

    assert verify(hub_signer.token(aud="joined-node")).scope == "run"
    assert refusal(hub_signer.token(aud=NODE_NAME)) == "wrong audience"


def sign_in_token(hub_signer, **claims) -> str:
    """A sign-in token as the hub's Open page carries it: two minutes, for this node, with the person's name."""
    defaults = {"name": "Ana Lima", "node_admin": False, "may_act": {"sub": f"node:{NODE_NAME}"}}
    return hub_signer.token(scope="signin", sub="ana", uid=7, ttl=120, **{**defaults, **claims})


def test_a_sign_in_token_is_accepted_where_its_scope_is_named_with_the_persons_name(hub_signer):
    claims = verify(sign_in_token(hub_signer, node_admin=True), scopes=("signin",))

    assert (claims.subject, claims.user_id, claims.scope) == ("ana", 7, "signin")


def test_a_sign_in_token_is_refused_by_every_caller_that_names_no_scope(hub_signer):
    assert refusal(sign_in_token(hub_signer)) == "a signin token is not accepted here"


def test_a_run_token_is_refused_where_only_a_sign_in_is_taken(hub_signer):
    with pytest.raises(HubTokenRefused) as refused:
        verify(hub_signer.token(scope="run"), scopes=("signin",))
    assert refused.value.reason == "a run token is not accepted here"


def test_an_unknown_scope_is_refused_whatever_scopes_the_caller_names(hub_signer):
    with pytest.raises(HubTokenRefused) as refused:
        verify(hub_signer.token(scope="admin"), scopes=("admin",))
    assert refused.value.reason == "unknown scope"
