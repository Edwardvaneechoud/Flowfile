"""Tests for the OAuth ``state`` JWT used by the GA connections routes.

The state is how the unauthenticated callback route proves the request
originated from our start endpoint and knows which user + connection the
callback applies to. A tampered state must be rejected.
"""

from __future__ import annotations

import time

import pytest
from fastapi import HTTPException

from flowfile_core.routes.ga_connections import _sign_oauth_state, _verify_oauth_state


def test_state_roundtrip() -> None:
    token = _sign_oauth_state(
        user_id=7,
        connection_name="ga-x",
        description="d",
        default_property_id="123",
    )
    payload = _verify_oauth_state(token)
    assert payload["user_id"] == 7
    assert payload["connection_name"] == "ga-x"
    assert payload["description"] == "d"
    assert payload["default_property_id"] == "123"


def test_tampered_state_rejected() -> None:
    token = _sign_oauth_state(
        user_id=1,
        connection_name="ga-y",
        description=None,
        default_property_id=None,
    )
    # Flip a character in the signature — the JWT library should reject it.
    parts = token.rsplit(".", 1)
    tampered = parts[0] + "." + ("A" if parts[1][0] != "A" else "B") + parts[1][1:]
    with pytest.raises(HTTPException) as exc:
        _verify_oauth_state(tampered)
    assert exc.value.status_code == 400


def test_wrong_type_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """A JWT signed with the same secret but a different ``type`` claim must
    not be accepted as a GA OAuth state (guards against token confusion with
    the regular access/refresh tokens)."""
    from jose import jwt

    from flowfile_core.auth.jwt import get_jwt_secret
    from flowfile_core.configs.settings import ALGORITHM

    token = jwt.encode(
        {"type": "access", "user_id": 1, "exp": int(time.time()) + 60},
        get_jwt_secret(),
        algorithm=ALGORITHM,
    )
    with pytest.raises(HTTPException) as exc:
        _verify_oauth_state(token)
    assert "wrong type" in exc.value.detail
