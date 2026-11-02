"""A worker that is a node on a hub takes the hub's tokens in ``X-Flowfile-Internal``, beside the internal token."""

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from flowfile_worker import main
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from shared.tests.hub_jwks import hub_signer  # noqa: F401
from tests.conftest import INTERNAL_AUTH_HEADERS, V1_HEADERS

client = TestClient(main.app)


def v1_headers(token: str) -> dict:
    return {"X-Flowfile-Internal": token, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}


@pytest.mark.worker
@pytest.mark.parametrize("scope", ["run", "dispatch"])
def test_a_hub_token_for_this_node_reaches_v1(hub_signer, scope):
    response = client.get("/v1/capabilities", headers=v1_headers(hub_signer.token(scope=scope)))

    assert response.status_code == 200
    assert response.json()["protocol_version"] == PROTOCOL_VERSION


@pytest.mark.worker
def test_the_internal_token_still_works_beside_hub_tokens(hub_signer):
    assert client.get("/v1/capabilities", headers=V1_HEADERS).status_code == 200
    assert client.get("/capabilities", headers=INTERNAL_AUTH_HEADERS).status_code == 200


@pytest.mark.worker
@pytest.mark.parametrize(
    ("make_token", "reason"),
    [
        (lambda hub: hub.token(ttl=-120), "expired"),
        (lambda hub: hub.token(aud="another-node"), "wrong audience"),
        (lambda hub: hub.token(scope="admin"), "unknown scope"),
    ],
    ids=["expired", "other node", "other scope"],
)
def test_a_refused_hub_token_is_401_naming_the_reason_and_not_the_token(hub_signer, make_token, reason):
    token = make_token(hub_signer)

    response = client.get("/v1/capabilities", headers=v1_headers(token))

    assert response.status_code == 401
    assert reason in response.json()["detail"]
    assert token not in response.text


@pytest.mark.worker
def test_a_hub_token_signed_with_an_unknown_key_is_401(hub_signer):
    from shared.tests.hub_jwks import SigningKey

    response = client.get("/v1/capabilities", headers=v1_headers(hub_signer.token(key=SigningKey())))

    assert response.status_code == 401
    assert "unknown key" in response.json()["detail"]


@pytest.mark.worker
def test_without_a_hub_url_a_hub_token_is_just_a_wrong_token(hub_signer, monkeypatch):
    token = hub_signer.token()
    monkeypatch.delenv("FLOWFILE_HUB_URL")

    response = client.get("/v1/capabilities", headers=v1_headers(token))

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing internal token"


@pytest.mark.worker
def test_the_submit_socket_takes_a_hub_token_and_closes_on_a_refused_one(hub_signer):
    with client.websocket_connect("/ws/submit", headers={"X-Flowfile-Internal": hub_signer.token()}):
        pass

    with pytest.raises(WebSocketDisconnect) as closed:
        with client.websocket_connect("/ws/submit", headers={"X-Flowfile-Internal": hub_signer.token(ttl=-120)}):
            pass
    assert closed.value.code == 1008
