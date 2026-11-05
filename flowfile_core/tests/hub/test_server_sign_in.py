"""A server joins a hub with keys of its own, tells the hub it is up, and signs a person in through the hub for as long
as the hub keeps that sign-in, end to end against one fake hub.

The fake holds the hub's contract and nothing more (``shared.tests.hub_join.FakeJoinHub``): it signs with its own test
key and serves it, takes the join with the node's keys, and checks every node assertion against the key that join
sent. Here it also answers one route for a person, ``GET /hub/flows``, with the bearer it was sent when that bearer is
an access token it issued. Core runs in process through ``TestClient`` with its lifespan, in docker mode as a server
does: it joins as ``--join`` and ``--hub`` start it, beats from its own thread, and takes its requests. Its
``hub_node.json``, its keys and its secure store live under the test's ``tmp_path``; the person's account is in the
session's database and removed afterwards.
"""

import time
from collections import Counter

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import session
from shared import hub_node, hub_node_key
from shared._version import get_version
from shared.tests.hub_join import (
    HEARTBEAT_PATH,
    JOIN_PATH,
    REFRESH_GRANT,
    REVOKE_PATH,
    TOKEN_EXCHANGE,
    TOKEN_PATH,
    FakeJoinHub,
)
from shared.tests.hub_jwks import JWKS_PATH, HubSigner
from tests import test_hub_sign_in
from tests.test_hub_sign_in import ENDED, NODE, bearer

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username

FLOWS_PATH = "/hub/flows"
UID = 7


class PersonHub(FakeJoinHub):
    """``FakeJoinHub`` with one route for a person: ``GET /hub/flows`` answers the bearer it was sent, and whose it is,
    for an access token this hub issued, else 401.

    ``issued`` maps each access token the hub answered a token request with to the person's id, in the order issued;
    ``bearers`` keeps each bearer the route was sent; ``assertions`` each node assertion the hub checked, with the path
    it was sent to and the sentence refusing it (``None`` when it held).
    """

    def __init__(self, signer: HubSigner):
        super().__init__(signer)
        self.issued: dict[str, int] = {}
        self.bearers: list[str] = []
        self.assertions: list[tuple[str, str | None]] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_GET(self):
                if self.path != FLOWS_PATH:
                    super().do_GET()
                    return
                hub.calls.append((self.path, None))
                self._answer(*hub.flows(self.headers.get("Authorization")))

        self._server.RequestHandlerClass = Handler

    def flows(self, authorization: str | None) -> tuple[int, object]:
        scheme, _, token = (authorization or "").partition(" ")
        with self._lock:
            self.bearers.append(token)
            uid = self.issued.get(token) if scheme == "Bearer" else None
        if uid is None:
            return 401, {"detail": "this access token was not issued by the hub"}
        return 200, [{"bearer": token, "uid": uid}]

    def check_assertion(self, assertion: str, path: str) -> tuple[str | None, dict | None]:
        refusal, verified = super().check_assertion(assertion, path)
        with self._lock:
            self.assertions.append((path, refusal))
        return refusal, verified

    def _delegate(self, delegation: dict) -> dict:
        answer = super()._delegate(delegation)
        with self._lock:
            self.issued[answer["access_token"]] = delegation["uid"]
        return answer


@pytest.fixture
def hub(node_file):
    """A running ``PersonHub`` with a signer of its own; the accounts its people got here are removed afterwards."""
    signer = HubSigner()
    fake = PersonHub(signer)
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        with get_db_context() as db:
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [row.user_id for row in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


def core_joins(monkeypatch, hub: PersonHub) -> str:
    """Core's own join at start for itself and its worker, as ``--join`` and ``--hub`` give it; the token it spent."""
    token = hub.mint(enable_on_join=True)
    monkeypatch.setattr(main, "JOIN_TOKEN", token)
    monkeypatch.setattr(main, "JOIN_HUB_URL", hub.url)
    monkeypatch.setattr(main, "JOIN_NODE_NAME", NODE)
    monkeypatch.setattr(settings, "JOIN_CORE_URL", "http://team-1.example:63578")
    monkeypatch.setattr(settings, "JOIN_WORKER_URL", "http://team-1.example:63579")
    main._join_hub()
    return token


def sent_to(hub: PersonHub, path: str) -> list:
    return [body for sent, body in list(hub.calls) if sent == path]


def eventually(read, timeout: float = 10.0):
    """What ``read`` answers once it answers something, or after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while not (value := read()) and time.monotonic() < deadline:
        time.sleep(0.02)
    return value


def me(core: TestClient, access_token: str) -> dict:
    response = core.get("/auth/users/me", headers=bearer(access_token))
    assert response.status_code == 200, response.text
    return response.json()


def test_a_server_joins_with_its_keys_beats_and_signs_a_person_in_for_as_long_as_the_hub_keeps_it(
    monkeypatch, hub, username
):
    join_token = core_joins(monkeypatch, hub)

    keys = hub_node_key.load()
    [joined] = sent_to(hub, JOIN_PATH)
    assert joined["token"] == join_token and joined["keys"] == keys.public_jwks()
    assert hub_node.read().has_core is True

    with TestClient(main.app) as core:
        beat = eventually(lambda: list(hub.beats))[0]
        assert beat["header"]["kid"] == keys.sig_kid
        assert (beat["claims"]["sub"], beat["claims"]["aud"]) == (f"node:{NODE}", hub.issuer + HEARTBEAT_PATH)
        addresses = beat["body"].pop("addresses")
        assert beat["body"] == {"version": get_version(), "process": "core"}
        assert addresses and len({address.rsplit(":", 1)[1] for address in addresses}) == 1, addresses

        sign_in_token = hub.sign_in_token(NODE, uid=UID, username=username, full_name="Ana Lima")
        signed_in = core.post("/auth/hub/signin", json={"token": sign_in_token})
        assert signed_in.status_code == 200, signed_in.text
        pair = signed_in.json()
        person = me(core, pair["access_token"])
        assert (person["username"], person["full_name"]) == (username, "Ana Lima")
        [delegated] = hub.issued

        flows = core.get("/hub_client/flows", headers=bearer(pair["access_token"]))
        assert (flows.status_code, flows.json()) == (200, [{"bearer": delegated, "uid": UID}])

        replaced = session.load(person["id"])["refresh_token"]
        refreshed = core.post("/auth/refresh", data={"refresh_token": pair["refresh_token"]})
        assert refreshed.status_code == 200, refreshed.text
        renewed = refreshed.json()
        rotated = session.load(person["id"])["refresh_token"]
        assert rotated != replaced and hub.live_refresh_tokens() == [rotated]
        assert me(core, renewed["access_token"])["id"] == person["id"]

        assert core.post("/auth/logout", headers=bearer(renewed["access_token"])).status_code == 204
        [revoked] = sent_to(hub, REVOKE_PATH)
        assert revoked["token"] == rotated and hub.live_refresh_tokens() == []
        assert session.load(person["id"]) is None
        after_logout = core.post("/auth/refresh", data={"refresh_token": renewed["refresh_token"]})
        assert (after_logout.status_code, after_logout.json()) == (401, {"detail": ENDED})

        again = core.post("/auth/hub/signin", json={"token": hub.sign_in_token(NODE, uid=UID, username=username)})
        assert again.status_code == 200, again.text
        second = again.json()
        assert me(core, second["access_token"])["id"] == person["id"]
        hub.sign_out_everywhere(UID)
        after_hub_ended = core.post("/auth/refresh", data={"refresh_token": second["refresh_token"]})
        assert (after_hub_ended.status_code, after_hub_ended.json()) == (401, {"detail": ENDED})
        assert session.load(person["id"]) is None

    grants = [form["grant_type"] for form in sent_to(hub, TOKEN_PATH)]
    assert grants == [TOKEN_EXCHANGE, REFRESH_GRANT, TOKEN_EXCHANGE, REFRESH_GRANT]
    assert hub.live_refresh_tokens() == []
    assert all(refusal is None for _, refusal in hub.assertions)
    assert all(token in hub.issued for token in hub.bearers)
    with_credentials = Counter(path for path, _ in hub.calls if path not in (JOIN_PATH, JWKS_PATH))
    checked = Counter(path for path, _ in hub.assertions) + Counter(FLOWS_PATH for _ in hub.bearers)
    assert with_credentials == checked
