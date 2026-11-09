"""A ``compute_backends`` row filled from a hub makes a ``WorkerBackend`` that signs with the token the hub mints for
its node, through the row's user's hub session; without that session the lookup says to connect to the hub again."""

import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client, node_tokens, session
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from tests.hub_client.conftest import HUB_URL, compute_node, hub, store  # noqa: F401

USER_ID = 41
TEAM = compute_node(3, "team")


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    registry.reset()
    monkeypatch.setattr(node_tokens, "_tokens", {})
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            db.delete(row)
        db.commit()
    registry.reset()
    handshake.reset_handshake()


@pytest.fixture
def hub_row(store):
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:team", url=TEAM["worker_url"], remote_id=TEAM["id"], account_user_id=USER_ID))
        db.commit()


@pytest.fixture
def connected(hub, store):
    hub.nodes = [TEAM]
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(
        USER_ID, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_name": "laptop"}
    )
    hub.calls.clear()
    return hub


def test_a_hub_filled_row_resolves_to_a_backend_signing_with_the_cached_token(hub_row, connected):
    token = node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    connected.calls.clear()

    backend = registry.backend("worker:team")

    assert isinstance(backend, WorkerBackend)
    assert backend.id == "worker:team"
    assert backend.url == TEAM["worker_url"]
    assert backend.token == token
    assert connected.calls == []


def test_the_first_lookup_mints_the_token_and_the_next_reuses_the_backend(hub_row, connected):
    backend = registry.backend("worker:team")

    assert connected.minted == [{"user": "ana", "node_id": TEAM["id"], "scope": "run"}]
    assert backend.token == node_tokens.token_for(USER_ID, TEAM["id"], TEAM["name"])
    assert registry.backend("worker:team") is backend
    assert len(connected.minted) == 1


def test_a_token_replaced_near_expiry_makes_the_backend_again_with_the_new_one(hub_row, connected):
    connected.node_token_ttl = 30
    first = registry.backend("worker:team")

    second = registry.backend("worker:team")

    assert second is not first
    assert isinstance(second, WorkerBackend)
    assert second.token != first.token
    assert second.url == first.url
    assert len(connected.minted) == 2


def test_without_a_hub_session_the_lookup_says_to_connect_to_the_hub_again(hub_row, hub):
    with pytest.raises(LookupError, match="'worker:team'.*connect to the hub again"):
        registry.backend("worker:team")
    assert hub.calls == []


def test_a_session_cleared_after_the_first_use_fails_the_next_lookup(hub_row, connected):
    registry.backend("worker:team")

    session.clear(USER_ID)

    with pytest.raises(LookupError, match="connect to the hub again"):
        registry.backend("worker:team")


def test_a_row_added_by_hand_still_signs_with_its_own_token(store):
    with get_db_context() as db:
        db.add(
            ConfiguredBackend(
                id="worker:own", url="https://own.example.test:63579", token=encrypt_secret("row-token", 1)
            )
        )
        db.commit()

    backend = registry.backend("worker:own")

    assert isinstance(backend, WorkerBackend)
    assert backend.token == "row-token"
    assert registry.backend("worker:own") is backend
