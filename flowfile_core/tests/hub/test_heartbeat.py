"""A core joined to a hub tells it that the node is up while it serves: at start, then every interval, until it stops.

The core joins ``FakeJoinHub`` with ``--join`` and ``--hub`` as it does at start, then runs its lifespan, which beats
from a thread. The interval is shortened here so a second beat comes within the test; the 60 seconds themselves are
pinned in ``shared/tests/test_hub_node.py``. ``hub_node.json`` and the keys live under the test's ``tmp_path``.
"""

import logging
import time

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings
from shared import hub_node, hub_node_key
from shared._version import get_version
from shared.tests import hub_join
from shared.tests.hub_join import HEARTBEAT_PATH, FakeJoinHub

join_hub = hub_join.join_hub

QUIET = 0.6
VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME")


@pytest.fixture(autouse=True)
def node_file(tmp_path, monkeypatch):
    """Core's storage dir is the test's own; no warning is spent before the test and a beat follows every 0.2 s."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(hub_node, "_warned_beats", set())
    monkeypatch.setattr(hub_node, "HEARTBEAT_SECONDS", 0.2)
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    return target


def core_joins(monkeypatch, hub: FakeJoinHub, *, enabled: bool = True) -> dict:
    """Core's own join for itself and its worker, as ``--join`` and ``--hub`` start it; the ``sig`` JWK it sent."""
    monkeypatch.setattr(main, "JOIN_TOKEN", hub.mint(enable_on_join=enabled))
    monkeypatch.setattr(main, "JOIN_HUB_URL", hub.url)
    monkeypatch.setattr(main, "JOIN_NODE_NAME", "team-1")
    monkeypatch.setattr(settings, "JOIN_CORE_URL", "http://team-1.example:63578")
    monkeypatch.setattr(settings, "JOIN_WORKER_URL", "http://team-1.example:63579")
    main._join_hub()
    return hub.calls[-1][1]["keys"]["sig"]


def warnings_of(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == "flowfile.hub_node" and r.levelno == logging.WARNING]


def test_a_joined_core_beats_at_start_and_again_with_the_key_it_joined_with(monkeypatch, join_hub):
    sent = core_joins(monkeypatch, join_hub)

    with TestClient(main.app):
        join_hub.wait_for_beats(2)

    first, second = join_hub.beats[:2]
    for beat in (first, second):
        assert beat["header"]["kid"] == sent["kid"]
        assert (beat["claims"]["sub"], beat["claims"]["aud"]) == ("node:team-1", join_hub.url + HEARTBEAT_PATH)
        assert beat["claims"]["exp"] - beat["claims"]["iat"] == 60
        assert beat["body"] == {"version": get_version(), "process": "core"}
    assert first["claims"]["jti"] != second["claims"]["jti"]


def test_a_core_stops_beating_when_it_stops(monkeypatch, join_hub):
    core_joins(monkeypatch, join_hub)

    with TestClient(main.app):
        join_hub.wait_for_beats(1)
    time.sleep(QUIET)
    stopped_at = len(join_hub.sent_beats())
    time.sleep(QUIET)

    assert len(join_hub.sent_beats()) == stopped_at


def test_a_core_the_hub_refuses_warns_once_while_it_keeps_beating(monkeypatch, join_hub, caplog):
    core_joins(monkeypatch, join_hub, enabled=False)

    with caplog.at_level(logging.DEBUG):
        with TestClient(main.app):
            join_hub.wait_for_beats(3)

    assert len(join_hub.sent_beats()) >= 3 and join_hub.beats == []
    assert warnings_of(caplog) == [
        f"The hub did not take this node's heartbeat: {join_hub.url} answered 401: this node is disabled on the hub"
    ]
    assert "eyJ" not in caplog.text



def test_a_core_that_never_joined_a_hub_sends_nothing(join_hub, node_file, caplog):
    with TestClient(main.app):
        time.sleep(QUIET)

    assert join_hub.calls == []
    assert warnings_of(caplog) == []
    assert not node_file.parent.exists()
