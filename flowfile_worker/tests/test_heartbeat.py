"""A worker that joined a hub alone tells it the node is up while it serves; one beside a core that joined stays quiet.

The worker's lifespan runs here without its pool, its children and its storage sweep, which the other tests in this
process share; the heartbeat it starts is the real one, against ``FakeJoinHub``, with the interval shortened.
"""

import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main, viz_sessions
from shared import hub_node
from shared._version import get_version
from shared.tests import hub_join
from shared.tests.hub_join import HEARTBEAT_PATH

pytestmark = pytest.mark.worker

join_hub = hub_join.join_hub

QUIET = 0.6


@pytest.fixture(autouse=True)
def node_file(tmp_path, monkeypatch):
    """The worker's storage dir is the test's own; no warning was spent before the test; a beat follows every 0.2 s; no
    earlier test's ``run`` left a serving port behind."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    monkeypatch.setattr(hub_node, "_warned_beats", set())
    monkeypatch.setattr(hub_node, "HEARTBEAT_SECONDS", 0.2)
    monkeypatch.setattr(main, "serving_port", None)
    return target


@pytest.fixture
def serving(monkeypatch):
    """Enters the worker's lifespan with its pool, its children and its storage sweep left alone."""
    monkeypatch.setattr(main, "task_pool", SimpleNamespace(prewarm=lambda: None, shutdown=lambda: None))
    monkeypatch.setattr(main, "mp_context", SimpleNamespace(active_children=lambda: []))
    monkeypatch.setattr(main, "storage", SimpleNamespace(cleanup_directories=lambda: None))
    monkeypatch.setattr(viz_sessions, "viz_session_registry", SimpleNamespace(shutdown=lambda: None))
    return lambda: TestClient(main.app)


def test_a_worker_that_joined_alone_beats_with_the_key_it_joined_with(monkeypatch, join_hub, serving):
    monkeypatch.setattr(main, "JOIN_TOKEN", join_hub.mint(enable_on_join=True))
    monkeypatch.setattr(main, "HUB_URL", join_hub.url)
    monkeypatch.setattr(main, "NODE_NAME", "worker-1")
    monkeypatch.setattr(main, "NODE_URL", "http://worker-1.example:63579")
    main.join_hub()
    sent = join_hub.calls[0][1]["keys"]["sig"]

    with serving():
        join_hub.wait_for_beats(2)

    first, second = join_hub.beats[:2]
    for beat in (first, second):
        assert beat["header"]["kid"] == sent["kid"]
        assert (beat["claims"]["sub"], beat["claims"]["aud"]) == ("node:worker-1", join_hub.url + HEARTBEAT_PATH)
        assert beat["claims"]["exp"] - beat["claims"]["iat"] == 60
        assert beat["body"] == {"version": get_version(), "process": "worker"}


def test_a_serving_worker_names_its_addresses_on_its_port_in_every_beat(monkeypatch, join_hub, serving):
    monkeypatch.setattr(main, "JOIN_TOKEN", join_hub.mint(enable_on_join=True))
    monkeypatch.setattr(main, "HUB_URL", join_hub.url)
    monkeypatch.setattr(main, "NODE_NAME", "worker-1")
    monkeypatch.setattr(main, "NODE_URL", "https://worker-1.example:64000")
    monkeypatch.setenv("FLOWFILE_WORKER_TLS_CERT", "/certs/worker.pem")
    monkeypatch.setattr(main, "serving_port", 64000)
    asked: list[tuple] = []

    def addresses_now(port, *, tls, hub_url):
        asked.append((port, tls, hub_url))
        return [f"https://10.0.0.{len(asked)}:{port}"]

    monkeypatch.setattr(hub_node, "reach_addresses", addresses_now)
    main.join_hub()

    with serving():
        join_hub.wait_for_beats(2)

    assert asked[:2] == [(64000, True, join_hub.url)] * 2
    assert [beat["body"]["addresses"] for beat in join_hub.beats[:2]] == [
        ["https://10.0.0.1:64000"],
        ["https://10.0.0.2:64000"],
    ]


def test_a_worker_beside_a_core_that_joined_for_both_does_not_beat(join_hub, serving):
    token = join_hub.mint(enable_on_join=True)
    hub_node.join(
        join_hub.url,
        token,
        name="team-1",
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )

    with serving():
        time.sleep(QUIET)

    assert join_hub.sent_beats() == []
