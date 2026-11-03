"""A core joins a hub with ``--join`` and ``--hub`` for itself and its worker, once, and then knows its own node.

The join runs against ``FakeJoinHub`` over real HTTP; the sync through ``/hub_client`` against the fake hub of
``tests/hub_client``. ``hub_node.json`` lives under the test's ``tmp_path``, never in the storage dir.
"""

import json
import sys

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens, reachability
from flowfile_core.routes import hub_client as hub_client_routes
from shared import hub_node
from shared.tests import hub_join
from tests.hub_client import conftest as hub_client_fakes
from tests.hub_client.conftest import HUB_URL, compute_node

join_hub = hub_join.join_hub
hub = hub_client_fakes.hub
store = hub_client_fakes.store

VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")
CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    """Core's ``hub_node.json`` in a storage dir of the test's own, absent until a join or the test writes it."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    return target


@pytest.fixture
def no_variables(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


def joined_as(node_file, name: str, hub_url: str = HUB_URL) -> None:
    """The file a core leaves after joining ``hub_url`` as ``name``."""
    node_file.parent.mkdir(parents=True, exist_ok=True)
    node_file.write_text(
        json.dumps(
            {
                "hub_url": hub_url,
                "issuer": hub_url,
                "name": name,
                "node_id": 1,
                "joined_at": "2026-11-03T09:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )


def parse(monkeypatch, *flags) -> tuple:
    """``--join``, ``--hub``, ``--name``, ``--url`` and ``--worker-url`` as core reads them from ``flags``."""
    monkeypatch.setattr(sys, "argv", ["flowfile_core", *flags])
    args = settings.parse_args()
    return args.join, args.hub, args.name, args.url, args.worker_url


def started_with(
    monkeypatch,
    hub_url,
    token,
    *,
    name="team-1",
    core_url="http://team-1.example:63578",
    worker_url="http://team-1.example:63579",
):
    """The module values core's command line leaves behind, as ``settings`` reads them at import."""
    monkeypatch.setattr(main, "JOIN_TOKEN", token)
    monkeypatch.setattr(main, "JOIN_HUB_URL", hub_url)
    monkeypatch.setattr(main, "JOIN_NODE_NAME", name)
    monkeypatch.setattr(main, "JOIN_CORE_URL", core_url)
    monkeypatch.setattr(main, "JOIN_WORKER_URL", worker_url)


def test_the_join_flags_are_read(monkeypatch, no_variables):
    flags = ("--join", "t", "--hub", "http://h", "--name", "n", "--url", "http://c:1", "--worker-url", "http://w:2")

    assert parse(monkeypatch, *flags) == ("t", "http://h", "n", "http://c:1", "http://w:2")


def test_join_without_a_hub_is_refused_naming_the_flag(monkeypatch, no_variables):
    with pytest.raises(ValueError, match="--join needs --hub"):
        parse(monkeypatch, "--join", "t")


def test_the_join_token_and_hub_come_from_the_variables_alone(monkeypatch, no_variables):
    monkeypatch.setenv("FLOWFILE_HUB_JOIN_TOKEN", "t")
    monkeypatch.setenv("FLOWFILE_HUB_URL", "http://h")

    assert parse(monkeypatch) == ("t", "http://h", None, None, None)


def test_without_flags_or_variables_there_is_nothing_to_join(monkeypatch, no_variables):
    assert parse(monkeypatch) == (None, None, None, None, None)


def test_a_core_joins_for_itself_and_its_worker_and_a_restart_makes_no_request(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, join_hub.mint())

    main._join_hub()

    assert [path for path, _ in join_hub.calls] == ["/hub/nodes/join"]
    body = join_hub.calls[0][1]
    assert (body["name"], body["core_url"], body["worker_url"]) == (
        "team-1",
        "http://team-1.example:63578",
        "http://team-1.example:63579",
    )
    kept = json.loads(node_file.read_text(encoding="utf-8"))
    assert sorted(kept) == ["hub_url", "issuer", "joined_at", "name", "node_id"]
    assert kept["name"] == "team-1"

    main._join_hub()

    assert len(join_hub.calls) == 1


@pytest.mark.parametrize("served_on,core_port", [(None, 64100), (64200, 64200)], ids=["SERVER_PORT", "run's port"])
def test_a_core_given_no_addresses_gives_this_machines_on_its_two_ports(
    monkeypatch, join_hub, node_file, served_on, core_port
):
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "team-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "team-7.example.com")
    monkeypatch.setattr(main, "SERVER_PORT", 64100)
    monkeypatch.setattr(main, "WORKER_PORT", 64101)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), name=None, core_url=None, worker_url=None)

    main._join_hub(served_on)

    body = join_hub.calls[0][1]
    assert (body["name"], body["core_url"], body["worker_url"]) == (
        "team-7",
        f"http://team-7.example.com:{core_port}",
        "http://team-7.example.com:64101",
    )


def test_a_core_without_a_join_token_asks_nothing(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, None)

    main._join_hub()

    assert join_hub.calls == []
    assert not node_file.exists()


def test_a_refused_join_stops_core_with_exit_code_2_before_it_serves(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, "join-never-minted")
    served = []
    monkeypatch.setattr(main.compute_registry, "mark_serving", lambda: served.append(True))

    with pytest.raises(SystemExit) as exited:
        main.run()

    assert exited.value.code == 2
    assert served == []
    assert not node_file.exists()


def test_the_own_node_is_named_by_the_variable_as_before(monkeypatch, no_variables, node_file):
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "team-local")

    assert hub_client_routes._own_node_name() == "team-local"


def test_the_own_node_is_named_by_the_joined_file_with_the_variable_unset(no_variables, node_file):
    joined_as(node_file, "team-local")

    assert hub_client_routes._own_node_name() == "team-local"


def test_a_core_neither_named_nor_joined_is_no_node(no_variables, node_file):
    assert hub_client_routes._own_node_name() is None


@pytest.fixture
def api(store) -> TestClient:
    """A client signed in to core; the token is minted after the store is rebuilt, so it verifies against it."""
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


@pytest.fixture
def no_configured_backends(monkeypatch):
    """No node token or reachability check outlives the test, and every backends row it left is removed."""
    monkeypatch.setattr(node_tokens, "_tokens", {})
    monkeypatch.setattr(reachability, "check_all", lambda backend_ids: None)
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    handshake.reset_handshake()
    reachability.clear()


def _row_ids() -> list[str]:
    with get_db_context() as db:
        return sorted(row.id for row in db.query(ConfiguredBackend).all())


def test_a_joined_core_never_lists_its_own_node_and_drops_a_row_written_before(
    api, hub, no_variables, node_file, no_configured_backends
):
    hub.nodes = [compute_node(1, "team-local"), compute_node(2, "gpu")]
    api.post("/hub_client/connection", json=CONNECT)
    assert _row_ids() == ["worker:gpu", "worker:team-local"]
    joined_as(node_file, "team-local")

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 200, response.text
    assert response.json()["skipped"] == []
    assert [row["id"] for row in response.json()["backends"]] == ["worker:gpu"]
    assert _row_ids() == ["worker:gpu"]
    reconnected = api.post("/hub_client/connection", json=CONNECT)
    assert reconnected.json()["nodes_synced"] == 1
    assert reconnected.json()["node_name"] == "team-local"
    assert _row_ids() == ["worker:gpu"]
