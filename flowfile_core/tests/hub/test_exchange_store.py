"""A team node takes the exchange store its hub sets: when it asks, what wins over it, and how a run opens it.

The hub is the fake of ``tests/hub_client`` behind the client's http seam; the store's connection is a row in the
test database, made as the exchange-store tests make one, and no bucket is opened.
"""

import logging
import re
import uuid

import pytest
from sqlalchemy import func

from flowfile_core import events
from flowfile_core.configs import exchange_config, logger, settings
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRun
from flowfile_core.flowfile.execution.exchange import ExchangeRun, exchange_store
from flowfile_core.flowfile.execution.protocol import CredentialRef
from flowfile_core.hub import exchange_store as hub_exchange_store
from flowfile_core.hub.exchange_store import team_node_store
from flowfile_core.hub_client.client import HubRefused
from flowfile_core.routes import routes
from shared import hub_node
from shared.tests import hub_join
from tests.flowfile.conftest import add_test_manual_input, create_test_graph
from tests.flowfile.execution import test_exchange_store as exchange_tests
from tests.hub_client import conftest as hub_client_fakes
from tests.hub_client.conftest import HUB_URL

hub = hub_client_fakes.hub
join_hub = hub_join.join_hub
multi_user = exchange_tests.multi_user
users = exchange_tests.users

HUB_STORE = {
    "uri": "s3://org-bucket/exchange",
    "connection": "org exchange",
    "max_rows": 50000,
    "set_by": 1,
    "set_at": "2026-11-03T09:00:00Z",
}
FROM_HUB = ExchangeConfig(uri="s3://org-bucket/exchange", connection="org exchange", max_rows=50000)
ENV_VARS = ("FLOWFILE_EXCHANGE_URI", "FLOWFILE_EXCHANGE_CONNECTION", "FLOWFILE_EXCHANGE_MAX_ROWS")


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_exchange_store, "_warned", set())


@pytest.fixture
def team_node(monkeypatch, hub):
    """This core as a team node of the fake hub, which sets ``HUB_STORE``."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setattr(settings, "FLOWFILE_HUB_URL", HUB_URL)
    hub.exchange_store = dict(HUB_STORE)
    return hub


@pytest.fixture
def warnings(monkeypatch, caplog):
    """The warnings core logs during the test."""
    monkeypatch.setattr(logger, "propagate", True)
    caplog.set_level(logging.WARNING, logger=logger.name)

    def collected() -> list[logging.LogRecord]:
        return [r for r in caplog.records if r.name == logger.name and r.levelno == logging.WARNING]

    return collected


def test_a_team_node_takes_the_store_its_hub_sets(team_node):
    assert team_node_store("t") == FROM_HUB
    assert team_node.calls == [("GET", "/hub/exchange-store")]


def test_a_hub_store_without_a_limit_has_none(team_node):
    team_node.exchange_store = {**HUB_STORE, "max_rows": None}

    assert team_node_store("t") == ExchangeConfig(uri="s3://org-bucket/exchange", connection="org exchange")


@pytest.mark.parametrize("token", [None, ""])
def test_a_run_no_request_started_asks_nothing(team_node, token):
    assert team_node_store(token) is None
    assert team_node.calls == []


def test_a_desktop_asks_nothing(team_node, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")

    assert team_node_store("t") is None
    assert team_node.calls == []


def test_a_core_without_a_hub_url_asks_nothing(team_node, monkeypatch):
    monkeypatch.setattr(settings, "FLOWFILE_HUB_URL", "")

    assert team_node_store("t") is None
    assert team_node.calls == []


@pytest.fixture
def joined_with_keys(tmp_path, monkeypatch, join_hub):
    """This storage dir joined ``join_hub`` with keys of its own, as a server whose people sign in through its hub."""
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    hub_node.join(
        join_hub.url,
        join_hub.mint(enable_on_join=True),
        name="team-1",
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )


@pytest.mark.parametrize("hub_url", [HUB_URL, ""], ids=["hub-url-set", "hub-url-unset"])
def test_a_server_joined_with_its_keys_asks_its_hub_for_no_store(team_node, joined_with_keys, monkeypatch, hub_url):
    monkeypatch.setattr(settings, "FLOWFILE_HUB_URL", hub_url)

    assert team_node_store("a token this server signed") is None
    assert team_node.calls == []


def test_a_hub_that_sets_no_store_is_none(team_node, warnings):
    team_node.exchange_store = None

    assert team_node_store("t") is None
    assert team_node.calls == [("GET", "/hub/exchange-store")]
    assert warnings() == []


def test_a_hub_that_is_down_is_no_store_with_one_warning(team_node, warnings):
    team_node.down = True

    assert team_node_store("t") is None
    assert team_node_store("t") is None

    [warning] = warnings()
    assert HUB_URL in warning.getMessage() and "HubUnreachable" in warning.getMessage()
    assert len(team_node.calls) == 2


def test_a_hub_that_refuses_is_no_store_with_its_status_and_never_the_token(team_node, warnings, monkeypatch):
    token = "a-session-token-the-hub-refuses"

    def refuse(url: str, access_token: str):
        raise HubRefused(401, f"Could not validate credentials {access_token}")

    monkeypatch.setattr(hub_exchange_store.hub_client, "get_exchange_store", refuse)

    assert team_node_store(token) is None
    assert team_node_store(token) is None

    [warning] = warnings()
    assert "(401)" in warning.getMessage() and HUB_URL in warning.getMessage()
    assert token not in warning.getMessage()


def test_the_hubs_store_applies_while_nothing_is_saved():
    assert exchange_config.effective_store(hub=FROM_HUB) == ExchangeConfig(
        uri="s3://org-bucket/exchange", connection="org exchange"
    )
    assert exchange_config.effective_max_rows(hub=FROM_HUB) == 50000
    assert exchange_config.effective_store() == ExchangeConfig()
    assert exchange_config.effective_max_rows() is None


def test_a_store_saved_on_the_node_wins_over_the_hubs():
    exchange_config.save(ExchangeConfig(uri="s3://node-bucket/x", connection="node exchange", max_rows=7))

    assert exchange_config.effective_store(hub=FROM_HUB) == ExchangeConfig(
        uri="s3://node-bucket/x", connection="node exchange"
    )
    assert exchange_config.effective_max_rows(hub=FROM_HUB) == 7


def test_a_cleared_store_hands_the_node_back_to_the_hubs():
    exchange_config.save(ExchangeConfig(uri="s3://node-bucket/x", connection="node exchange", max_rows=7))
    exchange_config.save(ExchangeConfig())

    assert exchange_config.effective_store(hub=FROM_HUB).uri == "s3://org-bucket/exchange"
    assert exchange_config.effective_max_rows(hub=FROM_HUB) == 50000


def test_the_environment_wins_over_the_saved_store_and_the_hubs(monkeypatch):
    exchange_config.save(ExchangeConfig(uri="s3://node-bucket/x", connection="node exchange", max_rows=7))
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", "s3://pinned/y")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", "pinned connection")

    assert exchange_config.effective_store(hub=FROM_HUB) == ExchangeConfig(
        uri="s3://pinned/y", connection="pinned connection"
    )
    assert exchange_config.effective_max_rows(hub=FROM_HUB) == 7, "the limit stays the saved one, as without a hub"

    exchange_config.save(ExchangeConfig())

    assert exchange_config.effective_store(hub=FROM_HUB).uri == "s3://pinned/y"
    assert exchange_config.effective_max_rows(hub=FROM_HUB) is None


def test_the_row_limit_variable_wins_over_the_hubs_limit(monkeypatch):
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "10")

    assert exchange_config.effective_max_rows(hub=FROM_HUB) == 10
    assert ExchangeRun(1, "r", hub_store=FROM_HUB).max_rows() == 10
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "ten")
    with pytest.raises(ValueError, match="whole number"):
        ExchangeRun(1, "r", hub_store=FROM_HUB).max_rows()


def test_a_runs_row_limit_is_the_hubs_while_its_store_applies():
    assert ExchangeRun(1, "r", hub_store=FROM_HUB).max_rows() == 50000
    assert ExchangeRun(1, "r").max_rows() is None


def test_a_run_opens_the_hubs_store_through_the_administrators_connection_of_its_name(multi_user, users):
    name = f"org exchange {uuid.uuid4().hex[:6]}"
    exchange_tests._store_minio_connection(name, users["regular"])
    admin_row = exchange_tests._store_minio_connection(name, users["admin"])
    run = ExchangeRun(1, "r", hub_store=ExchangeConfig(uri="s3://org-bucket/exchange", connection=name))

    assert run.prefix() == (
        "s3://org-bucket/exchange/1/r/",
        CredentialRef(connection_id=admin_row, user_id=users["admin"]),
    )
    assert exchange_store() is None, "without the hub's store this node has none"


def test_a_hub_store_whose_connection_the_node_cannot_find_is_refused_as_a_local_one(multi_user, users):
    name = f"org exchange {uuid.uuid4().hex[:6]}"
    exchange_tests._store_minio_connection(name, users["regular"])
    hub_store = ExchangeConfig(uri="s3://org-bucket/exchange", connection=name)
    reason = f"No administrator has a cloud connection named {name!r}"

    with pytest.raises(ValueError, match=re.escape(reason)):
        exchange_store(hub_store)
    with pytest.raises(ValueError, match=re.escape(reason)):
        ExchangeRun(1, "r", hub_store=hub_store).prefix()


@pytest.fixture
def run_rows():
    """Removes the run records the test's runs write."""
    with get_db_context() as db:
        before = db.query(func.max(FlowRun.id)).scalar() or 0
    yield
    with get_db_context() as db:
        db.query(FlowRun).filter(FlowRun.id > before).delete()
        db.commit()


def _tracked_run(graph, hub_token: str | None) -> list[tuple]:
    """Run ``graph`` as the run route does, with ``hub_token`` as the request's; what the flow and its exchange run held
    for the hub's store when the run started."""
    seen: list[tuple] = []

    def record(graph, **_):
        seen.append((graph.hub_exchange_store, graph.exchange_run.hub_store))

    events.subscribe("flow_run_started", record)
    try:
        assert graph.try_claim_run()
        routes._run_and_track(graph, user_id=None, hub_token=hub_token)
    finally:
        events._handlers["flow_run_started"].remove(record)
    return seen


def test_a_run_on_a_team_node_holds_its_hubs_store_asked_once_with_the_runs_token(team_node, run_rows):
    graph = create_test_graph(flow_id=8542)
    add_test_manual_input(graph, [{"id": 1}])

    assert _tracked_run(graph, "t") == [(FROM_HUB, FROM_HUB)]
    assert team_node.calls == [("GET", "/hub/exchange-store")]
    assert graph.hub_exchange_store is None


def test_a_run_no_request_token_started_holds_no_hub_store(team_node, run_rows):
    graph = create_test_graph(flow_id=8543)
    add_test_manual_input(graph, [{"id": 1}])

    assert _tracked_run(graph, None) == [(None, None)]
    assert team_node.calls == []


def test_a_run_that_raises_leaves_no_hub_store_on_the_flow(team_node, run_rows, monkeypatch):
    graph = create_test_graph(flow_id=8544)
    held: list = []

    def run_graph(**_):
        held.append(graph.hub_exchange_store)
        raise RuntimeError("the run broke")

    monkeypatch.setattr(graph, "run_graph", run_graph)
    assert graph.try_claim_run()

    with pytest.raises(RuntimeError, match="the run broke"):
        routes._run_and_track(graph, user_id=None, hub_token="t")
    graph.release_run()

    assert held == [FROM_HUB]
    assert graph.hub_exchange_store is None
