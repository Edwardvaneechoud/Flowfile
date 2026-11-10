"""A machine signed in to a hub takes the exchange store the hub sets, a server joined with keys of its own and a
desktop connected to it alike: when it asks, what wins over it, and how a run opens it.

The hub is the fake of ``tests/hub_client`` behind the client's http seam, and no bucket is opened.
"""

import logging
import re

import pytest
from sqlalchemy import func

from flowfile_core.configs import exchange_config, logger
from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRun
from flowfile_core.flowfile.execution.exchange import ExchangeRun
from flowfile_core.hub_client import exchange_store as hub_exchange_store
from flowfile_core.hub_client.client import HubRefused
from flowfile_core.routes import routes
from shared import hub_node
from shared.tests import hub_join
from tests.flowfile.conftest import add_test_manual_input, create_test_graph
from tests.hub_client import conftest as hub_client_fakes
from tests.hub_client.conftest import HUB_URL

hub = hub_client_fakes.hub
join_hub = hub_join.join_hub

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
def joined_with_keys(tmp_path, monkeypatch, join_hub):
    """This storage dir joined ``join_hub`` with keys of its own, as a server whose people sign in through its hub."""
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    hub_node.join(
        join_hub.url,
        join_hub.mint(enable_on_join=True),
        name="team-1",
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )


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


@pytest.fixture
def run_rows():
    """Removes the run records the test's runs write."""
    with get_db_context() as db:
        before = db.query(func.max(FlowRun.id)).scalar() or 0
    yield
    with get_db_context() as db:
        db.query(FlowRun).filter(FlowRun.id > before).delete()
        db.commit()


def test_a_run_that_raises_leaves_no_hub_store_on_the_flow(run_rows, monkeypatch):
    graph = create_test_graph(flow_id=8544)
    held: list = []
    vended = ExchangeConfig(uri=HUB_STORE["uri"], connection="org exchange", vended_for=1)
    monkeypatch.setattr(routes, "_hub_store_for", lambda flow, user_id: vended)

    def run_graph(**_):
        held.append(graph.hub_exchange_store)
        raise RuntimeError("the run broke")

    monkeypatch.setattr(graph, "run_graph", run_graph)
    assert graph.try_claim_run()

    with pytest.raises(RuntimeError, match="the run broke"):
        routes._run_and_track(graph, user_id=1)
    graph.release_run()

    assert held == [vended]
    assert graph.hub_exchange_store is None


store = hub_client_fakes.store


@pytest.fixture
def signed_in(store, hub):
    """User 1 has a hub session on the fake hub, which sets ``HUB_STORE``."""
    from flowfile_core.hub_client import client, session

    hub.exchange_store = dict(HUB_STORE)
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(1, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"]})
    hub.calls.clear()
    return hub


def test_a_server_joined_with_its_keys_takes_its_hubs_store_through_the_persons_hub_session(
    signed_in, joined_with_keys
):
    taken = hub_exchange_store.hub_store(1)

    assert taken == ExchangeConfig(uri=HUB_STORE["uri"], connection="org exchange", max_rows=50000, vended_for=1)
    assert ("GET", "/hub/exchange-store") in signed_in.calls


def test_a_joined_server_asks_nothing_for_no_user_or_a_person_without_a_hub_session(signed_in, joined_with_keys):
    assert hub_exchange_store.hub_store(None) is None
    assert hub_exchange_store.hub_store(2) is None
    assert ("GET", "/hub/exchange-store") not in signed_in.calls


def test_a_hub_that_refuses_is_no_store_with_one_warning_naming_its_status_and_never_the_token(
    signed_in, joined_with_keys, monkeypatch, caplog
):
    monkeypatch.setattr(logger, "propagate", True)
    caplog.set_level(logging.WARNING, logger=logger.name)
    sent: list[str] = []

    def refuse(url: str, access_token: str):
        sent.append(access_token)
        raise HubRefused(401, f"Could not validate credentials {access_token}")

    monkeypatch.setattr(hub_exchange_store.hub_client, "get_exchange_store", refuse)

    assert hub_exchange_store.hub_store(1) is None
    assert hub_exchange_store.hub_store(1) is None

    [message] = {r.getMessage() for r in caplog.records if r.name == logger.name and r.levelno == logging.WARNING}
    assert len(sent) == 2
    assert "(401)" in message and HUB_URL in message
    assert sent[0] not in message


def test_a_desktop_connected_to_the_hub_takes_its_store_as_a_joined_server_does(signed_in, tmp_path, monkeypatch):
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    assert hub_node.joined() is None

    taken = hub_exchange_store.hub_store(1)

    assert taken == ExchangeConfig(uri=HUB_STORE["uri"], connection="org exchange", max_rows=50000, vended_for=1)
    assert ("GET", "/hub/exchange-store") in signed_in.calls


def test_the_hubs_store_on_a_joined_server_is_opened_by_what_the_hub_vends_the_run():
    from flowfile_core.flowfile.execution.exchange import NOT_VENDED
    from flowfile_core.flowfile.execution.protocol import SealedCredential

    hub_store = ExchangeConfig(uri="s3://org-bucket/exchange/", connection="org exchange", vended_for=1)
    run = ExchangeRun(7, "r1", hub_store=hub_store)

    assert run.vended()
    with pytest.raises(ValueError, match=re.escape(NOT_VENDED)):
        run.prefix()
    run.sealed = SealedCredential(base_uri="s3://org-bucket/exchange", sealed={"team-1": "t", "worker": "u"})
    assert run.prefix() == ("s3://org-bucket/exchange/7/r1/", run.sealed)
    assert run.max_rows() is None


def test_a_store_saved_on_a_joined_server_wins_over_the_vended_one(monkeypatch, tmp_path):
    monkeypatch.setattr(exchange_config, "_settings_file", lambda: tmp_path / "exchange_store.yaml")
    exchange_config.save(ExchangeConfig(uri="s3://own-bucket/x", connection="own"))
    run = ExchangeRun(7, "r1", hub_store=ExchangeConfig(uri="s3://org-bucket/exchange", vended_for=1))

    assert not run.vended()


def test_a_joined_servers_run_asks_for_the_store_only_for_a_flow_with_a_node_set_elsewhere(signed_in, joined_with_keys):
    graph = create_test_graph(flow_id=8401, execution_location="local")
    add_test_manual_input(graph, [{"id": 1}], node_id=1)

    assert routes._hub_store_for(graph, 1) is None
    assert signed_in.calls == []

    graph.get_node(1).setting_input.execution_location = "worker:elsewhere"
    assert routes._hub_store_for(graph, 1).vended_for == 1


def test_the_exchange_card_shows_a_joined_servers_hub_store_as_the_hubs(signed_in, joined_with_keys):
    from types import SimpleNamespace

    from flowfile_core.routes import system_exchange

    state = system_exchange._state(system_exchange._hub_store(SimpleNamespace(id=1)))

    assert (state.source, state.uri) == ("hub", HUB_STORE["uri"])
    assert "vended" not in state.model_dump()
