"""The run report core posts to a hub: its shape, the post, and the hook in the designer's run path."""

import json
import logging
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable, FlowRegistration, FlowRun
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.hub import report
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA,
    add_test_catalog_writer,
    add_test_manual_input,
    catalog_cleanup,
    create_test_graph,
    create_test_namespace,
    run_test_graph,
)

HUB_URL = "http://hub.test"
REPORT_KEYS = {
    "run_id",
    "flow_uuid",
    "flow_version",
    "compute_node_id",
    "started_at",
    "ended_at",
    "success",
    "nodes",
    "inputs",
    "outputs",
    "vend_ids",
    "core_run_id",
}
NODE_KEYS = {"node_id", "node_type", "success", "skipped", "duration_seconds", "rows_out", "error"}
TABLE_KEYS = {"namespace_id", "table_id", "table_name"}
FLOW_UUID = "0b1c2d3e-0000-4000-8000-0000000000aa"


@pytest.fixture(autouse=True)
def clean_catalog():
    catalog_cleanup()
    yield
    with get_db_context() as db:
        db.query(FlowRun).delete()
        db.commit()
    catalog_cleanup()


@pytest.fixture
def hub(monkeypatch):
    """A hub behind an ``httpx.MockTransport``; ``hub.answer`` sets what it does with each request."""

    class Hub:
        requests: list[httpx.Request] = []
        answer = staticmethod(lambda request: httpx.Response(201, json={}))

    hub = Hub()
    hub.requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        hub.requests.append(request)
        return hub.answer(request)

    real_client = httpx.Client
    monkeypatch.setattr(
        report.httpx, "Client", lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs)
    )
    return hub


@pytest.fixture
def warnings(monkeypatch, caplog):
    """The warnings core logs during the test."""
    monkeypatch.setattr(report.logger, "propagate", True)
    caplog.set_level(logging.WARNING, logger=report.logger.name)

    def collected() -> list[logging.LogRecord]:
        return [r for r in caplog.records if r.name == report.logger.name and r.levelno == logging.WARNING]

    return collected


def _table_ref(namespace_id: int, table_name: str) -> dict:
    with get_db_context() as db:
        table = db.query(CatalogTable).filter_by(name=table_name, namespace_id=namespace_id).one()
        return {"namespace_id": namespace_id, "table_id": table.id, "table_name": table_name}


def _add_reader(graph, node_id: int, **settings) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader"))
    graph.add_catalog_reader(input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=node_id, **settings))


def _assert_contract_shape(built: dict) -> None:
    assert set(built) == REPORT_KEYS
    assert all(set(node) == NODE_KEYS for node in built["nodes"])
    assert all(set(ref) == TABLE_KEYS for ref in built["inputs"] + built["outputs"])


class TestBuildRunReport:
    def test_a_run_reports_its_nodes_and_the_table_it_wrote_without_any_data(self):
        ns_id = create_test_namespace()
        graph = create_test_graph(flow_id=8101)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="hub_people", namespace_id=ns_id)
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, core_run_id=41, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        uuid.UUID(built["run_id"])
        assert built["flow_uuid"] == FLOW_UUID
        assert built["core_run_id"] == 41
        assert (built["flow_version"], built["compute_node_id"], built["vend_ids"]) == (None, None, [])
        assert built["success"] is True
        assert built["started_at"] <= built["ended_at"]
        nodes = {node["node_id"]: node for node in built["nodes"]}
        assert nodes[1]["node_type"] == "manual_input"
        assert nodes[1]["rows_out"] == 3
        assert nodes[2]["node_type"] == "catalog_writer"
        assert all(node["success"] and node["error"] is None for node in nodes.values())
        assert all(node["duration_seconds"] is None or node["duration_seconds"] >= 0 for node in nodes.values())
        assert built["inputs"] == []
        assert built["outputs"] == [_table_ref(ns_id, "hub_people")]

        text = json.dumps(built)
        for value in ("Alice", "Amsterdam", "Copenhagen"):
            assert value not in text
        for key in ("plan", "sample", "schema", "columns"):
            assert f'"{key}"' not in text

    def test_catalog_readers_and_a_writer_by_full_namespace_name_resolve_to_table_ids(self):
        ns_id = create_test_namespace()
        seed = create_test_graph(flow_id=8102)
        add_test_manual_input(seed, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(seed, node_id=2, depending_on_id=1, table_name="hub_source", namespace_id=ns_id)
        run_test_graph(seed)
        source = _table_ref(ns_id, "hub_source")

        graph = create_test_graph(flow_id=8103)
        _add_reader(graph, 1, catalog_table_id=source["table_id"])
        _add_reader(graph, 3, catalog_full_table_name="TestSch.hub_source")
        graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="catalog_writer"))
        graph.add_catalog_writer(
            input_schema.NodeCatalogWriter(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                catalog_write_settings=input_schema.CatalogWriteSettings(
                    table_name="hub_copy", namespace_full_name="TestCat.TestSch"
                ),
                user_id=1,
            )
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, core_run_id=42, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert built["inputs"] == [source]
        assert built["outputs"] == [_table_ref(ns_id, "hub_copy")]
        assert {node["node_type"] for node in built["nodes"]} == {"catalog_reader", "catalog_writer"}

    def test_a_failed_writer_reports_its_error_and_no_written_table(self):
        ns_id = create_test_namespace()
        graph = create_test_graph(flow_id=8104)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="", namespace_id=ns_id)
        run_info = graph.run_graph()

        built = report.build_run_report(graph, run_info, core_run_id=43, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert built["success"] is False
        writer = next(node for node in built["nodes"] if node["node_id"] == 2)
        assert writer["success"] is False
        assert "table name" in writer["error"]
        assert built["outputs"] == []


class TestPostRunReport:
    REPORT = {"run_id": "r-1", "flow_uuid": FLOW_UUID, "core_run_id": 7, "nodes": []}

    def test_the_report_is_posted_to_the_hub_with_the_bearer_token(self, hub, warnings):
        report.post_run_report(f"{HUB_URL}/", "tok-123", self.REPORT)

        assert len(hub.requests) == 1
        sent = hub.requests[0]
        assert (sent.method, str(sent.url)) == ("POST", f"{HUB_URL}/hub/runs")
        assert sent.headers["Authorization"] == "Bearer tok-123"
        assert json.loads(sent.content) == self.REPORT
        assert warnings() == []

    @pytest.mark.parametrize(
        "answer",
        [
            lambda request: httpx.Response(403, json={"detail": "not allowed"}),
            lambda request: (_ for _ in ()).throw(httpx.ReadTimeout("timed out", request=request)),
            lambda request: (_ for _ in ()).throw(httpx.ConnectError("connection refused", request=request)),
        ],
        ids=["refuses", "times-out", "down"],
    )
    def test_a_hub_that_fails_costs_one_warning_and_never_raises(self, hub, warnings, answer):
        hub.answer = answer

        report.post_run_report(HUB_URL, "tok-123", self.REPORT)

        assert len(hub.requests) == 1
        assert len(warnings()) == 1
        assert "run 7" in warnings()[0].getMessage()


def _token() -> str:
    with TestClient(main.app) as client:
        return client.post("/auth/token").json()["access_token"]


@pytest.fixture
def registered_flow(tmp_path):
    """A registered flow with one manual input, open in the designer: ``(flow_id, flow_uuid)``."""
    flow_path = tmp_path / "hub_reported_flow.yaml"
    flow_id = routes.flow_file_handler.add_flow(name="hub_reported_flow", flow_path=str(flow_path), user_id=1)
    flow = routes.flow_file_handler.get_flow(flow_id)
    flow.flow_settings.execution_location = "local"
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
    flow.save_flow(str(flow_path))
    auto_register_flow(str(flow_path), "hub_reported_flow", 1)
    with get_db_context() as db:
        flow_uuid = db.query(FlowRegistration.flow_uuid).filter_by(flow_path=str(flow_path)).scalar()
    yield flow_id, flow_uuid
    routes.flow_file_handler.delete_flow(flow_id)


def _runs_of(flow_uuid: str) -> list[FlowRun]:
    with get_db_context() as db:
        runs = db.query(FlowRun).filter_by(flow_uuid=flow_uuid).all()
        db.expunge_all()
        return runs


class TestRunHook:
    def test_a_run_started_with_a_token_posts_one_report_linked_to_its_run_row(
        self, hub, warnings, registered_flow, monkeypatch
    ):
        monkeypatch.setattr(routes, "FLOWFILE_HUB_URL", HUB_URL)
        flow_id, flow_uuid = registered_flow
        token = _token()

        with TestClient(main.app) as client:
            response = client.post(
                "/flow/run/", params={"flow_id": flow_id}, headers={"Authorization": f"Bearer {token}"}
            )
        assert response.status_code == 200, response.text

        [run] = _runs_of(flow_uuid)
        assert run.success is True and run.ended_at is not None
        hub_posts = [r for r in hub.requests if r.url.host == "hub.test"]
        assert len(hub_posts) == 1
        assert hub_posts[0].headers["Authorization"] == f"Bearer {token}"
        sent = json.loads(hub_posts[0].content)
        _assert_contract_shape(sent)
        assert (sent["flow_uuid"], sent["core_run_id"], sent["success"]) == (flow_uuid, run.id, True)
        assert [node["node_type"] for node in sent["nodes"]] == ["manual_input"]
        assert warnings() == []

    def test_a_hub_that_is_down_leaves_the_run_and_its_row_intact(self, hub, warnings, registered_flow, monkeypatch):
        monkeypatch.setattr(routes, "FLOWFILE_HUB_URL", HUB_URL)
        hub.answer = lambda request: (_ for _ in ()).throw(httpx.ConnectError("refused", request=request))
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1, hub_token="tok-123")

        [run] = _runs_of(flow_uuid)
        assert run.success is True and run.ended_at is not None and run.nodes_completed == 1
        assert len(hub.requests) == 1
        assert len(warnings()) == 1
        assert not flow.flow_settings.is_running

    def test_with_no_hub_url_nothing_is_posted(self, hub, warnings, registered_flow, monkeypatch):
        monkeypatch.setattr(routes, "FLOWFILE_HUB_URL", "")
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1, hub_token="tok-123")

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert warnings() == []

    def test_a_run_with_no_token_posts_nothing(self, hub, warnings, registered_flow, monkeypatch):
        monkeypatch.setattr(routes, "FLOWFILE_HUB_URL", HUB_URL)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert warnings() == []
