"""The run report core posts to a hub: its shape, the post, which hub a run reports to, and the hook in the run path."""

import json
import logging
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth import secrets as core_secrets
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable, FlowRegistration, FlowRun
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.hub_client import report
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.output_model import NodeResult, RunInformation
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA,
    add_test_catalog_writer,
    add_test_manual_input,
    catalog_cleanup,
    create_test_graph,
    create_test_namespace,
    run_test_graph,
)
from tests.hub_client.conftest import FakeHubWithCatalog

HUB_URL = "http://hub.test"
OTHER_HUB_URL = "http://other-hub.test"
# The ``hub`` fixture replaces ``httpx.Client`` module-wide; everything that wants a real one takes it from here.
_REAL_CLIENT = httpx.Client
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

    monkeypatch.setattr(
        report.httpx, "Client", lambda **kwargs: _REAL_CLIENT(transport=httpx.MockTransport(handler), **kwargs)
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


@pytest.fixture
def desktop(monkeypatch, tmp_path):
    """A core with a secure store of its own and a fake hub behind the hub client's seam.

    ``desktop.connect(user_id)`` gives that user a hub session on ``HUB_URL``, as connecting from the Hub tab does;
    the fake mints the access tokens the session path refreshes with and answers the hub's catalog routes.
    """
    path = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(path))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(path))
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    fake = FakeHubWithCatalog()
    monkeypatch.setattr(hub_client, "_module_http", _REAL_CLIENT(transport=httpx.MockTransport(fake.handle)))

    def connect(user_id: int, hub_url: str = HUB_URL) -> None:
        tokens = hub_client.sign_in(hub_url, "ana", "s3cret")
        session.save(
            user_id,
            {"hub_url": hub_url, "refresh_token": tokens["refresh_token"], "device_id": 1, "device_name": "laptop"},
        )
        fake.calls.clear()

    fake.connect = connect
    return fake


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
    def test_a_run_reports_its_nodes_without_any_data_and_no_table_of_its_own_catalog(self):
        ns_id = create_test_namespace()
        graph = create_test_graph(flow_id=8101)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="hub_people", namespace_id=ns_id)
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        uuid.UUID(built["run_id"])
        assert built["flow_uuid"] == FLOW_UUID
        assert (built["flow_version"], built["compute_node_id"], built["vend_ids"]) == (None, None, [])
        assert built["success"] is True
        assert built["started_at"] <= built["ended_at"]
        nodes = {node["node_id"]: node for node in built["nodes"]}
        assert nodes[1]["node_type"] == "manual_input"
        assert nodes[1]["rows_out"] == 3
        assert nodes[2]["node_type"] == "catalog_writer"
        assert all(node["success"] and node["error"] is None for node in nodes.values())
        assert all(node["duration_seconds"] is None or node["duration_seconds"] >= 0 for node in nodes.values())
        assert (built["inputs"], built["outputs"]) == ([], [])

        text = json.dumps(built)
        for value in ("Alice", "Amsterdam", "Copenhagen"):
            assert value not in text
        for key in ("plan", "sample", "schema", "columns"):
            assert f'"{key}"' not in text

    def test_a_failed_writer_reports_its_error_and_no_written_table(self):
        ns_id = create_test_namespace()
        graph = create_test_graph(flow_id=8104)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="", namespace_id=ns_id)
        run_info = graph.run_graph()

        built = report.build_run_report(graph, run_info, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert built["success"] is False
        writer = next(node for node in built["nodes"] if node["node_id"] == 2)
        assert writer["success"] is False
        assert "table name" in writer["error"]
        assert built["outputs"] == []

    def test_a_flow_that_came_from_a_hub_reports_the_version_it_ran(self):
        graph = create_test_graph(flow_id=8105)
        graph.flow_settings.hub_origin = schemas.HubOrigin(hub_url=HUB_URL, flow_uuid=FLOW_UUID, version=4)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert built["flow_version"] == 4
        assert built["compute_node_id"] is None

    @pytest.mark.parametrize("mode", ["hub", "docker", None, "electron"], ids=["hub", "docker", "no-mode", "electron"])
    def test_a_report_names_no_table_of_its_own_catalog_in_any_mode(self, monkeypatch, mode):
        if mode is None:
            monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        else:
            monkeypatch.setenv("FLOWFILE_MODE", mode)
        ns_id = create_test_namespace()
        seed = create_test_graph(flow_id=8108)
        add_test_manual_input(seed, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(seed, node_id=2, depending_on_id=1, table_name="hub_mine", namespace_id=ns_id)
        run_test_graph(seed)
        graph = create_test_graph(flow_id=8107)
        graph.flow_settings.hub_origin = schemas.HubOrigin(hub_url=HUB_URL, flow_uuid=FLOW_UUID, version=6)
        _add_reader(graph, 3, catalog_table_id=_table_ref(ns_id, "hub_mine")["table_id"])
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="hub_both", namespace_id=ns_id)
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert (built["inputs"], built["outputs"]) == ([], [])
        assert built["flow_version"] == 6
        assert {node["node_id"] for node in built["nodes"]} == {1, 2, 3}


class TestPostRunReport:
    REPORT = {"run_id": "r-1", "flow_uuid": FLOW_UUID, "nodes": []}

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
        assert "run r-1" in warnings()[0].getMessage()


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
    def test_a_run_started_through_the_route_reports_with_the_sessions_token_never_the_requests(
        self, hub, warnings, registered_flow, desktop
    ):
        desktop.connect(1)
        flow_id, flow_uuid = registered_flow
        _origin(routes.flow_file_handler.get_flow(flow_id), flow_uuid, version=2)
        token = _token()

        with TestClient(main.app) as client:
            response = client.post(
                "/flow/run/", params={"flow_id": flow_id}, headers={"Authorization": f"Bearer {token}"}
            )
        assert response.status_code == 200, response.text

        [run] = _runs_of(flow_uuid)
        assert run.success is True and run.ended_at is not None
        [sent] = [r for r in hub.requests if r.url.host == "hub.test"]
        assert sent.headers["Authorization"] != f"Bearer {token}"
        assert desktop._bearer_user(sent) == "ana"
        body = json.loads(sent.content)
        _assert_contract_shape(body)
        assert (body["flow_uuid"], body["flow_version"], body["success"]) == (flow_uuid, 2, True)
        assert [node["node_type"] for node in body["nodes"]] == ["manual_input"]
        assert warnings() == []

    def test_with_no_session_nothing_is_posted(self, hub, warnings, registered_flow, desktop):
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        flow.flow_settings.hub_origin = schemas.HubOrigin(hub_url=HUB_URL, flow_uuid=flow_uuid, version=1)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert desktop.calls == []
        assert warnings() == []

    def test_a_hub_url_in_the_environment_makes_no_core_report_without_a_session(
        self, hub, warnings, registered_flow, desktop, monkeypatch
    ):
        monkeypatch.setenv("FLOWFILE_HUB_URL", HUB_URL)
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert warnings() == []


def _origin(flow, flow_uuid: str, version: int, hub_url: str = HUB_URL) -> None:
    flow.flow_settings.hub_origin = schemas.HubOrigin(hub_url=hub_url, flow_uuid=flow_uuid, version=version)


class TestDesktopSession:
    """A core not joined with keys reports through the user's hub session, for flows from that hub only."""

    def test_a_flow_opened_from_the_hub_posts_one_report_with_its_version_and_the_sessions_token(
        self, hub, warnings, registered_flow, desktop
    ):
        desktop.connect(1)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        _origin(flow, flow_uuid, version=3)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        [run] = _runs_of(flow_uuid)
        assert run.success is True
        assert desktop.calls == [("POST", "/auth/refresh")]
        assert len(hub.requests) == 1
        sent = hub.requests[0]
        assert str(sent.url) == f"{HUB_URL}/hub/runs"
        assert desktop._bearer_user(sent) == "ana", "the bearer token is one the fake hub minted for the session's user"
        body = json.loads(sent.content)
        _assert_contract_shape(body)
        assert (body["flow_uuid"], body["flow_version"]) == (flow_uuid, 3)
        assert body["compute_node_id"] is None
        uuid.UUID(body["run_id"])
        assert body["vend_ids"] == []
        assert warnings() == []

    def test_a_flow_without_an_origin_posts_nothing(self, hub, warnings, registered_flow, desktop):
        desktop.connect(1)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        assert flow.flow_settings.hub_origin is None

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert desktop.calls == []
        assert warnings() == []

    def test_a_flow_from_another_hub_posts_nothing(self, hub, warnings, registered_flow, desktop):
        desktop.connect(1)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        _origin(flow, flow_uuid, version=2, hub_url=OTHER_HUB_URL)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert desktop.calls == []
        assert warnings() == []

    def test_a_session_of_another_user_does_not_report_this_users_run(self, hub, warnings, registered_flow, desktop):
        desktop.connect(2)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        _origin(flow, flow_uuid, version=2)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert desktop.calls == []

    def test_a_hub_that_is_down_costs_one_warning_and_the_run_completes(self, hub, warnings, registered_flow, desktop):
        desktop.connect(1)
        hub.answer = lambda request: (_ for _ in ()).throw(httpx.ConnectError("refused", request=request))
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        _origin(flow, flow_uuid, version=3)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        [run] = _runs_of(flow_uuid)
        assert run.success is True and run.ended_at is not None and run.nodes_completed == 1
        assert len(hub.requests) == 1
        assert len(warnings()) == 1
        assert not flow.flow_settings.is_running

    def test_a_refresh_the_hub_does_not_grant_costs_one_warning_and_posts_nothing(
        self, hub, warnings, registered_flow, desktop
    ):
        desktop.connect(1)
        desktop.live_refresh.clear()
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        _origin(flow, flow_uuid, version=3)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        [run] = _runs_of(flow_uuid)
        assert run.success is True
        assert desktop.calls == [("POST", "/auth/refresh")]
        assert hub.requests == []
        assert len(warnings()) == 1
        assert not flow.flow_settings.is_running


HUB_RUN_ID = "5f0c9a1e8b7d4c3a9e2f1d0c6b5a4f3e"
ORDERS_COLUMNS = [{"name": "id", "dtype": "Int64"}, {"name": "country", "dtype": "String"}]


@pytest.fixture
def hub_tables_flow(registered_flow, desktop):
    """The registered flow, connected for user 1, with a reader of the hub's ``sales.eu.orders`` (node 2) feeding a
    writer of ``orders_eu`` into the same schema (node 3); ``(flow, flow_uuid, schema)``.
    """
    desktop.connect(1)
    catalog = desktop.seed_catalog("sales", storage_uri="s3://acme/sales")
    schema = desktop.seed_schema(catalog, "eu")
    orders = desktop.seed_table(
        schema, "orders", file_path="s3://acme/sales/orders", is_remote_storage=True, schema_columns=ORDERS_COLUMNS
    )
    flow_id, flow_uuid = registered_flow
    flow = routes.flow_file_handler.get_flow(flow_id)
    _add_reader(
        flow, 2, user_id=1, hub_table_id=orders["id"], hub_namespace_id=schema["id"], catalog_table_name="orders"
    )
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow.flow_id, node_id=3, node_type="catalog_writer"))
    flow.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_id=2,
            user_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="orders_eu", hub_namespace_id=schema["id"]
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(from_id=2, to_id=3))
    desktop.calls.clear()
    desktop.catalog_calls.clear()
    return flow, flow_uuid, schema


def _ran(flow, monkeypatch, last_hub_vend: tuple[str, int] | None, success: bool = True) -> None:
    """Replace ``flow``'s run with one whose three nodes ended as ``success`` says and that leaves ``last_hub_vend``.

    The vend and the storage a real run reaches are the end-to-end test's; here only what the report reads is set.
    """

    def run_graph(**kwargs) -> RunInformation:
        flow.last_hub_vend = last_hub_vend
        results = []
        for node_id in (1, 2, 3):
            result = NodeResult(node_id=node_id)
            result.finish(success=success or node_id == 1, error="" if success or node_id == 1 else "refused")
            results.append(result)
        flow.release_run()
        return RunInformation(
            flow_id=flow.flow_id,
            end_time=None,
            success=success,
            nodes_completed=3 if success else 1,
            number_of_nodes=3,
            node_step_result=results,
            run_type="full_run",
        )

    monkeypatch.setattr(flow, "run_graph", run_graph)


class TestHubTables:
    """A desktop reports a run that vended from its hub, naming the hub's tables by the hub's ids and the vend."""

    @pytest.mark.parametrize("listed", [True, False], ids=["written-table-listed", "written-table-not-listed"])
    def test_a_run_that_vended_posts_one_report_with_the_vend_and_the_hubs_table_ids(
        self, hub, warnings, hub_tables_flow, desktop, monkeypatch, listed
    ):
        flow, flow_uuid, schema = hub_tables_flow
        written = desktop.seed_table(schema, "orders_eu", file_path="s3://acme/sales/orders_eu_1") if listed else None
        _ran(flow, monkeypatch, (HUB_RUN_ID, 7))
        assert flow.flow_settings.hub_origin is None

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        [run] = _runs_of(flow_uuid)
        assert run.success is True
        assert desktop.calls == [("GET", "/catalog/tables")], "one hub call beside the post, on the cached token"
        assert desktop.catalog_calls == [("/catalog/tables", f"namespace_id={schema['id']}")]
        [sent] = hub.requests
        assert str(sent.url) == f"{HUB_URL}/hub/runs"
        assert desktop._bearer_user(sent) == "ana"
        body = json.loads(sent.content)
        _assert_contract_shape(body)
        assert (body["run_id"], body["vend_ids"]) == (HUB_RUN_ID, [7])
        assert (body["flow_uuid"], body["flow_version"]) == (flow_uuid, None)
        orders = next(t for t in desktop.tables if t["name"] == "orders")
        assert body["inputs"] == [{"namespace_id": schema["id"], "table_id": orders["id"], "table_name": "orders"}]
        assert body["outputs"] == [
            {
                "namespace_id": schema["id"],
                "table_id": written["id"] if listed else None,
                "table_name": "orders_eu",
            }
        ]
        assert warnings() == []

    def test_a_hub_that_does_not_list_its_tables_leaves_the_written_id_out_with_one_warning(
        self, hub, warnings, hub_tables_flow, desktop, monkeypatch
    ):
        flow, flow_uuid, schema = hub_tables_flow
        _ran(flow, monkeypatch, (HUB_RUN_ID, 7))

        def handle(request):
            if request.url.path == "/catalog/tables":
                return httpx.Response(503, json={"detail": "catalog unavailable"})
            return desktop.handle(request)

        monkeypatch.setattr(hub_client, "_module_http", _REAL_CLIENT(transport=httpx.MockTransport(handle)))

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        [sent] = hub.requests
        body = json.loads(sent.content)
        assert body["outputs"] == [{"namespace_id": schema["id"], "table_id": None, "table_name": "orders_eu"}]
        assert body["vend_ids"] == [7]
        assert len(warnings()) == 1
        assert "catalog unavailable" in warnings()[0].getMessage()

    def test_a_run_of_hub_tables_that_vended_nothing_posts_nothing(
        self, hub, warnings, hub_tables_flow, desktop, monkeypatch
    ):
        flow, flow_uuid, _ = hub_tables_flow
        _ran(flow, monkeypatch, None, success=False)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1)

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert desktop.calls == []
        assert warnings() == []
