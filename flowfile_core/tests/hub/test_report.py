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
from flowfile_core.hub import report
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import session
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema, schemas
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA,
    add_test_catalog_writer,
    add_test_manual_input,
    catalog_cleanup,
    create_test_graph,
    create_test_namespace,
    run_test_graph,
)
from tests.hub_client.conftest import FakeHub

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
    """A core with no ``FLOWFILE_HUB_URL``, a secure store of its own and a fake hub behind the hub client's seam.

    ``desktop.connect(user_id)`` gives that user a hub session on ``HUB_URL``, as connecting from the Hub tab does;
    the fake mints the access tokens the session path refreshes with.
    """
    monkeypatch.setattr(report, "FLOWFILE_HUB_URL", "")
    path = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(path))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(path))
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    fake = FakeHub()
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
        assert built["core_run_id"] is None
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

    def test_a_flow_that_came_from_a_hub_reports_the_version_it_ran(self):
        graph = create_test_graph(flow_id=8105)
        graph.flow_settings.hub_origin = schemas.HubOrigin(hub_url=HUB_URL, flow_uuid=FLOW_UUID, version=4)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, core_run_id=44, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert built["flow_version"] == 4
        assert built["compute_node_id"] is None
        assert built["core_run_id"] is None

    @pytest.mark.parametrize(
        ("mode", "expected"),
        [("hub", 45), ("docker", 45), (None, None), ("electron", None), ("package", None)],
        ids=["hub", "docker", "no-mode", "electron", "package"],
    )
    def test_only_a_server_core_names_its_own_run_row(self, monkeypatch, mode, expected):
        if mode is None:
            monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        else:
            monkeypatch.setenv("FLOWFILE_MODE", mode)
        graph = create_test_graph(flow_id=8106)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        run_info = run_test_graph(graph)

        built = report.build_run_report(graph, run_info, core_run_id=45, flow_uuid=FLOW_UUID)

        _assert_contract_shape(built)
        assert built["core_run_id"] == expected

    def test_a_desktop_report_differs_from_a_server_cores_only_in_the_run_row_id(self, monkeypatch):
        ns_id = create_test_namespace()
        graph = create_test_graph(flow_id=8107)
        graph.flow_settings.hub_origin = schemas.HubOrigin(hub_url=HUB_URL, flow_uuid=FLOW_UUID, version=6)
        add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
        add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="hub_both", namespace_id=ns_id)
        run_info = run_test_graph(graph)

        monkeypatch.setenv("FLOWFILE_MODE", "hub")
        on_server = report.build_run_report(graph, run_info, core_run_id=46, flow_uuid=FLOW_UUID)
        monkeypatch.delenv("FLOWFILE_MODE")
        on_desktop = report.build_run_report(graph, run_info, core_run_id=46, flow_uuid=FLOW_UUID)

        assert on_server["core_run_id"] == 46
        assert on_desktop["core_run_id"] is None
        assert uuid.UUID(on_server["run_id"]) != uuid.UUID(on_desktop["run_id"])
        same = REPORT_KEYS - {"run_id", "core_run_id"}
        assert {k: on_desktop[k] for k in same} == {k: on_server[k] for k in same}
        assert on_desktop["flow_version"] == 6
        assert on_desktop["outputs"] == [_table_ref(ns_id, "hub_both")]


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
        # The route authenticates as a desktop here; only the report is built as a server core builds it.
        monkeypatch.setattr(report, "is_server_mode", lambda: True)
        monkeypatch.setattr(report, "FLOWFILE_HUB_URL", HUB_URL)
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
        monkeypatch.setenv("FLOWFILE_MODE", "hub")
        monkeypatch.setattr(report, "FLOWFILE_HUB_URL", HUB_URL)
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

    def test_a_core_with_a_hub_url_outside_server_mode_posts_one_report_without_its_run_row(
        self, hub, warnings, registered_flow, monkeypatch
    ):
        monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        monkeypatch.setattr(report, "FLOWFILE_HUB_URL", HUB_URL)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1, hub_token="tok-123")

        [run] = _runs_of(flow_uuid)
        assert run.success is True and run.ended_at is not None
        assert len(hub.requests) == 1
        sent = json.loads(hub.requests[0].content)
        _assert_contract_shape(sent)
        assert (sent["flow_uuid"], sent["core_run_id"], sent["success"]) == (flow_uuid, None, True)
        assert warnings() == []

    def test_with_no_hub_url_and_no_session_nothing_is_posted(self, hub, warnings, registered_flow, desktop):
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        flow.flow_settings.hub_origin = schemas.HubOrigin(hub_url=HUB_URL, flow_uuid=flow_uuid, version=1)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1, hub_token="tok-123")

        assert len(_runs_of(flow_uuid)) == 1
        assert hub.requests == []
        assert desktop.calls == []
        assert warnings() == []

    def test_a_run_with_no_token_posts_nothing(self, hub, warnings, registered_flow, monkeypatch):
        monkeypatch.setattr(report, "FLOWFILE_HUB_URL", HUB_URL)
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
    """A core with no ``FLOWFILE_HUB_URL`` reports through the user's hub session, for flows from that hub only."""

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
        assert (body["flow_uuid"], body["flow_version"], body["core_run_id"]) == (flow_uuid, 3, None)
        assert body["compute_node_id"] is None
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

    def test_the_hub_url_wins_over_the_session(self, hub, warnings, registered_flow, desktop, monkeypatch):
        desktop.connect(1)
        monkeypatch.setattr(report, "FLOWFILE_HUB_URL", HUB_URL)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)
        _origin(flow, flow_uuid, version=5)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1, hub_token="tok-123")

        assert desktop.calls == []
        assert len(hub.requests) == 1
        assert hub.requests[0].headers["Authorization"] == "Bearer tok-123"
        assert json.loads(hub.requests[0].content)["flow_version"] == 5
        assert warnings() == []

    def test_the_hub_url_with_a_request_token_reports_without_any_session(
        self, hub, warnings, registered_flow, desktop, monkeypatch
    ):
        monkeypatch.setattr(report, "FLOWFILE_HUB_URL", HUB_URL)
        flow_id, flow_uuid = registered_flow
        flow = routes.flow_file_handler.get_flow(flow_id)

        assert flow.try_claim_run()
        routes._run_and_track(flow, user_id=1, hub_token="tok-123")

        assert desktop.calls == []
        assert len(hub.requests) == 1
        assert hub.requests[0].headers["Authorization"] == "Bearer tok-123"
        assert json.loads(hub.requests[0].content)["flow_version"] is None
        assert warnings() == []

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
