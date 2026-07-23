"""Tests for materialising catalog views to kernel-readable IPC snapshots.

Covers ``CatalogService.materialize_table`` / ``VirtualTableService.materialize_virtual_table``
and the ``POST /catalog/tables/{table_id}/materialize`` route. The worker call is
monkeypatched at the ``virtual_tables`` seam: the fake records the request and writes a
real IPC file so the path/row-count contract is exercised end-to-end without a worker.
"""

import hashlib
import io
import tempfile
from types import SimpleNamespace

import polars as pl
import pytest
import requests
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.exceptions import (
    NotAuthorizedError,
    NotAVirtualTableError,
    TableNotFoundError,
    WorkerUnavailableError,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import virtual_tables as virtual_tables_module
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.transform_schema import BasicFilter, FilterInput
from shared.storage_config import storage
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA as SAMPLE_DATA,
)
from tests.flowfile.conftest import (
    add_test_catalog_writer as _add_catalog_writer,
)
from tests.flowfile.conftest import (
    add_test_manual_input as _add_manual_input,
)
from tests.flowfile.conftest import (
    catalog_cleanup as _cleanup,
)
from tests.flowfile.conftest import (
    create_test_flow_registration as _create_flow_registration,
)
from tests.flowfile.conftest import (
    create_test_graph as _create_graph,
)
from tests.flowfile.conftest import (
    create_test_namespace as _create_namespace,
)
from tests.flowfile.conftest import (
    run_test_graph as _run_graph,
)

# Helpers


def _get_auth_token() -> str:
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]


def _get_test_client() -> TestClient:
    token = _get_auth_token()
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _get_test_client()


def _wipe_shared_results() -> None:
    shared_dir = storage.shared_virtual_results_directory
    if shared_dir.exists():
        for entry in shared_dir.iterdir():
            if entry.is_file():
                entry.unlink(missing_ok=True)


@pytest.fixture(autouse=True)
def clean_state():
    _cleanup()
    _wipe_shared_results()
    yield
    _cleanup()
    _wipe_shared_results()


@pytest.fixture()
def catalog_service() -> CatalogService:
    with get_db_context() as db:
        yield CatalogService(SQLAlchemyCatalogRepository(db))


@pytest.fixture()
def fake_worker(monkeypatch) -> list[dict]:
    """Replace the worker trigger with a fake that records args and writes real IPC."""
    calls: list[dict] = []

    def fake_trigger(table_id, plan_bytes, source_versions_hash, target="virtual_results"):
        calls.append(
            {
                "table_id": table_id,
                "plan_bytes": plan_bytes,
                "source_versions_hash": source_versions_hash,
                "target": target,
            }
        )
        target_dir = storage.shared_virtual_results_directory
        target_dir.mkdir(parents=True, exist_ok=True)
        name = f"fvt-{table_id}-{source_versions_hash[:16]}.arrow"
        df = pl.LazyFrame.deserialize(io.BytesIO(plan_bytes)).collect()
        df.write_ipc(target_dir / name)
        return {
            "ipc_path": name,
            "mtime": (target_dir / name).stat().st_mtime,
            "row_count": df.height,
        }

    monkeypatch.setattr(virtual_tables_module, "trigger_resolve_virtual_table", fake_trigger)
    return calls


def _seed_physical_delta_table(ns_id: int, name: str = "people") -> int:
    """Write SAMPLE_DATA as a real Delta dir and register it as a physical catalog table."""
    delta_dir = tempfile.mkdtemp(prefix=f"materialize_{name}_")
    pl.DataFrame(SAMPLE_DATA).write_delta(delta_dir)
    with get_db_context() as db:
        table = CatalogTable(
            name=name,
            namespace_id=ns_id,
            owner_id=1,
            file_path=delta_dir,
            storage_format="delta",
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id


def _seed_query_view(ns_id: int, sql_query: str, name: str = "people_view") -> int:
    """Insert a query-based virtual table row directly (resolve re-validates the SQL)."""
    with get_db_context() as db:
        table = CatalogTable(
            name=name,
            namespace_id=ns_id,
            owner_id=1,
            file_path=None,
            storage_format="delta",
            table_type="virtual",
            sql_query=sql_query,
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id


def _seed_optimized_flow_virtual(ns_id: int, table_name: str = "adults_virtual") -> int:
    """manual_input → age-filter → virtual writer; returns the optimized virtual table id."""
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        flow_path = f.name
    reg_id = _create_flow_registration(ns_id, name=f"prod_{table_name}", path=flow_path)
    graph = _create_graph(source_registration_id=reg_id)
    _add_manual_input(graph, SAMPLE_DATA, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter"))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=FilterInput(
                mode="basic",
                basic_filter=BasicFilter(field="age", operator="greater_than", value="28"),
            ),
        )
    )
    from flowfile_core.flowfile.flow_graph import add_connection

    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
    _add_catalog_writer(
        graph, node_id=3, depending_on_id=2, table_name=table_name, namespace_id=ns_id, write_mode="virtual"
    )
    _run_graph(graph)
    graph.save_flow(flow_path)
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        return next(t.id for t in repo.list_tables(namespace_id=ns_id) if t.name == table_name)


# Service-level tests


class TestMaterializeService:
    def test_query_view_over_delta_table(self, catalog_service, fake_worker):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name, age FROM people WHERE age > 28")

        result = catalog_service.materialize_table(view_id)

        assert len(fake_worker) == 1
        call = fake_worker[0]
        assert call["table_id"] == view_id
        assert call["target"] == "kernel_shared"
        key = hashlib.sha256(call["plan_bytes"]).hexdigest()
        assert call["source_versions_hash"] == key
        expected = storage.shared_virtual_results_directory / f"fvt-{view_id}-{key[:16]}.arrow"
        assert result.path == str(expected)
        assert result.format == "ipc"
        assert result.row_count == 2
        df = pl.read_ipc(result.path)
        assert set(df["name"].to_list()) == {"Alice", "Charlie"}

    def test_optimized_flow_virtual(self, catalog_service, fake_worker):
        ns_id = _create_namespace()
        vt_id = _seed_optimized_flow_virtual(ns_id)

        result = catalog_service.materialize_table(vt_id)

        assert fake_worker[0]["target"] == "kernel_shared"
        assert result.row_count == 2
        df = pl.read_ipc(result.path)
        assert set(df["name"].to_list()) == {"Alice", "Charlie"}

    def test_physical_table_raises(self, catalog_service, fake_worker):
        ns_id = _create_namespace()
        table_id = _seed_physical_delta_table(ns_id)
        with pytest.raises(NotAVirtualTableError):
            catalog_service.materialize_table(table_id)
        assert fake_worker == []

    def test_missing_table_raises(self, catalog_service, fake_worker):
        with pytest.raises(TableNotFoundError):
            catalog_service.materialize_table(999_999)

    def test_circular_view_raises_value_error(self, catalog_service, fake_worker):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_a = _seed_query_view(ns_id, "SELECT * FROM view_b", name="view_a")
        _seed_query_view(ns_id, "SELECT * FROM view_a", name="view_b")

        with pytest.raises(ValueError):
            catalog_service.materialize_table(view_a)
        assert fake_worker == []

    def test_worker_unreachable_raises_typed_error(self, catalog_service, monkeypatch):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name FROM people")

        def unreachable(*args, **kwargs):
            raise requests.exceptions.ConnectionError("worker down")

        monkeypatch.setattr(virtual_tables_module, "trigger_resolve_virtual_table", unreachable)
        with pytest.raises(WorkerUnavailableError):
            catalog_service.materialize_table(view_id)

    def test_resolution_error_scrubs_absolute_paths(self, catalog_service, fake_worker, monkeypatch):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name FROM people")

        def exploding_resolve(self, table_id, user_id=None, **kwargs):
            raise pl.exceptions.ComputeError("failed to read /app/user_data/catalog_tables/secret_dir/part-0.parquet")

        monkeypatch.setattr(
            virtual_tables_module.VirtualTableService, "resolve_virtual_flow_table", exploding_resolve
        )
        with pytest.raises(ValueError) as exc_info:
            catalog_service.materialize_table(view_id)
        assert "/app/user_data" not in str(exc_info.value)
        assert "part-0.parquet" in str(exc_info.value)
        assert fake_worker == []

    def test_worker_error_scrubs_absolute_paths(self, catalog_service, monkeypatch):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name FROM people")

        def failing_trigger(*args, **kwargs):
            raise RuntimeError("worker failed writing /Users/someone/.flowfile/temp/kernel_shared/out.arrow")

        monkeypatch.setattr(virtual_tables_module, "trigger_resolve_virtual_table", failing_trigger)
        with pytest.raises(WorkerUnavailableError) as exc_info:
            catalog_service.materialize_table(view_id)
        assert "/Users/someone" not in str(exc_info.value)
        assert "out.arrow" in str(exc_info.value)

    def test_sharing_enabled_non_granted_user_forbidden(self, fake_worker, monkeypatch):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name FROM people")

        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        stranger = SimpleNamespace(id=999_999, username="materialize_stranger", is_admin=False)
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db), access=AccessResolver(db, stranger))
            with pytest.raises(NotAuthorizedError):
                svc.materialize_table(view_id, user_id=stranger.id)
        assert fake_worker == []


# Route-level tests


class TestMaterializeRoute:
    def test_route_returns_result(self, fake_worker):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name, age FROM people WHERE age > 28")

        resp = client.post(f"/catalog/tables/{view_id}/materialize")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["format"] == "ipc"
        assert body["row_count"] == 2
        assert body["path"].endswith(".arrow")

    def test_route_physical_table_returns_400(self, fake_worker):
        ns_id = _create_namespace()
        table_id = _seed_physical_delta_table(ns_id)
        resp = client.post(f"/catalog/tables/{table_id}/materialize")
        assert resp.status_code == 400
        assert "not a virtual table" in resp.json()["detail"]

    def test_route_missing_table_returns_404(self, fake_worker):
        resp = client.post("/catalog/tables/999999/materialize")
        assert resp.status_code == 404

    def test_route_circular_view_returns_422(self, fake_worker):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_a = _seed_query_view(ns_id, "SELECT * FROM view_b", name="view_a")
        _seed_query_view(ns_id, "SELECT * FROM view_a", name="view_b")
        resp = client.post(f"/catalog/tables/{view_a}/materialize")
        assert resp.status_code == 422

    def test_route_worker_unreachable_returns_503(self, monkeypatch):
        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name FROM people")

        def unreachable(*args, **kwargs):
            raise requests.exceptions.ConnectionError("worker down")

        monkeypatch.setattr(virtual_tables_module, "trigger_resolve_virtual_table", unreachable)
        resp = client.post(f"/catalog/tables/{view_id}/materialize")
        assert resp.status_code == 503

    def test_route_accepts_internal_service_token(self, fake_worker):
        """The kernel authenticates with X-Internal-Token + X-Kernel-Id, not a JWT."""
        from flowfile_core.auth.jwt import get_internal_token

        ns_id = _create_namespace()
        _seed_physical_delta_table(ns_id)
        view_id = _seed_query_view(ns_id, "SELECT name, age FROM people WHERE age > 28")

        kernel_client = TestClient(main.app)
        resp = kernel_client.post(
            f"/catalog/tables/{view_id}/materialize",
            headers={"X-Internal-Token": get_internal_token(), "X-Kernel-Id": "test-kernel"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["row_count"] == 2
