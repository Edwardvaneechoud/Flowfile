"""Full-stack change-tracking (CDC) scenario, driven over HTTP exactly as the editor drives it.

Every step goes through the real FastAPI app: create a flow, place nodes, save settings, run,
read the node preview, call the ``/catalog/tables/{id}/cdc`` routes. Real Delta tables, a real
cursor, no mocking. Complements ``tests/flowfile/test_catalog_cdc.py`` (in-process FlowGraph) and
``tests/test_catalog_cdc_routes.py`` (routes against hand-built tables).
"""

import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogCdcCursor,
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
    FlowSchedule,
    TableFavorite,
)
from flowfile_core.routes.routes import flow_file_handler
from flowfile_core.schemas import input_schema, schemas

V1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
V2 = [{"id": 2, "name": "Bobby"}, {"id": 3, "name": "Carol"}]

FLOWS_DIR = Path("flowfile_core/tests/support_files/flows/tmp")


@pytest.fixture(scope="module")
def client():
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
        c.headers.update({"Authorization": f"Bearer {token}"})
        yield c


def _cleanup_catalog():
    with get_db_context() as db:
        db.query(CatalogCdcCursor).delete()
        db.query(TableFavorite).delete()
        db.query(CatalogTableReadLink).delete()
        db.query(FlowSchedule).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


@pytest.fixture(autouse=True)
def clean_state():
    _cleanup_catalog()
    created: list[Path] = []
    yield created
    for flow in list(flow_file_handler.flowfile_flows):
        flow_file_handler.delete_flow(flow.flow_id)
    for path in created:
        path.unlink(missing_ok=True)
    _cleanup_catalog()


@pytest.fixture
def namespace_id(client) -> int:
    catalog = client.post("/catalog/namespaces", json={"name": f"CdcE2E_{uuid.uuid4().hex[:8]}"})
    assert catalog.status_code == 201, catalog.text
    schema = client.post("/catalog/namespaces", json={"name": "sales", "parent_id": catalog.json()["id"]})
    assert schema.status_code == 201, schema.text
    return schema.json()["id"]


def _new_flow(client, clean_state, stem: str) -> int:
    """Create (and register) a flow the way the editor's New Flow button does.

    Switched to Development mode, as the editor does, so every node keeps a cached
    result the ``/node/data`` preview can show.
    """
    path = FLOWS_DIR / f"{stem}.yaml"
    path.unlink(missing_ok=True)
    clean_state.append(path)
    response = client.post("editor/create_flow", params={"flow_path": str(path)})
    assert response.status_code == 200, response.text
    flow_id = response.json()

    current = client.get("/flow_settings", params={"flow_id": flow_id})
    assert current.status_code == 200, current.text
    settings = schemas.FlowSettings(
        **{k: v for k, v in current.json().items() if k in schemas.FlowSettings.model_fields}
    )
    settings.execution_mode = "Development"
    updated = client.post("/flow_settings", json=settings.model_dump(mode="json"))
    assert updated.status_code == 200, updated.text
    return flow_id


def _add_node(client, flow_id: int, node_id: int, node_type: str) -> None:
    response = client.post(
        "/editor/add_node",
        params={"flow_id": flow_id, "node_id": node_id, "node_type": node_type, "pos_x": 0, "pos_y": 0},
    )
    assert response.status_code == 200, response.text


def _save_settings(client, settings, node_type: str) -> None:
    response = client.post("/update_settings/", json=settings.model_dump(), params={"node_type": node_type})
    assert response.status_code == 200, response.text


def _connect(client, flow_id: int, from_id: int, to_id: int) -> None:
    connection = input_schema.NodeConnection.create_from_simple_input(from_id, to_id)
    response = client.post("/editor/connect_node/", data=connection.model_dump_json(), params={"flow_id": flow_id})
    assert response.status_code == 200, response.text


def _run(client, flow_id: int) -> dict:
    """Run a flow and return its RunInformation once it has settled."""
    started = client.post("/flow/run/", params={"flow_id": flow_id})
    assert started.status_code in (200, 202), started.text
    for _ in range(600):
        status = client.get("/flow/run_status/", params={"flow_id": flow_id})
        assert status.status_code in (200, 202), status.text
        if status.status_code == 200 and not status.json()["is_running"]:
            return status.json()
        time.sleep(0.1)
    raise AssertionError(f"Flow {flow_id} never finished")


def _node_error(run_info: dict, node_id: int) -> str:
    return next(r["error"] for r in run_info["node_step_result"] if r["node_id"] == node_id)


def _preview(client, flow_id: int, node_id: int) -> dict:
    response = client.get("/node/data", params={"flow_id": flow_id, "node_id": node_id})
    assert response.status_code == 200, response.text
    return response.json()


def _build_writer_flow(
    client,
    clean_state,
    namespace_id: int,
    table_name: str,
    stem: str,
    *,
    track_changes: bool = True,
    write_mode: str = "upsert",
) -> tuple[int, int]:
    """A manual_input -> catalog_writer flow. Returns ``(flow_id, manual_input_node_id)``."""
    flow_id = _new_flow(client, clean_state, stem)
    _add_node(client, flow_id, 1, "manual_input")
    _save_settings(
        client,
        input_schema.NodeManualInput(flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist(V1)),
        "manual_input",
    )
    _add_node(client, flow_id, 2, "catalog_writer")
    _save_settings(
        client,
        input_schema.NodeCatalogWriter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=table_name,
                namespace_id=namespace_id,
                write_mode=write_mode,
                merge_keys=["id"] if write_mode == "upsert" else [],
                track_changes=track_changes,
            ),
        ),
        "catalog_writer",
    )
    _connect(client, flow_id, 1, 2)
    return flow_id, 1


def _rewrite_with(client, flow_id: int, node_id: int, rows: list[dict]) -> None:
    _save_settings(
        client,
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(rows)
        ),
        "manual_input",
    )


def _build_reader_flow(client, clean_state, table_id: int, stem: str, **reader_kwargs) -> tuple[int, int]:
    """A lone catalog_reader flow. Returns ``(flow_id, reader_node_id)``."""
    flow_id = _new_flow(client, clean_state, stem)
    _add_node(client, flow_id, 1, "catalog_reader")
    _save_settings(
        client,
        input_schema.NodeCatalogReader(flow_id=flow_id, node_id=1, catalog_table_id=table_id, **reader_kwargs),
        "catalog_reader",
    )
    return flow_id, 1


def _table_id(client, namespace_id: int, table_name: str) -> int:
    response = client.get("/catalog/tables", params={"namespace_id": namespace_id})
    assert response.status_code == 200, response.text
    match = next(t for t in response.json() if t["name"] == table_name)
    return match["id"]


def _cdc(client, table_id: int) -> dict:
    response = client.get(f"/catalog/tables/{table_id}/cdc")
    assert response.status_code == 200, response.text
    return response.json()


def test_tracked_writer_enables_change_tracking(client, clean_state, namespace_id):
    """A writer with Track changes on turns the Delta change data feed on for its table."""
    flow_id, _ = _build_writer_flow(client, clean_state, namespace_id, "orders", "cdc_e2e_writer")
    run_info = _run(client, flow_id)
    assert run_info["success"] is True, run_info["node_step_result"]

    table_id = _table_id(client, namespace_id, "orders")
    status = _cdc(client, table_id)
    assert status["cdc_enabled"] is True
    assert status["cdc_enabled_version"] == 0
    assert status["current_version"] == 0
    assert status["cursors"] == []

    listed = client.get("/catalog/tables", params={"namespace_id": namespace_id}).json()
    assert next(t for t in listed if t["id"] == table_id)["cdc_enabled"] is True


def test_untracked_writer_leaves_change_tracking_off(client, clean_state, namespace_id):
    flow_id, _ = _build_writer_flow(client, clean_state, namespace_id, "plain", "cdc_e2e_plain", track_changes=False)
    assert _run(client, flow_id)["success"] is True
    status = _cdc(client, _table_id(client, namespace_id, "plain"))
    assert status["cdc_enabled"] is False
    assert status["cdc_enabled_version"] is None


def test_change_reader_round_trip(client, clean_state, namespace_id):
    """The headline scenario: replay, empty window, only-the-delta, reset, replay again."""
    writer_flow, input_node = _build_writer_flow(client, clean_state, namespace_id, "orders", "cdc_e2e_rt_writer")
    assert _run(client, writer_flow)["success"] is True
    table_id = _table_id(client, namespace_id, "orders")

    reader_flow, reader_node = _build_reader_flow(
        client, clean_state, table_id, "cdc_e2e_rt_reader", cdc_mode="since_last_run", cdc_start="beginning"
    )

    # First run replays the creation commit: two inserts, carrying the three feed columns.
    assert _run(client, reader_flow)["success"] is True
    first = _preview(client, reader_flow, reader_node)
    assert first["number_of_records"] == 2
    assert {"_change_type", "_commit_version", "_commit_timestamp"} <= set(first["columns"])
    assert sorted(r["_change_type"] for r in first["data"]) == ["insert", "insert"]
    assert {r["_commit_version"] for r in first["data"]} == {0}

    status = _cdc(client, table_id)
    assert len(status["cursors"]) == 1
    cursor = status["cursors"][0]
    consumer_key = cursor["consumer_key"]
    assert consumer_key.startswith("flow:") and consumer_key.endswith(f":node:{reader_node}")
    assert cursor["last_version"] == 0
    assert cursor["pending_commits"] == 0

    # Nothing new happened: the same flow re-reads an empty window, cursor unmoved.
    assert _run(client, reader_flow)["success"] is True
    assert _preview(client, reader_flow, reader_node)["number_of_records"] == 0
    assert _cdc(client, table_id)["cursors"][0]["last_version"] == 0

    # One changed row and one new row land as a second commit.
    _rewrite_with(client, writer_flow, input_node, V2)
    assert _run(client, writer_flow)["success"] is True
    assert _cdc(client, table_id)["current_version"] == 1

    assert _run(client, reader_flow)["success"] is True
    delta = _preview(client, reader_flow, reader_node)
    assert delta["number_of_records"] == 2
    by_type = {r["_change_type"]: r for r in delta["data"]}
    assert sorted(by_type) == ["insert", "update_postimage"]
    assert by_type["update_postimage"]["id"] == 2 and by_type["update_postimage"]["name"] == "Bobby"
    assert by_type["insert"]["id"] == 3 and by_type["insert"]["name"] == "Carol"
    assert all(r["_commit_version"] == 1 for r in delta["data"])
    assert _cdc(client, table_id)["cursors"][0]["last_version"] == 1

    # Reset to the beginning and the whole tracked history replays.
    reset = client.post(
        f"/catalog/tables/{table_id}/cdc/cursors/reset",
        json={"consumer_key": consumer_key, "to": "beginning"},
    )
    assert reset.status_code == 200, reset.text
    assert reset.json()["last_version"] == -1

    assert _run(client, reader_flow)["success"] is True
    replay = _preview(client, reader_flow, reader_node)
    assert replay["number_of_records"] == 4
    assert sorted(r["_change_type"] for r in replay["data"]) == [
        "insert",
        "insert",
        "insert",
        "update_postimage",
    ]
    assert _cdc(client, table_id)["cursors"][0]["last_version"] == 1

    # And the cursor can be forgotten entirely.
    deleted = client.delete(f"/catalog/tables/{table_id}/cdc/cursors/{consumer_key}")
    assert deleted.status_code == 204, deleted.text
    assert _cdc(client, table_id)["cursors"] == []


def test_preimage_is_available_on_request(client, clean_state, namespace_id):
    writer_flow, input_node = _build_writer_flow(client, clean_state, namespace_id, "orders", "cdc_e2e_pre_writer")
    assert _run(client, writer_flow)["success"] is True
    table_id = _table_id(client, namespace_id, "orders")
    _rewrite_with(client, writer_flow, input_node, V2)
    assert _run(client, writer_flow)["success"] is True

    reader_flow, reader_node = _build_reader_flow(
        client,
        clean_state,
        table_id,
        "cdc_e2e_pre_reader",
        cdc_mode="since_version",
        cdc_from_version=0,
        cdc_include_preimage=True,
    )
    assert _run(client, reader_flow)["success"] is True
    rows = _preview(client, reader_flow, reader_node)["data"]
    assert sorted(r["_change_type"] for r in rows) == ["insert", "update_postimage", "update_preimage"]
    assert next(r for r in rows if r["_change_type"] == "update_preimage")["name"] == "Bob"
    # A version-window read keeps no cursor.
    assert _cdc(client, table_id)["cursors"] == []


def test_change_reader_on_untracked_table_errors_clearly(client, clean_state, namespace_id):
    writer_flow, _ = _build_writer_flow(
        client, clean_state, namespace_id, "plain", "cdc_e2e_untracked_writer", track_changes=False
    )
    assert _run(client, writer_flow)["success"] is True
    table_id = _table_id(client, namespace_id, "plain")

    reader_flow, reader_node = _build_reader_flow(
        client, clean_state, table_id, "cdc_e2e_untracked_reader", cdc_mode="since_last_run"
    )
    run_info = _run(client, reader_flow)
    assert run_info["success"] is False
    assert "Enable change tracking on 'plain' first" in _node_error(run_info, reader_node)
    assert _cdc(client, table_id)["cursors"] == []

    # The table-level enable action is the fix the error points at.
    enabled = client.post(f"/catalog/tables/{table_id}/cdc/enable")
    assert enabled.status_code == 200, enabled.text
    assert enabled.json()["cdc_enabled"] is True
    assert _run(client, reader_flow)["success"] is True


def test_vacuum_refuses_to_strand_a_cursor_unless_forced(client, clean_state, namespace_id):
    writer_flow, input_node = _build_writer_flow(client, clean_state, namespace_id, "orders", "cdc_e2e_vac_writer")
    assert _run(client, writer_flow)["success"] is True
    table_id = _table_id(client, namespace_id, "orders")

    reader_flow, _ = _build_reader_flow(client, clean_state, table_id, "cdc_e2e_vac_reader", cdc_mode="since_last_run")
    assert _run(client, reader_flow)["success"] is True
    consumer_key = _cdc(client, table_id)["cursors"][0]["consumer_key"]

    # Advance the table past the cursor so its next read reaches into history.
    _rewrite_with(client, writer_flow, input_node, V2)
    assert _run(client, writer_flow)["success"] is True
    assert _cdc(client, table_id)["cursors"][0]["pending_commits"] == 1

    refused = client.post(f"/catalog/tables/{table_id}/vacuum", json={"retention_hours": 0, "dry_run": True})
    assert refused.status_code == 409, refused.text
    detail = refused.json()["detail"]
    assert detail["error_code"] == "CDC_CURSORS_AT_RISK"
    assert [c["consumer_key"] for c in detail["cursors"]] == [consumer_key]

    forced = client.post(
        f"/catalog/tables/{table_id}/vacuum",
        json={"retention_hours": 0, "dry_run": True, "force": True},
    )
    assert forced.status_code == 200, forced.text
    assert forced.json()["dry_run"] is True


def test_deleting_the_table_purges_its_cursors(client, clean_state, namespace_id):
    writer_flow, _ = _build_writer_flow(client, clean_state, namespace_id, "orders", "cdc_e2e_del_writer")
    assert _run(client, writer_flow)["success"] is True
    table_id = _table_id(client, namespace_id, "orders")
    reader_flow, _ = _build_reader_flow(client, clean_state, table_id, "cdc_e2e_del_reader", cdc_mode="since_last_run")
    assert _run(client, reader_flow)["success"] is True
    assert len(_cdc(client, table_id)["cursors"]) == 1

    deleted = client.delete(f"/catalog/tables/{table_id}")
    assert deleted.status_code == 204, deleted.text
    with get_db_context() as db:
        assert db.query(CatalogCdcCursor).filter(CatalogCdcCursor.table_id == table_id).count() == 0
