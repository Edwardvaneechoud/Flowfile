"""Core runs a node only a kernel session's cells hold (``POST /notebook/session/node_run``) from the cell's
settings and its inputs' rows, through the ``kernel-sim`` manager (no Docker). A source that resolves a stored
connection (a database or cloud read) is held too: the kernel opens no database and holds no key."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest
from pydantic import SecretStr

from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_database_connection,
    store_database_connection,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas import input_schema
from shared.notebook_display import TABLE_MIME
from tests.notebook.conftest import IN_KERNEL_OP, NOTEBOOK_OWNER_ID

LOOPBACK = ("127.0.0.1", 50123)
OTHER_KERNEL = "other-kernel"


@pytest.fixture
def client(client_as, kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


@pytest.fixture
def locking_client(client_as, locking_kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


@pytest.fixture
def coded_flow(open_as):
    """``from_dict -> polars_code``: the code node is deferred in a session, so its rows come from the canvas."""
    import flowfile as ff

    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return open_as(orders.polars_code("input_df.with_columns(pl.col('amount') * 10)").flow_graph)


def _coded_id(flow) -> int:
    return next(node.node_id for node in flow.nodes if node.node_type == "polars_code")


def _bind(node_id: int) -> str:
    frames = "(v for v in list(globals().values()) if type(v).__name__ == 'FlowFrame')"
    return f"coded = next(v for v in {frames} if v.node_id == {node_id})\n"


def _execute(client, flow, sim, code: str, cell_id: str = "cell-1") -> dict:
    body = {"flow_id": flow.flow_id, "kernel_id": sim.kernel.id, "cell_id": cell_id, "code": code}
    response = client.post("/notebook/session/execute", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def _rows(result: dict) -> list:
    tables = [out for out in result["display_outputs"] if out["mime_type"] == TABLE_MIME]
    assert tables, result
    return json.loads(tables[0]["data"])["data"]


def _text(result: dict) -> str:
    return "\n".join(out["data"] for out in result["display_outputs"] if out["mime_type"] == "text/plain")


def test_a_gate_a_cell_built_runs_in_core_over_the_canvas_rows(coded_flow, client, kernel_sim):
    """A new gate has no canvas node: core runs it from the cell's settings over its input's canvas rows (the
    parquet core handed the session) and answers only the live exit; the dead one says so and runs nothing again."""
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + "g = ff.Gate(coded, formula='[amount] > 150')\nprint(g.then.columns)\ndisplay(g.then)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert shown["stdout"].strip() == "['id', 'amount']", shown
    assert [row["amount"] for row in _rows(shown)] == [100, 200, 300], shown["display_outputs"]
    [run] = kernel_sim.node_runs
    assert run["node"]["type"] == "gate" and not run["schema_only"], run
    assert [body["node_id"] for body in kernel_sim.node_results] == [node_id]
    assert [Path(entry["path"]).name.split("_")[0] for entry in run["inputs"]] == [str(node_id)], run["inputs"]

    dead = _execute(client, coded_flow, kernel_sim, "display(g.otherwise)")
    assert dead["success"], dead
    assert "a gate routed it away" in _text(dead), dead["display_outputs"]
    assert len(kernel_sim.node_runs) == 1, "the dead exit runs nothing again"


def test_a_held_node_between_nodes_this_kernel_computes(coded_flow, client, kernel_sim):
    """The kernel computes the filter, writes its rows for core, core runs the gate, the kernel computes below it;
    reading again reuses core's answer."""
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + (
        "big = coded.filter(ff.col('amount') > 100)\n"
        "g = ff.Gate(big, formula='[amount] > 0')\n"
        "out = g.then.with_columns(x=ff.lit(1))\n"
        "display(out)"
    )
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert _rows(shown) == [{"id": 2, "amount": 200, "x": 1}, {"id": 3, "amount": 300, "x": 1}], shown
    [run] = kernel_sim.node_runs
    [written] = run["inputs"]
    assert Path(written["path"]).name.startswith("input_"), written
    assert Path(written["path"]).parent == Path(kernel_sim.shared_volume_path, "notebook", str(coded_flow.flow_id))

    again = _execute(client, coded_flow, kernel_sim, "print(out.collect().height)")
    assert again["success"] and again["stdout"].strip() == "2", again
    assert len(kernel_sim.node_runs) == 1


def test_a_parameter_declared_in_the_session_routes_a_held_gate(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + (
        "env = ff.add_flow_parameter(flow, ff.Parameter('env', default='prod'))\n"
        "g = ff.Gate(coded, parameter=env, operator='equals', value='dev')\n"
        "display(g.otherwise)"
    )
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert len(_rows(shown)) == 3, shown
    [run] = kernel_sim.node_runs
    assert [(p["name"], p["default_value"]) for p in run["parameters"]] == [("env", "prod")], run["parameters"]
    dead = _execute(client, coded_flow, kernel_sim, "display(g.then)")
    assert dead["success"] and "a gate routed it away" in _text(dead), dead


def test_a_script_a_cell_built_on_the_sessions_own_kernel_is_refused_at_once(
    coded_flow, locking_client, locking_kernel_sim
):
    node_id = _coded_id(coded_flow)
    kernel = locking_kernel_sim.kernel.id
    cell = _bind(node_id) + f"new = ff.PythonScript(coded, code='x = 1', kernel={kernel!r}).output"
    assert _execute(locking_client, coded_flow, locking_kernel_sim, cell)["success"]
    started = time.monotonic()
    read = _execute(locking_client, coded_flow, locking_kernel_sim, "new.collect()")
    assert time.monotonic() - started < locking_kernel_sim.LOCK_WAIT, "refused, not waited for"
    assert not read["success"], read
    assert "own kernel" in read["error"] and "another kernel" in read["error"], read["error"]


def test_a_writer_a_cell_built_is_refused_and_writes_nothing(coded_flow, client, kernel_sim, tmp_path):
    node_id = _coded_id(coded_flow)
    target = tmp_path / "out.parquet"
    cell = _bind(node_id) + f"w = coded.write_parquet({str(target)!r})\nw.collect()"
    result = _execute(client, coded_flow, kernel_sim, cell)
    assert not result["success"], result
    assert "writes when the flow runs: Push" in result["error"], result["error"]
    assert not target.exists()


def test_stop_cancels_a_held_run(coded_flow, locking_client, locking_kernel_sim):
    """A held script waits on ``OTHER_KERNEL``, whose lock this test holds; Stop cancels core's run and the cell
    comes back."""
    from flowfile_core.notebook import held_run

    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + f"new = ff.PythonScript(coded, code='x = 1', kernel={OTHER_KERNEL!r}).output"
    assert _execute(locking_client, coded_flow, locking_kernel_sim, cell)["success"]
    held = locking_kernel_sim._locks.setdefault(OTHER_KERNEL, threading.Lock())
    held.acquire()
    results: list = []
    key = {"flow_id": coded_flow.flow_id, "kernel_id": locking_kernel_sim.kernel.id}
    try:
        thread = threading.Thread(
            target=lambda: results.append(_execute(locking_client, coded_flow, locking_kernel_sim, "new.collect()"))
        )
        thread.start()
        deadline = time.monotonic() + locking_kernel_sim.LOCK_WAIT
        while not held_run._running and time.monotonic() < deadline:
            time.sleep(0.01)
        assert held_run._running, "core's run of the held node started"
        answer = locking_client.post("/notebook/session/interrupt", json=key)
        assert answer.status_code == 200 and answer.json()["status"] == "interrupted", answer.text
        thread.join(timeout=locking_kernel_sim.LOCK_WAIT)
        assert not thread.is_alive(), "the cell came back once the run was cancelled"
    finally:
        held.release()
    [result] = results
    assert not result["success"] and "cancelled" in result["error"], result
    assert not held_run._running


def test_a_held_run_is_bound_to_the_kernels_open_session(coded_flow, kernel_sim):
    from fastapi import HTTPException

    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.notebook import held_run, kernel_runner

    owner = PydanticUser(username="owner", id=kernel_sim.owner_id, disabled=False)
    other = PydanticUser(username="other", id=kernel_sim.owner_id + 1, disabled=False)
    gate = {"id": 9, "type": "gate", "setting_input": {"gate_input": {"condition_source": "formula", "formula": "1"}}}
    body = held_run.NodeRunRequest(flow_id=coded_flow.flow_id, node=gate)

    with pytest.raises(HTTPException) as no_session:
        held_run.run_held_node(kernel_sim.kernel.id, owner, body)
    assert no_session.value.status_code == 403 and "no notebook session" in no_session.value.detail
    kernel_runner._sessions[coded_flow.flow_id] = {kernel_sim.kernel.id: None}
    with pytest.raises(HTTPException) as not_owner:
        held_run.run_held_node(kernel_sim.kernel.id, other, body)
    assert not_owner.value.status_code == 403
    with pytest.raises(HTTPException) as elsewhere:
        held_run.run_held_node(
            kernel_sim.kernel.id,
            owner,
            body.model_copy(update={"inputs": [held_run.HeldInput(node_id=1, path="/etc/passwd")]}),
        )
    assert elsewhere.value.status_code == 422 and "not a file of this session" in elsewhere.value.detail
    pivot = {"id": 9, "type": "pivot", "setting_input": {}}
    with pytest.raises(HTTPException) as refused:
        held_run.run_held_node(kernel_sim.kernel.id, owner, body.model_copy(update={"node": pivot}))
    assert refused.value.status_code == 422 and "cannot run from a cell: Push" in refused.value.detail


def test_a_held_run_leaves_no_logger_log_file_or_folder(coded_flow, client, kernel_sim, monkeypatch):
    from flowfile_core.notebook import held_run

    ids: list[int] = []
    free = held_run._free_flow_id
    monkeypatch.setattr(held_run, "_free_flow_id", lambda: (ids.append(free()), ids[-1])[1])
    cell = _bind(_coded_id(coded_flow)) + "g = ff.Gate(coded, formula='[amount] > 0')\ndisplay(g.then)"
    assert _execute(client, coded_flow, kernel_sim, cell)["success"]
    assert len(ids) == 1
    for scratch_id in ids:
        assert FlowLogger.get_instance(scratch_id) is None
        assert not get_flow_log_file(scratch_id).exists()
        assert not Path(kernel_sim.shared_volume_path, str(scratch_id)).exists()


def test_a_database_read_a_cell_built_is_run_by_core_over_the_stored_connection(
    coded_flow, client, kernel_sim, kernel_db_opens, tmp_path
):
    """The kernel holds neither the connection nor a key: the placement check, the columns (``schema_only``) and the
    read itself are core's, and the kernel opens no catalog connection."""
    path = tmp_path / "held.db"
    with sqlite3.connect(path) as db:
        db.execute("create table movies (id integer, title text)")
        db.executemany("insert into movies values (?, ?)", [(1, "The Matrix"), (2, "Inception")])
    connection = input_schema.FullDatabaseConnection(
        connection_name="notebook_held_sqlite", database_type="sqlite", username="", password=SecretStr(""),
        database=str(path),
    )  # fmt: skip
    with get_db_context() as db:
        if get_database_connection(db, connection.connection_name, NOTEBOOK_OWNER_ID) is None:
            store_database_connection(db, connection, user_id=NOTEBOOK_OWNER_ID)
    cell = "rows = ff.read_database('notebook_held_sqlite', table_name='movies')\nprint(rows.columns)\ndisplay(rows)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert shown["stdout"].strip() == "['id', 'title']", shown
    assert [row["title"] for row in _rows(shown)] == ["The Matrix", "Inception"], shown["display_outputs"]
    runs = [(run["node"]["type"], run["schema_only"]) for run in kernel_sim.node_runs]
    assert runs == [("database_reader", True), ("database_reader", False)], runs
    assert "placement_refusal" in {body["kind"] for body in kernel_sim.lookups}
    assert not kernel_db_opens, f"a kernel op opened the catalog database from {sorted(set(kernel_db_opens))}"


def test_a_cloud_read_a_cell_built_is_held_and_read_by_core(coded_flow, client, kernel_sim, kernel_db_opens, tmp_path, monkeypatch):
    """A cloud reader is deferred in a kernel session (it resolves a stored connection), so core predicts its columns
    and reads it; the kernel never opens the source itself."""
    csv = tmp_path / "cloud.csv"
    csv.write_text("a;b\n1;x\n2;y\n")
    in_kernel: list[bool] = []
    original = FlowDataEngine.from_cloud_storage_obj

    def spied(*args, **kwargs):
        in_kernel.append(IN_KERNEL_OP.get())
        return original(*args, **kwargs)

    monkeypatch.setattr(FlowDataEngine, "from_cloud_storage_obj", spied)
    cell = f"rows = ff.read_from_cloud_storage({str(csv)!r}, file_format='csv')\nprint(rows.columns)\ndisplay(rows)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert shown["stdout"].strip() == "['a', 'b']", shown
    assert _rows(shown) == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], shown["display_outputs"]
    runs = [(run["node"]["type"], run["schema_only"]) for run in kernel_sim.node_runs]
    assert runs == [("cloud_storage_reader", True), ("cloud_storage_reader", False)], runs
    assert in_kernel and not any(in_kernel), "core opened the source, the kernel never did"
    assert not kernel_db_opens, f"a kernel op opened the catalog database from {sorted(set(kernel_db_opens))}"


def test_a_held_run_commits_no_source_progress(coded_flow, client, kernel_sim, monkeypatch):
    from flowfile_core.flowfile.flow_graph import FlowGraph

    committed: list[int] = []
    original = FlowGraph._run_post_execution_callbacks

    def spy(self, *args, **kwargs):
        committed.append(self.flow_id)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FlowGraph, "_run_post_execution_callbacks", spy)
    cell = _bind(_coded_id(coded_flow)) + "g = ff.Gate(coded, formula='[amount] > 0')\ndisplay(g.then)"
    assert _execute(client, coded_flow, kernel_sim, cell)["success"]
    assert len(kernel_sim.node_runs) == 1 and committed == []


def test_a_keyed_node_fed_both_exits_of_a_split_reads_each(coded_flow, client, kernel_sim):
    """Both exits of a split the kernel computed feed one held ``run_flow``: the session sends one input per edge
    (not per source), core serves each exit under its own handle, and the child sees them apart."""
    from uuid import uuid4

    import flowfile as ff

    catalog = ff.CatalogReference(f"NbSplit_{uuid4().hex[:8]}", auto_create=True)
    child = ff.create_flow_graph()
    big = ff.FlowInput("big", schema={"id": ff.Int64, "amount": ff.Int64}, flow_graph=child)
    small = ff.FlowInput("small", schema={"id": ff.Int64, "amount": ff.Int64}, flow_graph=child)
    ff.concat([big, small]).to_flow_output("joined")
    ref = catalog.schema("flows", auto_create=True).register_flow(big, name=f"child_{uuid4().hex[:8]}")

    cell = _bind(_coded_id(coded_flow)) + (
        f"ref = ff.flow_ref(registration_id={ref.registration_id})\n"
        "big, small = coded.filter_split(ff.col('amount') > 150)\n"
        "run = ff.RunFlow(ref, big=big, small=small)\n"
        "display(run['joined'])"
    )
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert sorted(row["amount"] for row in _rows(shown)) == [100, 200, 300], shown["display_outputs"]
    [run] = [run for run in kernel_sim.node_runs if not run["schema_only"]]
    edges = sorted((entry["node_id"], entry["handle"]) for entry in run["inputs"])
    assert len({source for source, _ in edges}) == 1, edges
    assert [handle for _, handle in edges] == ["output-0", "output-1"], edges
