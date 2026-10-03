"""The canvas notebook on a notebook kernel, through the ``kernel-sim`` manager (no Docker): the session routes,
a cell as real Python, reset, the mode gate, and an unedited corpus push through the kernel's clean run."""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path

import pytest

from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.notebook.push import NotebookPushRequest, plan_push
from flowfile_core.notebook.render import render
from shared.notebook_display import TABLE_MIME
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, cell_provenance
from tests.notebook.test_ledger import grade

LOOPBACK = ("127.0.0.1", 50123)

LOOP_CELL = """import re

frames = sorted(n for n, v in list(globals().items()) if type(v).__name__ == "FlowFrame")
for name in frames:
    if re.fullmatch(r"[a-z_0-9]+", name):
        print("frame", name)
last = globals()[frames[-1]]
display(last)
last
"""


@pytest.fixture
def client(client_as, kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


def _body(flow, kernel_sim, **extra) -> dict:
    return {"flow_id": flow.flow_id, "kernel_id": kernel_sim.kernel.id, **extra}


def _execute(client, flow, kernel_sim, code: str, cell_id: str = "cell-1", **extra) -> dict:
    body = _body(flow, kernel_sim, cell_id=cell_id, code=code, **extra)
    response = client.post("/notebook/session/execute", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def _op(request) -> str:
    return re.search(r'"op": "(\w+)"', request.code).group(1)


def test_a_cell_with_an_import_and_a_loop_runs_in_the_session(orders_flow, client, kernel_sim):
    opened = client.post("/notebook/session/open", json=_body(orders_flow, kernel_sim))
    assert opened.status_code == 200, opened.text
    assert opened.json() == {"status": "ready"}

    result = _execute(client, orders_flow, kernel_sim, LOOP_CELL)
    assert result["success"], result
    assert "frame " in result["stdout"] and "@@" not in result["stdout"]
    mimes = [out["mime_type"] for out in result["display_outputs"]]
    assert mimes == [TABLE_MIME, "text/plain"], mimes
    assert result["namespace_generation"] and result["revision"] == 1

    schemas = client.post("/notebook/session/schemas", json=_body(orders_flow, kernel_sim)).json()
    assert schemas["state"] == "ready"
    assert "last" in {frame["name"] for frame in schemas["dataframes"]}


def test_execute_opens_the_session_and_reports_a_failing_line(orders_flow, client, kernel_sim):
    result = _execute(client, orders_flow, kernel_sim, "x = 1\nraise ValueError('boom')")
    assert not result["success"]
    assert result["error"].startswith("Traceback") and result["error"].rstrip().endswith("ValueError: boom")
    assert "line 2" in result["error"] and result["line"] == 2
    assert "notebook_cells.py" not in result["error"]
    assert "Traceback" not in result["stderr"]


def test_reset_drops_the_sessions_variables(orders_flow, client, kernel_sim):
    from flowfile_core.notebook.kernel_runner import kernel_flow_id

    assert _execute(client, orders_flow, kernel_sim, "x = 41")["success"]
    assert _execute(client, orders_flow, kernel_sim, "print(x + 1)")["stdout"].strip() == "42"
    kernel_namespace = kernel_sim.namespaces[kernel_flow_id(orders_flow.flow_id)]
    assert kernel_namespace["x"] == 41
    reset = client.post("/notebook/session/reset", json=_body(orders_flow, kernel_sim))
    assert reset.status_code == 200 and reset.json() == {"status": "cleared"}
    assert "x" not in kernel_namespace and "ff" in kernel_namespace
    result = _execute(client, orders_flow, kernel_sim, "x")
    assert not result["success"] and "NameError" in result["error"]


def test_docker_mode_refuses_kernel_sessions(orders_flow, client, kernel_sim, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    assert client.get("/notebook/status").json() == {"kernel_sessions": False}
    response = client.post("/notebook/session/execute", json=_body(orders_flow, kernel_sim, cell_id="c", code="1"))
    assert response.status_code == 403
    assert not kernel_sim.requests


def test_a_remote_caller_is_refused(orders_flow, client_as, kernel_sim):
    remote = client_as(NOTEBOOK_OWNER_ID, client=("192.168.1.20", 50000))
    assert remote.post("/notebook/session/open", json=_body(orders_flow, kernel_sim)).status_code == 403


def test_an_unedited_corpus_push_through_the_kernel_changes_nothing(notebook_corpus, corpus_runs, kernel_sim):
    owner = PydanticUser(username="nb_kernel", id=NOTEBOOK_OWNER_ID, disabled=False, is_admin=True)
    exec_runs = corpus_runs["exec"].runs
    stray, pushed = {}, []
    for name, graph in notebook_corpus:
        run = exec_runs[name]
        if run.result.error:
            continue
        grades = grade(graph, {"flowfile_data": run.result.flowfile_data, "cells": run.result.node_ids_by_cell})
        if "LOSSY" in grades.values():
            continue
        rendering = render(graph)
        request = NotebookPushRequest(
            flow_id=graph.flow_id,
            cells=[(cell.cell_id, cell.code) for cell in rendering.cells],
            provenance=cell_provenance(graph, rendering),
            code_fingerprint=rendering.code_fingerprint,
            client_max_node_id=max((node.node_id for node in graph.nodes), default=0),
            kernel_id=kernel_sim.kernel.id,
        )
        plan, _ = plan_push(graph, owner, request)
        pushed.append(name)
        if plan.operations:
            stray[name] = [op.model_dump(mode="json") for op in plan.operations]
    assert not stray, json.dumps(stray, indent=1, default=str)
    assert len(pushed) > 10, pushed
    assert sum('"op": "clean_run"' in r.code for r in kernel_sim.requests) == len(pushed)


def test_a_notebook_kernel_refuses_to_open_a_catalog_database(tmp_path, monkeypatch):
    """In a kernel (``FLOWFILE_KERNEL_ID`` set) the catalog engine fails every connection before pysqlite would
    create the file; without the variable (a script, the kernel-sim) nothing is installed, and core's own engine
    is never touched."""
    from sqlalchemy import create_engine, event, text
    from sqlalchemy.orm import sessionmaker

    from flowfile_core.database import connection
    from flowfile_frame import notebook_kernel
    from shared import database as shared_database

    path = tmp_path / "flowfile_catalog.db"
    engine = create_engine(f"sqlite:///{path}")
    monkeypatch.setattr(shared_database, "get_catalog_engine", lambda url=None: engine)
    try:
        monkeypatch.delenv("FLOWFILE_KERNEL_ID", raising=False)
        notebook_kernel._refuse_database()
        assert not event.contains(engine, "do_connect", notebook_kernel._refuse_connect)

        monkeypatch.setenv("FLOWFILE_KERNEL_ID", "k")
        notebook_kernel._refuse_database()
        notebook_kernel._refuse_database()
        with pytest.raises(RuntimeError, match="no catalog database") as refused:
            engine.connect()
        assert "ff.list_catalogs" in str(refused.value)
        with pytest.raises(RuntimeError, match="no catalog database"):
            sessionmaker(bind=engine)().execute(text("SELECT 1"))
        assert not path.exists()
    finally:
        engine.dispose()
    assert not event.contains(connection.engine, "do_connect", notebook_kernel._refuse_connect)


def test_stopping_the_kernel_forgets_its_sessions(orders_flow, client, kernel_sim):
    from flowfile_core.notebook import kernel_runner

    assert client.post("/notebook/session/open", json=_body(orders_flow, kernel_sim)).status_code == 200
    assert any(key[0] == kernel_sim.kernel.id for key in kernel_runner._verified)
    kernel_runner.forget_kernel(kernel_sim.kernel.id, kernel_sim.shared_volume_path)
    assert orders_flow.flow_id not in kernel_runner._sessions
    assert not any(key[0] == kernel_sim.kernel.id for key in kernel_runner._verified)


def test_an_unconfigured_node_on_the_canvas_keeps_the_session_usable(open_as, client, kernel_sim):
    from flowfile_core.flowfile import flow_graph as graph_module
    from flowfile_core.schemas import input_schema

    import flowfile as ff

    source = ff.from_dict({"id": [1, 2, 3]})
    graph = source.flow_graph
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=90, node_type="filter"))
    graph_module.add_connection(graph, input_schema.NodeConnection.create_from_simple_input(source.node_id, 90))
    flow = open_as(graph)

    for route in ("open", "reset"):
        response = client.post(f"/notebook/session/{route}", json=_body(flow, kernel_sim))
        assert response.status_code == 200, response.text
    cells = [cell for cell in render(flow).cells if cell.kind in ("imports", "node")]
    for cell in cells:
        assert _execute(client, flow, kernel_sim, cell.code)["success"], cell.code
    result = _execute(client, flow, kernel_sim, "print(filtered_90.columns)")
    assert result["success"] and result["stdout"].strip() == "[]", result


def test_a_cell_runs_as_its_own_node_and_every_other_op_as_node_0(orders_flow, client, kernel_sim):
    """The kernel clears a node's artifacts before every call it runs as that node, so what a cell publishes must
    outlive the calls that follow it, such as the editor's schemas refresh."""
    assert _execute(client, orders_flow, kernel_sim, "x = 1", node_id=1234)["success"]
    assert client.post("/notebook/session/schemas", json=_body(orders_flow, kernel_sim)).status_code == 200
    ops = [(_op(request), request.node_id) for request in kernel_sim.requests]
    assert ops == [("hello", 0), ("open", 0), ("execute", 1234), ("schemas", 0)], ops


def test_a_kernel_whose_flowfile_has_another_version_is_refused(orders_flow, client, kernel_sim, monkeypatch):
    from flowfile_core.notebook import kernel_runner
    from flowfile_frame import notebook_kernel

    monkeypatch.setattr(notebook_kernel, "_hello", lambda request: {"ok": True, "version": "0.0.0"})
    response = client.post("/notebook/session/execute", json=_body(orders_flow, kernel_sim, cell_id="c", code="1"))
    assert response.status_code == 409 and "0.0.0" in response.json()["detail"], response.text
    assert not kernel_runner._verified


class _KernelClient:
    """The kernel runtime's ``flowfile_client`` as far as a session's displays use it: its own list of displays."""

    def __init__(self) -> None:
        self.shown: list[dict] = []

    def _get_displays(self) -> list[dict]:
        return self.shown

    def display(self, obj, title: str = "") -> None:
        self.shown.append({"mime_type": "text/plain", "data": str(obj), "title": title})

    explore = display


def test_displays_come_in_the_order_the_cell_made_them(orders_flow, client, kernel_sim, monkeypatch):
    from flowfile_frame import notebook_kernel

    kernel = _KernelClient()
    monkeypatch.setattr(notebook_kernel, "_kernel_client", lambda: kernel)
    code = (
        'frames = sorted(n for n, v in list(globals().items()) if type(v).__name__ == "FlowFrame")\n'
        "display('first', title='note')\n"
        "display(globals()[frames[-1]])\n"
        "explore('last')\n"
    )
    result = _execute(client, orders_flow, kernel_sim, code)
    assert result["success"], result
    shown = [
        (out["mime_type"], out["data"] if out["mime_type"] == "text/plain" else None, out["title"])
        for out in result["display_outputs"]
    ]
    assert shown == [("text/plain", "first", "note"), (TABLE_MIME, None, ""), ("text/plain", "last", "")], shown
    assert kernel.shown == []


def test_a_close_that_lands_after_the_flow_reopened_leaves_the_new_session(
    orders_flow, client, kernel_sim, monkeypatch
):
    """Closing a flow closes its sessions in the background; the flow open again by the time the kernel takes the
    close (the store runs the next cell without opening a session), the cell runs in a new session and the close
    leaves it and the flow's canvas results be. A close of that new session then removes both."""
    from flowfile_core.notebook import kernel_runner
    from flowfile_frame import notebook_kernel

    close = kernel_runner._close_in_kernels
    queued, ready = [], threading.Event()
    monkeypatch.setattr(kernel_runner, "_close_in_kernels", lambda *args: (queued.append(args), ready.set()))

    def close_flow() -> tuple:
        queued.clear()
        ready.clear()
        kernel_runner.close_flow_sessions(orders_flow.flow_id)
        assert ready.wait(5)
        return queued[0]

    assert _execute(client, orders_flow, kernel_sim, "x = 1")["success"]
    results = Path(kernel_runner._results_dir(kernel_sim, orders_flow.flow_id))
    results.mkdir(parents=True)
    (results / "canvas.parquet").touch()
    stale = close_flow()

    assert _execute(client, orders_flow, kernel_sim, "y = 2")["success"]
    close(*stale)
    assert _execute(client, orders_flow, kernel_sim, "print(y)")["stdout"].strip() == "2"
    assert "NameError" in _execute(client, orders_flow, kernel_sim, "x")["error"]
    assert (results / "canvas.parquet").exists()

    close(*close_flow())
    assert orders_flow.flow_id not in notebook_kernel._SESSIONS
    assert not results.exists()


def test_a_flows_session_namespace_is_apart_from_every_catalog_notebooks():
    from flowfile_core.notebook.kernel_runner import kernel_flow_id

    assert kernel_flow_id(7) == -(1 << 40) - 7
    assert kernel_flow_id(0xFFFFFFFF) < -1_600_000_000


def test_the_kernel_mirrors_cores_custom_node_files(tmp_path, monkeypatch):
    """``_mirror_custom_nodes`` writes the node files core lists into the kernel's own nodes folder, rewrites one whose
    hash changed, removes only the files it wrote, and leaves an unchanged file alone."""
    import hashlib

    from flowfile_core.flowfile.user_defined.registry import registry
    from flowfile_frame import _metadata, notebook_kernel
    from test_utils.notebook_demo import MOOD_EMOJI

    def listed(source: str) -> list:
        return [_metadata.CustomNodeSource("mood_emoji", source, hashlib.sha256(source.encode()).hexdigest())]

    source = MOOD_EMOJI.read_text()
    answer = listed(source)
    monkeypatch.setattr(_metadata, "custom_node_sources", lambda: answer)
    monkeypatch.setattr(notebook_kernel, "_MIRRORED", {})
    original = registry._directory
    monkeypatch.setattr(registry, "_directory", tmp_path / "nodes")
    try:
        registry.scan()
        path = tmp_path / "nodes" / "mood_emoji.py"
        notebook_kernel._mirror_custom_nodes()
        assert path.read_text() == source and registry.get("mood_emoji") is not None
        written = path.stat().st_mtime_ns
        notebook_kernel._mirror_custom_nodes()
        assert path.stat().st_mtime_ns == written, "an unchanged file is not rewritten"

        changed = source + "\n# edited\n"
        answer[:] = listed(changed)
        notebook_kernel._mirror_custom_nodes()
        assert path.read_text() == changed and registry.get("mood_emoji").source_text == changed

        foreign = tmp_path / "nodes" / "foreign.py"
        foreign.write_text("# not a node\n")
        answer[:] = []
        notebook_kernel._mirror_custom_nodes()
        assert not path.exists() and foreign.exists(), "only mirrored files are removed"
        assert registry.get("mood_emoji") is None
    finally:
        registry._directory = original
        registry.scan()
