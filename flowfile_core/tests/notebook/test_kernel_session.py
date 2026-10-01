"""The canvas notebook on a notebook kernel, through the ``kernel-sim`` manager (no Docker): the session routes,
a cell as real Python, reset, the mode gate, and an unedited corpus push through the kernel's clean run."""

from __future__ import annotations

import json

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


def _execute(client, flow, kernel_sim, code: str, cell_id: str = "cell-1") -> dict:
    response = client.post("/notebook/session/execute", json=_body(flow, kernel_sim, cell_id=cell_id, code=code))
    assert response.status_code == 200, response.text
    return response.json()


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


def test_stopping_the_kernel_forgets_its_sessions_and_database_copy(orders_flow, client, kernel_sim):
    from flowfile_core.kernel import notebook_db
    from flowfile_core.notebook import kernel_runner

    assert client.post("/notebook/session/open", json=_body(orders_flow, kernel_sim)).status_code == 200
    copy = notebook_db.copy_path(kernel_sim.shared_volume_path, kernel_sim.kernel.id)
    assert copy.exists()
    kernel_runner.forget_kernel(kernel_sim.kernel.id, kernel_sim.shared_volume_path)
    assert orders_flow.flow_id not in kernel_runner._sessions
    assert not copy.parent.exists()


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
