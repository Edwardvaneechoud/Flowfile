"""``POST /editor/notebook/push/``, ``POST /notebook/plan`` and ``POST /editor/notebook/run_lineage/`` (plan 2.5, 2.7).

The clean run goes through the test-only in-process runner (``InProcessCleanRunner``), installed per test.
"""

import pytest
from fastapi.testclient import TestClient

import flowfile as fl
from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.notebook import bridge
from flowfile_core.notebook.push import push_refusals
from flowfile_core.notebook.render import code_fingerprint, render

OWNER_ID = 1


@pytest.fixture
def flag():
    before = bool(settings.FEATURE_FLAG_CANVAS_NOTEBOOK)
    settings.FEATURE_FLAG_CANVAS_NOTEBOOK.set(True)
    yield settings.FEATURE_FLAG_CANVAS_NOTEBOOK
    settings.FEATURE_FLAG_CANVAS_NOTEBOOK.set(before)


@pytest.fixture
def runner():
    before = bridge._runner
    bridge.set_clean_runner(bridge.InProcessCleanRunner())
    yield
    bridge.set_clean_runner(before)


@pytest.fixture
def client_as():
    def _as(user_id: int) -> TestClient:
        user = PydanticUser(username=f"nb_{user_id}", id=user_id, disabled=False, is_admin=user_id == OWNER_ID)
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        main.app.dependency_overrides[get_current_user] = lambda: user
        return TestClient(main.app)

    yield _as
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def open_as(tmp_path):
    """Open a frame-built flow the way the editor does (save, then ``open_flow``, history on) for ``user_id``."""
    opened = []

    def _open(graph, user_id=OWNER_ID):
        path = tmp_path / f"flow_{len(opened)}.yaml"
        graph.save_flow(str(path))
        graph = open_flow(path)
        flow_file_handler._flows[graph.flow_id] = graph
        flow_file_handler._register_user_session(user_id, graph.flow_id)
        opened.append((graph.flow_id, user_id))
        return graph

    yield _open
    for flow_id, user_id in opened:
        flow_file_handler._unregister_user_session(user_id, flow_id)
        flow_file_handler.delete_flow(flow_id)


@pytest.fixture
def orders_flow(open_as):
    orders = fl.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    result = orders.filter(fl.col("amount") > 10).with_columns((fl.col("amount") * 2).alias("double"))
    return open_as(result.flow_graph)


def _body(graph, edit=None, changed=()):
    """Push body from the live rendering; ``edit(cells) -> cells`` rewrites cell text."""
    rendering = render(graph)
    cells = {cell.cell_id: cell.code for cell in rendering.cells}
    if edit is not None:
        cells = edit(cells)
    return {
        "flow_id": graph.flow_id,
        "cells": [[cell_id, code] for cell_id, code in cells.items()],
        "changed_cell_ids": list(changed),
        "provenance": {
            cell.cell_id: [[graph.get_node(n).node_type, n] for n in cell.node_ids]
            for cell in rendering.cells
            if cell.node_ids
        },
        "code_fingerprint": rendering.code_fingerprint,
        "client_max_node_id": max(n.node_id for n in graph.nodes),
    }


def _node_of_type(graph, node_type):
    return next(n for n in graph.nodes if n.node_type == node_type)


def _raise_threshold(cells):
    filter_cell = next(k for k, v in cells.items() if ".filter(" in v)
    return {**cells, filter_cell: cells[filter_cell].replace("> 10", "> 20")}


def test_push_one_filter_edit_is_one_update_and_one_undo_step(flag, runner, orders_flow, client_as):
    client = client_as(OWNER_ID)
    graph = orders_flow
    source, filt, formula = (_node_of_type(graph, t) for t in ("manual_input", "filter", "formula"))
    hashes = {n.node_id: n.hash for n in graph.nodes}
    formula_settings = formula.setting_input.model_dump(exclude={"user_id"})
    fingerprint = code_fingerprint(graph)
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]
    body = _body(graph, _raise_threshold, changed=[f"node-{filt.node_id}"])

    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200, plan.text
    assert [(op["op"], op["settings"]["node_id"]) for op in plan.json()["operations"]] == [
        ("update_settings", filt.node_id)
    ]

    response = client.post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    pushed = response.json()
    assert "20" in graph.get_node(filt.node_id).setting_input.filter_input.advanced_filter
    assert pushed["code_fingerprint"] == code_fingerprint(graph) != fingerprint
    assert pushed["max_node_id"] == max(n.node_id for n in graph.nodes)
    assert pushed["node_ids_by_cell"][f"node-{filt.node_id}"] == [filt.node_id]
    assert pushed["history"]["undo_count"] == undo_before + 1
    assert graph.get_node(source.node_id).hash == hashes[source.node_id]
    assert graph.get_node(formula.node_id).setting_input.model_dump(exclude={"user_id"}) == formula_settings
    assert graph.get_node(formula.node_id).hash != hashes[formula.node_id]

    assert client.post("/editor/undo/", params={"flow_id": graph.flow_id}).status_code == 200
    assert code_fingerprint(graph) == fingerprint


def test_push_with_nothing_changed_applies_nothing(flag, runner, orders_flow, client_as):
    client = client_as(OWNER_ID)
    undo_before = client.get("/editor/history_status/", params={"flow_id": orders_flow.flow_id}).json()["undo_count"]
    response = client.post("/editor/notebook/push/", json=_body(orders_flow))
    assert response.status_code == 200, response.text
    assert response.json()["history"]["undo_count"] == undo_before
    assert response.json()["code_fingerprint"] == code_fingerprint(orders_flow)


def test_push_refuses_a_stale_fingerprint_with_the_live_one(flag, runner, orders_flow, client_as):
    body = {**_body(orders_flow, _raise_threshold), "code_fingerprint": "0" * 64}
    response = client_as(OWNER_ID).post("/editor/notebook/push/", json=body)
    assert response.status_code == 409
    assert response.json()["detail"]["code_fingerprint"] == code_fingerprint(orders_flow)


def test_push_refuses_a_failing_cell_and_leaves_the_canvas(flag, runner, orders_flow, client_as):
    fingerprint = code_fingerprint(orders_flow)

    def broken(cells):
        return {**cells, "node-99": "raise ValueError('boom')"}

    response = client_as(OWNER_ID).post("/editor/notebook/push/", json=_body(orders_flow, broken, ["node-99"]))
    assert response.status_code == 422 and "boom" in response.json()["detail"]
    assert code_fingerprint(orders_flow) == fingerprint


def test_push_refuses_an_in_memory_lazy_frame(flag, runner, orders_flow, client_as):
    def lazy(cells):
        return {**cells, "node-99": "import polars as pl\nextra = fl.FlowFrame(pl.LazyFrame({'x': [1]}))"}

    response = client_as(OWNER_ID).post("/editor/notebook/push/", json=_body(orders_flow, lazy, ["node-99"]))
    assert response.status_code == 422, response.text
    assert "LazyFrame" in response.json()["detail"]


def test_refusals_name_custom_classes_inline_rest_secrets_and_lazy_frames():
    live = {"nodes": [{"id": 1, "type": "polars_lazy_frame", "setting_input": None}]}
    rest = {"rest_api_settings": {"auth": {"auth_type": "bearer", "secret_name": "", "secret": None}}}
    session = {
        "nodes": [
            {"id": 2, "type": "cell_node", "setting_input": {"is_user_defined": True, "settings": {}}},
            {"id": 3, "type": "rest_api_reader", "setting_input": rest},
            {
                "id": 4,
                "type": "rest_api_reader",
                "setting_input": {"rest_api_settings": {"auth": {"auth_type": "none"}}},
            },
        ]
    }
    refusals = push_refusals(live, session, installed=lambda node_type: False)
    assert len(refusals) == 3
    assert "Node 1" in refusals[0] and "LazyFrame" in refusals[0]
    assert "cell_node" in refusals[1] and "fl.custom_nodes.install" in refusals[1]
    assert "Node 3" in refusals[2] and "secret_name" in refusals[2]
    assert push_refusals(live, session, installed=lambda node_type: True)[1:] == refusals[2:]


def test_push_is_503_without_a_runner_or_the_flag(flag, orders_flow, client_as):
    before = bridge._runner
    bridge.set_clean_runner(None)
    try:
        response = client_as(OWNER_ID).post("/editor/notebook/push/", json=_body(orders_flow, _raise_threshold))
        assert response.status_code == 503 and response.json()["detail"] == bridge.NO_RUNNER_DETAIL
    finally:
        bridge.set_clean_runner(before)
    flag.set(False)
    assert client_as(OWNER_ID).post("/editor/notebook/push/", json=_body(orders_flow)).status_code == 503
    assert client_as(OWNER_ID).post("/notebook/plan", json=_body(orders_flow)).status_code == 503


def test_user_2_cannot_push_plan_or_run_user_3s_flow(flag, runner, open_as, client_as):
    graph = open_as(fl.from_dict({"a": [1, 2]}).filter(fl.col("a") > 1).flow_graph, user_id=3)
    fingerprint = code_fingerprint(graph)
    body = _body(graph, _raise_threshold)
    intruder = client_as(2)
    assert intruder.post("/editor/notebook/push/", json=body).status_code == 404
    assert intruder.post("/notebook/plan", json=body).status_code == 404
    lineage = {"flow_id": graph.flow_id, "node_id": _node_of_type(graph, "filter").node_id}
    assert intruder.post("/editor/notebook/run_lineage/", json=lineage).status_code == 404
    assert code_fingerprint(graph) == fingerprint
    owner = client_as(3).post("/notebook/plan", json=body)
    assert owner.status_code == 200, owner.text


def _gated_writer(tmp_path, default):
    mode = fl.Parameter("mode", default=default, type="enum", enum_values=["full", "quick"])
    source = fl.from_dict({"region": ["N", "S"], "amount": [1.0, 2.0]})
    fl.add_flow_parameter(source, mode)
    gate = fl.Gate(source, parameter=mode, value="full")
    target = tmp_path / f"written_{default}.csv"
    gate.then.write_csv(str(target))
    return gate.then.flow_graph, target


@pytest.mark.parametrize("default, written", [("quick", False), ("full", True)])
def test_run_lineage_honours_a_closed_gate(flag, open_as, client_as, tmp_path, default, written):
    graph, target = _gated_writer(tmp_path, default)
    assert not target.exists()
    open_as(graph)
    writer = _node_of_type(graph, "output")
    response = client_as(OWNER_ID).post(
        "/editor/notebook/run_lineage/", json={"flow_id": graph.flow_id, "node_id": writer.node_id}
    )
    assert response.status_code == 200, response.text
    assert set(response.json()["node_ids"]) == {n.node_id for n in graph.nodes}
    status = client_as(OWNER_ID).get("/flow/run_status/", params={"flow_id": graph.flow_id})
    assert status.status_code == 200 and status.json()["success"] is True
    assert target.exists() is written


def test_run_lineage_runs_only_the_ancestors(flag, open_as, client_as):
    source = fl.from_dict({"a": [1, 2, 3]})
    kept = source.filter(fl.col("a") > 1)
    source.sort("a")
    graph = open_as(kept.flow_graph)
    response = client_as(OWNER_ID).post(
        "/editor/notebook/run_lineage/",
        json={"flow_id": graph.flow_id, "node_id": _node_of_type(graph, "filter").node_id},
    )
    assert response.status_code == 200, response.text
    ran = {
        r["node_id"]
        for r in client_as(OWNER_ID)
        .get("/flow/run_status/", params={"flow_id": graph.flow_id})
        .json()["node_step_result"]
    }
    assert ran == {_node_of_type(graph, "manual_input").node_id, _node_of_type(graph, "filter").node_id}


def test_set_flow_parameters_applies_outside_undo_and_rolls_back_with_the_batch(orders_flow, client_as):
    client = client_as(OWNER_ID)
    graph = orders_flow
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]
    set_params = {"op": "set_flow_parameters", "parameters": [{"name": "limit", "default_value": "5"}]}

    failing = [set_params, {"op": "delete_connection", "connection": _missing_connection()}]
    response = client.post(
        "/editor/apply_operations/", json={"flow_id": graph.flow_id, "label": "x", "operations": failing}
    )
    assert response.status_code == 422, response.text
    assert graph.flow_settings.parameters == []

    response = client.post(
        "/editor/apply_operations/", json={"flow_id": graph.flow_id, "label": "x", "operations": [set_params]}
    )
    assert response.status_code == 200, response.text
    assert [(p.name, p.default_value) for p in graph.flow_settings.parameters] == [("limit", "5")]
    assert response.json()["history"]["undo_count"] == undo_before


def _missing_connection():
    return {
        "output_connection": {"node_id": 998, "connection_class": "output-0"},
        "input_connection": {"node_id": 999, "connection_class": "input-0"},
    }
