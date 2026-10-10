"""``POST /editor/notebook/push/``, ``POST /notebook/plan`` and ``POST /editor/notebook/run_lineage/``.

The clean run goes through the production runner (``runner``) or, where a test says so, its test-only
``exec`` twin (``exec_runner``), installed per test.
"""

import re
import sys
from contextlib import contextmanager

import pytest

import flowfile as ff
from flowfile_core import events
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRun
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.flowfile.util.layout.placement import node_box
from flowfile_core.notebook import bridge
from flowfile_core.notebook.push import needs_confirmation, refused_nodes
from flowfile_core.notebook.reconcile import ReconcilePlan
from flowfile_core.notebook.render import code_fingerprint, render
from flowfile_core.notebook.runner import NotebookRunner
from flowfile_core.routes import routes as editor_routes
from flowfile_core.schemas import input_schema
from tests.notebook.conftest import ExecRunner
from tests.notebook.corpus import build_drawer_script, build_python_script_cells

OWNER_ID = 1


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


def _cell_of(graph, node_id):
    return next(cell.cell_id for cell in render(graph).cells if node_id in cell.node_ids)


def _node_of_type(graph, node_type):
    return next(n for n in graph.nodes if n.node_type == node_type)


def _raise_threshold(cells):
    filter_cell = next(k for k, v in cells.items() if ".filter(" in v)
    return {**cells, filter_cell: cells[filter_cell].replace("> 10", "> 20")}


def test_push_one_filter_edit_is_one_update_and_one_undo_step(runner, orders_flow, client_as):
    client = client_as(OWNER_ID)
    graph = orders_flow
    source, filt, formula = (_node_of_type(graph, t) for t in ("manual_input", "filter", "formula"))
    hashes = {n.node_id: n.hash for n in graph.nodes}
    formula_settings = formula.setting_input.model_dump(exclude={"user_id"})
    fingerprint = code_fingerprint(graph)
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]
    cell_id = _cell_of(graph, filt.node_id)
    body = _body(graph, _raise_threshold, changed=[cell_id])

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
    assert filt.node_id in pushed["node_ids_by_cell"][cell_id]
    assert pushed["history"]["undo_count"] == undo_before + 1
    assert graph.get_node(source.node_id).hash == hashes[source.node_id]
    assert graph.get_node(formula.node_id).setting_input.model_dump(exclude={"user_id"}) == formula_settings
    assert graph.get_node(formula.node_id).hash != hashes[formula.node_id]

    assert client.post("/editor/undo/", params={"flow_id": graph.flow_id}).status_code == 200
    assert code_fingerprint(graph) == fingerprint


def test_push_with_nothing_changed_applies_nothing(runner, orders_flow, client_as):
    client = client_as(OWNER_ID)
    undo_before = client.get("/editor/history_status/", params={"flow_id": orders_flow.flow_id}).json()["undo_count"]
    response = client.post("/editor/notebook/push/", json=_body(orders_flow))
    assert response.status_code == 200, response.text
    assert response.json()["history"]["undo_count"] == undo_before
    assert response.json()["code_fingerprint"] == code_fingerprint(orders_flow)


def test_an_edited_explore_cell_keeps_the_charts_saved_on_the_canvas(runner, open_as, client_as):
    """``ff.explore`` carries no charts, so pushing its cell keeps the ones the designer saved on the node."""
    client = client_as(OWNER_ID)
    charts = [{"name": "Chart 1", "visId": "gw_1"}]
    big = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]}).filter(ff.col("amount") > 10)
    ff.Node("explore_data", big, settings={"graphic_walker_input": {"is_initial": False, "specList": charts}})
    graph = open_as(big.flow_graph)
    explore = _node_of_type(graph, "explore_data")
    cell_id = _cell_of(graph, explore.node_id)
    code = f"ff.explore(filtered_{big.node_id})"
    assert {cell.cell_id: cell.code for cell in render(graph).cells}[cell_id] == code

    unchanged = client.post("/notebook/plan", json=_body(graph, changed=[cell_id]))
    assert unchanged.status_code == 200, unchanged.text
    assert unchanged.json()["operations"] == []

    def describe(cells):
        return {**cells, cell_id: code[:-1] + ', description="charts")'}

    response = client.post("/editor/notebook/push/", json=_body(graph, describe, changed=[cell_id]))
    assert response.status_code == 200, response.text
    settings = graph.get_node(explore.node_id).setting_input
    assert settings.description == "charts"
    assert settings.graphic_walker_input.specList == charts


def _script_input(graph):
    return _node_of_type(graph, "python_script").setting_input.python_script_input


def test_an_unedited_drawer_script_pushes_nothing_and_keeps_its_cells(runner_kind, request, open_as, client_as):
    """The script renders as a function without a ``return``; its cells keep their ids and trailing newlines."""
    request.getfixturevalue("runner" if runner_kind == "interpreting" else "exec_runner")
    client, graph = client_as(OWNER_ID), open_as(build_drawer_script())
    stored = _script_input(graph).model_dump()
    assert all(cell["code"].endswith("\n") for cell in stored["cells"])
    body = _body(graph)
    code = dict(body["cells"])[_cell_of(graph, _node_of_type(graph, "python_script").node_id)]
    assert code.startswith('@ff.python_script(kernel="corpus_kernel", description="")\ndef _script_2():\n'), code
    assert code.endswith("\n\n\npython_script_2 = _script_2(source_1)"), code

    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200 and plan.json()["operations"] == [], plan.text
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]
    response = client.post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    assert response.json()["history"]["undo_count"] == undo_before
    assert _script_input(graph).model_dump() == stored


def test_an_edited_drawer_script_is_one_update_that_keeps_its_kernel(runner_kind, request, open_as, client_as):
    request.getfixturevalue("runner" if runner_kind == "interpreting" else "exec_runner")
    client, graph = client_as(OWNER_ID), open_as(build_drawer_script())
    script_id = _node_of_type(graph, "python_script").node_id
    cell_id = _cell_of(graph, script_id)
    body = _body(graph, lambda cells: {**cells, cell_id: cells[cell_id].replace("> 0", "> 5")}, changed=[cell_id])

    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200, plan.text
    assert [(op["op"], op["settings"]["node_id"]) for op in plan.json()["operations"]] == [
        ("update_settings", script_id)
    ]
    assert client.post("/editor/notebook/push/", json=body).status_code == 200
    stored = _script_input(graph)
    assert stored.kernel_id == "corpus_kernel"
    assert [cell.code for cell in stored.cells] == [
        "import polars as pl\n\ndf = flowfile_ctx.read_input()\n\n# Your transformation here",
        "flowfile_ctx.publish_output(df.filter(pl.col('amount') > 5))",
    ]
    assert stored.code == "\n\n".join(cell.code for cell in stored.cells)
    assert client.post("/notebook/plan", json=_body(graph)).json()["operations"] == []


def test_an_unedited_script_the_render_keeps_as_cells_pushes_nothing(runner, open_as, client_as):
    client, graph = client_as(OWNER_ID), open_as(build_python_script_cells())
    stored = _script_input(graph).model_dump()
    body = _body(graph)
    assert "ff.PythonScript(" in dict(body["cells"])[_cell_of(graph, _node_of_type(graph, "python_script").node_id)]
    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200 and plan.json()["operations"] == [], plan.text
    assert client.post("/editor/notebook/push/", json=body).status_code == 200
    assert _script_input(graph).model_dump() == stored


def test_push_refuses_a_stale_fingerprint_with_the_live_one(runner, orders_flow, client_as):
    body = {**_body(orders_flow, _raise_threshold), "code_fingerprint": "0" * 64}
    response = client_as(OWNER_ID).post("/editor/notebook/push/", json=body)
    assert response.status_code == 409
    assert response.json()["detail"]["code_fingerprint"] == code_fingerprint(orders_flow)


def test_a_run_sync_that_deletes_is_held_until_pushed_without_a_trigger(runner, orders_flow, client_as):
    client = client_as(OWNER_ID)
    graph = orders_flow
    formula = _node_of_type(graph, "formula").node_id
    formula_cell = _cell_of(graph, formula)
    fingerprint = code_fingerprint(graph)
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]
    body = _body(graph, lambda cells: {k: v for k, v in cells.items() if k != formula_cell})

    held = client.post("/editor/notebook/push/", json={**body, "trigger": "run"})
    assert held.status_code == 200, held.text
    assert held.json()["applied"] is False and formula in held.json()["deletions"]
    assert any(f"Deletes node {formula}" in w for w in held.json()["warnings"])
    assert held.json()["code_fingerprint"] == code_fingerprint(graph) == fingerprint
    assert held.json()["history"]["undo_count"] == undo_before
    assert graph.get_node(formula) is not None

    applied = client.post("/editor/notebook/push/", json=body)
    assert applied.status_code == 200, applied.text
    assert applied.json()["applied"] is True and formula in applied.json()["deletions"]
    assert graph.get_node(formula) is None
    assert applied.json()["code_fingerprint"] == code_fingerprint(graph) != fingerprint
    assert applied.json()["history"]["undo_count"] == undo_before + 1


def test_only_an_applied_push_publishes_notebook_pushed(runner, orders_flow, client_as, monkeypatch):
    published = []
    monkeypatch.setitem(events._handlers, "notebook_pushed", [lambda: published.append(1)])
    client = client_as(OWNER_ID)
    formula_cell = _cell_of(orders_flow, _node_of_type(orders_flow, "formula").node_id)
    body = _body(orders_flow, lambda cells: {k: v for k, v in cells.items() if k != formula_cell})

    assert client.post("/editor/notebook/push/", json={**body, "trigger": "run"}).json()["applied"] is False
    assert published == []
    assert client.post("/editor/notebook/push/", json=body).json()["applied"] is True
    assert published == [1]


def test_an_applied_push_answers_the_fingerprint_it_left_under_the_edit_lock(
    runner, orders_flow, client_as, monkeypatch
):
    graph = orders_flow
    body = _body(graph, _raise_threshold, changed=[_cell_of(graph, _node_of_type(graph, "filter").node_id)])
    left_under_lock = []
    edit_flow = editor_routes.edit_flow

    @contextmanager
    def another_tab_edits_after_the_lock(flow, description, *args, **kwargs):
        with edit_flow(flow, description, *args, **kwargs) as txn:
            yield txn
        if description == "Push notebook":
            left_under_lock.append(code_fingerprint(graph))
            graph.flow_settings.parameters = [FlowParameter(name="other_tab", default_value="1")]

    monkeypatch.setattr(editor_routes, "edit_flow", another_tab_edits_after_the_lock)
    response = client_as(OWNER_ID).post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    assert response.json()["code_fingerprint"] == left_under_lock[0] != code_fingerprint(graph)


def test_a_push_with_nothing_to_review_applies_in_one_call(runner, orders_flow, client_as):
    graph = orders_flow
    filt = _node_of_type(graph, "filter")
    body = {**_body(graph, _raise_threshold, changed=[_cell_of(graph, filt.node_id)]), "trigger": "push"}

    response = client_as(OWNER_ID).post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    pushed = response.json()
    assert (pushed["applied"], pushed["deletions"], pushed["parameter_changes"], pushed["warnings"]) == (
        True,
        [],
        False,
        [],
    )
    assert "20" in graph.get_node(filt.node_id).setting_input.filter_input.advanced_filter


def test_a_node_pushed_into_the_middle_of_a_chain_lands_clear_of_every_node(runner, orders_flow, client_as):
    client = client_as(OWNER_ID)
    graph = orders_flow
    assert client.post("/flow/apply_standard_layout/", params={"flow_id": graph.flow_id}).status_code == 200

    def positions():
        return {n.node_id: (n.setting_input.pos_x, n.setting_input.pos_y) for n in graph.nodes}

    before = positions()
    cell_id = _cell_of(graph, _node_of_type(graph, "filter").node_id)
    step = '.filter(ff.col("amount") > 10)'

    def insert_sort(cells):
        assert step in cells[cell_id]
        return {**cells, cell_id: cells[cell_id].replace(step, f'{step}\n    .sort("amount")')}

    response = client.post("/editor/notebook/push/", json=_body(graph, insert_sort, changed=[cell_id]))
    assert response.status_code == 200, response.text
    sort = _node_of_type(graph, "sort")
    after = positions()
    assert {node_id: after[node_id] for node_id in before} == before
    boxes = {node_id: node_box(x, y) for node_id, (x, y) in after.items()}
    assert not any(boxes[sort.node_id].overlaps(box) for node_id, box in boxes.items() if node_id != sort.node_id)


def test_a_run_reviews_only_deletions_and_a_push_anything_to_review():
    warned = ReconcilePlan(warnings=["Changes the flow parameters; undo does not restore them."])
    assert not needs_confirmation(warned, "run") and needs_confirmation(warned, "push")
    assert needs_confirmation(ReconcilePlan(deletions=[1]), "run")
    assert needs_confirmation(ReconcilePlan(parameter_changes=True), "push")
    assert not needs_confirmation(ReconcilePlan(parameter_changes=True), "run")
    assert not needs_confirmation(ReconcilePlan(), "push")


def _with_cell(code, cell_id="node-99"):
    def edit(cells):
        return {**cells, cell_id: code}

    return edit


def _push(client, graph, code, cell_id="node-99"):
    return client.post("/editor/notebook/push/", json=_body(graph, _with_cell(code, cell_id), [cell_id]))


def test_push_refuses_a_failing_cell_and_leaves_the_canvas(exec_runner, orders_flow, client_as):
    fingerprint = code_fingerprint(orders_flow)

    response = _push(client_as(OWNER_ID), orders_flow, "raise ValueError('boom')")
    assert response.status_code == 422, response.text
    assert response.json()["detail"] == {
        "message": "ValueError: boom",
        "cell_id": "node-99",
        "line": 1,
        "kind": "error",
    }
    assert code_fingerprint(orders_flow) == fingerprint


def test_push_refuses_a_cell_outside_the_dialect_and_leaves_the_canvas(runner, orders_flow, client_as):
    fingerprint = code_fingerprint(orders_flow)

    response = _push(client_as(OWNER_ID), orders_flow, "threshold = 8\nraise ValueError('boom')")
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert (detail["cell_id"], detail["line"], detail["kind"]) == ("node-99", 2, "needs_kernel")
    assert detail["message"] == "A `Raise` statement is not part of the notebook's flow code; this needs a kernel"
    assert code_fingerprint(orders_flow) == fingerprint


def test_push_refuses_an_in_memory_lazy_frame(exec_runner, orders_flow, client_as):
    code = "import polars as pl\nextra = ff.FlowFrame(pl.LazyFrame({'x': [1]}))"

    response = _push(client_as(OWNER_ID), orders_flow, code)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert (detail["cell_id"], detail["line"], detail["kind"]) == ("node-99", None, "refused")
    assert "LazyFrame" in detail["message"]


def test_an_in_memory_lazy_frame_needs_a_kernel_in_core(runner, orders_flow, client_as):
    code = "import polars as pl\nextra = ff.FlowFrame(pl.LazyFrame({'x': [1]}))"

    response = _push(client_as(OWNER_ID), orders_flow, code)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert (detail["cell_id"], detail["line"], detail["kind"]) == ("node-99", 2, "needs_kernel")
    assert "needs a kernel" in detail["message"]


@pytest.mark.parametrize("code", ["extra = ff.LazyFrame()", "extra = ff.DataFrame(schema={'x': ff.Int64})"])
def test_a_frame_without_data_is_refused_as_an_in_memory_lazy_frame(runner_kind, request, orders_flow, client_as, code):
    request.getfixturevalue("runner" if runner_kind == "interpreting" else "exec_runner")
    fingerprint = code_fingerprint(orders_flow)

    response = _push(client_as(OWNER_ID), orders_flow, code)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert (detail["cell_id"], detail["line"], detail["kind"]) == ("node-99", None, "refused")
    assert "LazyFrame" in detail["message"]
    assert code_fingerprint(orders_flow) == fingerprint


FAILING_CHAIN = """extra = ff.from_raw_data({'columns': [{'name': 'a', 'data_type': 'Integer'}], 'data': [[1]]})
out = (
    extra.filter(ff.col('a') > 1)
    .join(extra, on='nope')
)"""
# 3.10 reports a method call with keywords on the call's first line, 3.11+ on the method name's line
FAILING_CHAIN_LINE = 4 if sys.version_info >= (3, 11) else 3


@pytest.mark.parametrize(
    "code, line",
    [
        (FAILING_CHAIN, FAILING_CHAIN_LINE),
        ("extra = missing_frame.filter(ff.col('a') > 1)", 1),
        ("threshold = 8\nx = (", 2),
    ],
    ids=["frame_error_in_a_chain", "undefined_name", "syntax_error"],
)
def test_a_failing_call_reports_its_cell_line_and_message_like_exec(orders_flow, client_as, code, line):
    details = []
    for runner_class in (ExecRunner, NotebookRunner):
        before = bridge._runner
        bridge.set_clean_runner(runner_class())
        try:
            for route in ("/notebook/plan", "/editor/notebook/push/"):
                response = client_as(OWNER_ID).post(route, json=_body(orders_flow, _with_cell(code), ["node-99"]))
                assert response.status_code == 422, response.text
                details.append(response.json()["detail"])
        finally:
            bridge.set_clean_runner(before)
    assert details[0]["message"] and "Traceback" not in details[0]["message"]
    assert (details[0]["cell_id"], details[0]["line"], details[0]["kind"]) == ("node-99", line, "error")
    masked = [re.sub(r"<cell-node-99-\d+>", "<cell>", str(detail)) for detail in details]
    assert masked == [masked[0]] * 4


def test_a_request_over_a_bound_is_refused_with_the_cell_that_breaks_it(runner, orders_flow, client_as, monkeypatch):
    from flowfile_core.notebook import allowlist

    fingerprint = code_fingerprint(orders_flow)
    limit = max(len(cell.code.encode("utf-8")) for cell in render(orders_flow).cells)
    monkeypatch.setitem(allowlist.BOUNDS, "bytes_per_cell", limit)
    response = _push(client_as(OWNER_ID), orders_flow, "threshold = " + "1" * limit)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert (detail["cell_id"], detail["line"], detail["kind"]) == ("node-99", None, "refused")
    assert f"larger than {limit} bytes" in detail["message"]
    assert code_fingerprint(orders_flow) == fingerprint

    response = _push(client_as(OWNER_ID), orders_flow, "threshold = 8", cell_id="cell/../x")
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["kind"] == "refused" and response.json()["detail"]["cell_id"] is None


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
    refusals = [m for m, _ in refused_nodes(live, session, installed=lambda node_type: False)]
    assert len(refusals) == 3
    assert "Node 1" in refusals[0] and "LazyFrame" in refusals[0]
    assert "cell_node" in refusals[1] and "ff.custom_nodes.install" in refusals[1]
    assert "Node 3" in refusals[2] and "secret_name" in refusals[2]
    assert [m for m, _ in refused_nodes(live, session, installed=lambda node_type: True)][1:] == refusals[2:]


def test_refusals_accept_a_custom_node_file_written_after_the_scan(tmp_path, monkeypatch):
    from flowfile_core.flowfile.user_defined.registry import registry

    monkeypatch.setattr(registry, "_directory", tmp_path)
    monkeypatch.setattr(registry, "_entries", {})
    monkeypatch.setattr(registry, "_stamps", None)
    monkeypatch.setattr(registry, "on_registered", None)
    (tmp_path / "late_push_node.py").write_text(  # another process installed it after core's scan
        "from flowfile import node_designer as nd\n\n\n"
        "class LatePushNode(nd.CustomNodeBase):\n"
        '    node_name: str = "Late Push Node"\n\n'
        "    def process(self, *inputs):\n"
        "        return inputs[0]\n"
    )
    session = {"nodes": [{"id": 5, "type": "late_push_node", "setting_input": {"is_user_defined": True}}]}

    assert refused_nodes({"nodes": []}, session) == []


BROKEN_NODE_FILES = {
    "does_not_parse": ("broken_push_node", "class BrokenPushNode(:\n    pass\n"),
    "fails_when_placed": (
        "exec_broken_push_node",
        "from flowfile import node_designer as nd\n\n"
        "raise RuntimeError('this node file fails when it is executed')\n\n\n"
        "class ExecBrokenPushNode(nd.CustomNodeBase):\n"
        '    node_name: str = "Exec Broken Push Node"\n\n'
        "    def process(self, *inputs):\n"
        "        return inputs[0]\n",
    ),
}


@pytest.mark.parametrize("case", sorted(BROKEN_NODE_FILES))
def test_a_custom_node_file_that_does_not_load_counts_as_not_installed(tmp_path, monkeypatch, case):
    from flowfile_core.flowfile.user_defined.registry import registry

    monkeypatch.setattr(registry, "_directory", tmp_path)
    monkeypatch.setattr(registry, "_entries", {})
    monkeypatch.setattr(registry, "_stamps", None)
    monkeypatch.setattr(registry, "on_registered", None)
    monkeypatch.setattr(registry, "on_unregistered", None)
    node_type, source = BROKEN_NODE_FILES[case]
    (tmp_path / f"{node_type}.py").write_text(source)
    registry.refresh()
    entry = registry.get(node_type)
    assert entry is not None
    if case == "fails_when_placed":
        assert entry.error is None and registry.get_class(node_type) is None and entry.exec_error
    else:
        assert entry.error
    session = {"nodes": [{"id": 5, "type": node_type, "setting_input": {"is_user_defined": True}}]}

    refusals = [m for m, _ in refused_nodes({"nodes": []}, session)]
    assert len(refusals) == 1 and "Node 5" in refusals[0] and node_type in refusals[0]


def test_push_is_503_without_a_runner(orders_flow, client_as):
    before = bridge._runner
    bridge.set_clean_runner(None)
    try:
        response = client_as(OWNER_ID).post("/editor/notebook/push/", json=_body(orders_flow, _raise_threshold))
        assert response.status_code == 503 and response.json()["detail"] == bridge.NO_RUNNER_DETAIL
    finally:
        bridge.set_clean_runner(before)


@pytest.mark.parametrize("database_type, status", [("sqlite", 200), ("postgresql", 422)])
def test_an_inline_file_database_syncs_without_the_password_it_does_not_use(
    runner, open_as, client_as, tmp_path, database_type, status
):
    graph = ff.create_flow_graph()
    connection = input_schema.DatabaseConnection(
        database_type=database_type, database=str(tmp_path / "orders.db"), password_ref="gone"
    )
    settings = input_schema.DatabaseSettings(connection_mode="inline", database_connection=connection, table_name="t")
    graph.add_database_reader(
        input_schema.NodeDatabaseReader(
            flow_id=graph.flow_id,
            node_id=1,
            user_id=OWNER_ID,
            database_settings=settings,
            fields=[input_schema.MinimalFieldInfo(name="x", data_type="Int64")],
        )
    )
    graph = open_as(graph)

    response = client_as(OWNER_ID).post("/notebook/plan", json=_body(graph))
    assert response.status_code == status, response.text
    if status == 422:
        detail = response.json()["detail"]
        assert detail["kind"] == "refused" and detail["message"].endswith("Password not found")


def test_a_described_catalog_reader_renders_its_description_and_syncs_back_keeping_it(
    runner, notebook_corpus, open_as, client_as
):
    demo = dict(notebook_corpus)["demo"]
    sales = next(node.setting_input for node in demo.nodes if getattr(node.setting_input, "catalog_table_name", None))
    frame = ff.read_catalog_table(
        sales.catalog_table_name, namespace_id=sales.catalog_namespace_id, description="sales data"
    )
    graph = open_as(frame.flow_graph)
    reader = _node_of_type(graph, "catalog_reader").node_id
    cell_id = _cell_of(graph, reader)
    assert 'description="sales data"' in next(cell.code for cell in render(graph).cells if cell.cell_id == cell_id)
    client, body = client_as(OWNER_ID), _body(graph, changed=[cell_id])

    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200, plan.text
    assert plan.json()["operations"] == []
    response = client.post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    assert graph.get_node(reader).setting_input.description == "sales data"


def test_a_cosmetic_edit_of_a_designer_configured_catalog_writer_plans_nothing(runner, designer_writer_flow, client_as):
    """The cell carries the namespace name only; the designer's id beside it is no change to send back."""
    graph = designer_writer_flow
    cell_id = _cell_of(graph, _node_of_type(graph, "catalog_writer").node_id)
    body = _body(graph, lambda cells: {**cells, cell_id: "# reviewed\n" + cells[cell_id]}, changed=[cell_id])

    plan = client_as(OWNER_ID).post("/notebook/plan", json=body)
    assert plan.status_code == 200, plan.text
    assert plan.json()["operations"] == []


def test_user_2_cannot_push_plan_or_run_user_3s_flow(runner, open_as, client_as):
    graph = open_as(ff.from_dict({"a": [1, 2]}).filter(ff.col("a") > 1).flow_graph, user_id=3)
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
    mode = ff.Parameter("mode", default=default, type="enum", enum_values=["full", "quick"])
    source = ff.from_dict({"region": ["N", "S"], "amount": [1.0, 2.0]})
    ff.add_flow_parameter(source, mode)
    gate = ff.Gate(source, parameter=mode, value="full")
    target = tmp_path / f"written_{default}.csv"
    gate.then.write_csv(str(target))
    return gate.then.flow_graph, target


@pytest.mark.parametrize("default, written", [("quick", False), ("full", True)])
def test_run_lineage_honours_a_closed_gate(open_as, client_as, tmp_path, default, written):
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


def test_run_lineage_runs_only_the_ancestors(open_as, client_as):
    source = ff.from_dict({"a": [1, 2, 3]})
    kept = source.filter(ff.col("a") > 1)
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
    with get_db_context() as db:
        run = db.query(FlowRun).filter_by(flow_path=graph.flow_settings.path).one()
        assert (run.success, run.nodes_completed, run.number_of_nodes) == (True, 2, 2)


def test_run_lineage_commits_no_source_progress(open_as, client_as):
    """A node cell's Run shows rows: a source's commit callback waits for a run of the whole flow."""
    graph = open_as(ff.from_dict({"a": [1, 2, 3]}).filter(ff.col("a") > 1).flow_graph)
    source = _node_of_type(graph, "manual_input")
    committed = []
    source._on_flow_complete = committed.append
    response = client_as(OWNER_ID).post(
        "/editor/notebook/run_lineage/",
        json={"flow_id": graph.flow_id, "node_id": _node_of_type(graph, "filter").node_id},
    )
    assert response.status_code == 200, response.text
    assert graph.get_run_info().success
    assert committed == [] and source._on_flow_complete is not None


def test_run_lineage_of_a_writer_commits_source_progress(open_as, client_as, tmp_path):
    """A writer cell's Run writes, so the source's progress is committed with it: the flow's next run must not
    write the same rows again."""
    source = ff.from_dict({"a": [1, 2, 3]})
    source.write_csv(str(tmp_path / "out.csv"))
    graph = open_as(source.flow_graph)
    committed = []
    _node_of_type(graph, "manual_input")._on_flow_complete = committed.append
    response = client_as(OWNER_ID).post(
        "/editor/notebook/run_lineage/",
        json={"flow_id": graph.flow_id, "node_id": _node_of_type(graph, "output").node_id},
    )
    assert response.status_code == 200, response.text
    assert (tmp_path / "out.csv").exists() and committed == [True]


def test_set_flow_parameters_applies_outside_undo_and_rolls_back_with_the_batch(orders_flow, client_as):
    client = client_as(OWNER_ID)
    graph = orders_flow
    graph.flow_settings.parameters = [FlowParameter(name="limit", default_value="1")]
    fingerprint = code_fingerprint(graph)
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]
    set_params = {"op": "set_flow_parameters", "parameters": [{"name": "limit", "default_value": "5"}]}
    add_node = {"op": "add_node", "node_id": 99, "node_type": "sample"}

    failing = [set_params, add_node, {"op": "delete_connection", "connection": _missing_connection()}]
    response = client.post(
        "/editor/apply_operations/", json={"flow_id": graph.flow_id, "label": "x", "operations": failing}
    )
    assert response.status_code == 422, response.text
    assert [(p.name, p.default_value) for p in graph.flow_settings.parameters] == [("limit", "1")]
    assert graph.get_node(99) is None and code_fingerprint(graph) == fingerprint

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


def test_a_push_numbers_new_nodes_above_every_id_the_canvas_has_held(runner, orders_flow, client_as):
    """The canvas keeps the ceiling: a deleted id is not reused, and the client need not send its counter."""
    client, graph = client_as(OWNER_ID), orders_flow
    formula = _node_of_type(graph, "formula")
    highest = max(n.node_id for n in graph.nodes)
    assert formula.node_id == highest
    assert client.post("/editor/delete_node/", params={"flow_id": graph.flow_id, "node_id": highest}).status_code == 200
    filt = _node_of_type(graph, "filter")
    cell_id = _cell_of(graph, filt.node_id)

    def add_a_filter(cells):
        code = cells[cell_id]
        name = re.match(r"(\w+) = ", code).group(1)
        return {**cells, cell_id: f"{code}\n{name}_more = {name}.filter(ff.col('amount') > 15)\n"}

    body = {**_body(graph, add_a_filter, changed=[cell_id]), "client_max_node_id": 0}
    response = client.post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    new_ids = {n.node_id for n in graph.nodes} - {filt.node_id, _node_of_type(graph, "manual_input").node_id}
    assert new_ids and min(new_ids) > highest, new_ids
    assert response.json()["max_node_id"] == max(new_ids)


# visual groups: declared in the groups cell, joined with add_to_group, reconciled as one set_groups op


@pytest.fixture
def grouped_flow(open_as):
    outer = ff.FlowGroup("Clean", color="blue")
    inner = ff.FlowGroup("Inner", parent_group=outer)
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    kept = orders.filter(ff.col("amount") > 10).add_to_group(outer)
    return open_as(kept.with_columns((ff.col("amount") * 2).alias("double")).add_to_group(inner).flow_graph)


def _group_shape(graph):
    by_id = {g.id: g for g in graph._groups.values()}
    return {
        g.name: (
            g.color,
            by_id[g.parent_group_id].name if g.parent_group_id else None,
            sorted(graph._member_node_ids(g.id)),
        )
        for g in by_id.values()
    }


def test_an_unedited_grouped_flow_pushes_nothing(runner, grouped_flow, client_as):
    client = client_as(OWNER_ID)
    body = _body(grouped_flow)
    assert "groups" in dict(body["cells"])
    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200 and plan.json()["operations"] == [], plan.text


def test_push_creates_renames_and_regroups_from_the_cells_in_one_undo_step(runner, grouped_flow, client_as):
    client, graph = client_as(OWNER_ID), grouped_flow
    filt, formula = (_node_of_type(graph, t) for t in ("filter", "formula"))
    before = _group_shape(graph)
    assert before == {"Clean": ("blue", None, [filt.node_id]), "Inner": (None, "Clean", [formula.node_id])}
    clean_id = next(g.id for g in graph._groups.values() if g.name == "Clean")
    undo_before = client.get("/editor/history_status/", params={"flow_id": graph.flow_id}).json()["undo_count"]

    def edit(cells):
        node_cell = next(k for k in cells if k.startswith("cell-"))
        cells["groups"] = (
            cells["groups"].replace('"Clean", color="blue"', '"Cleaning", color="green"')
            + '\nextra = ff.FlowGroup("Extra", parent_group=clean)'
        )
        cells[node_cell] = cells[node_cell].replace(".add_to_group(inner)", ".add_to_group(extra)")
        return cells

    body = _body(graph, edit, changed=["groups", _cell_of(graph, filt.node_id)])
    plan = client.post("/notebook/plan", json=body)
    assert plan.status_code == 200, plan.text
    assert [op["op"] for op in plan.json()["operations"]] == ["update_group", "update_group"]
    response = client.post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    assert _group_shape(graph) == {
        "Cleaning": ("green", None, [filt.node_id]),
        "Extra": (None, "Cleaning", [formula.node_id]),
    }
    assert next(g.id for g in graph._groups.values() if g.name == "Cleaning") == clean_id
    assert response.json()["history"]["undo_count"] == undo_before + 1

    assert client.post("/editor/undo/", params={"flow_id": graph.flow_id}).status_code == 200
    assert _group_shape(graph) == before


def test_a_new_node_in_an_edited_cell_joins_the_group_its_cell_names(runner, grouped_flow, client_as):
    client, graph = client_as(OWNER_ID), grouped_flow
    formula = _node_of_type(graph, "formula")
    cell_id = _cell_of(graph, formula.node_id)

    def edit(cells):
        var = next(line.split(" = ")[0] for line in cells[cell_id].splitlines() if " = " in line)
        cells[cell_id] += f'\nsorted_out = {var}.sort("amount").add_to_group(clean)'
        return cells

    body = _body(graph, edit, changed=[cell_id])
    response = client.post("/editor/notebook/push/", json=body)
    assert response.status_code == 200, response.text
    new_node = _node_of_type(graph, "sort")
    assert new_node.setting_input.node_reference == "sorted_out"
    assert _group_shape(graph)["Clean"] == (
        "blue",
        None,
        sorted([_node_of_type(graph, "filter").node_id, new_node.node_id]),
    )


def test_dropping_every_add_to_group_removes_the_groups(runner, grouped_flow, client_as):
    client, graph = client_as(OWNER_ID), grouped_flow
    node_cell = _cell_of(graph, _node_of_type(graph, "filter").node_id)

    def edit(cells):
        cells[node_cell] = cells[node_cell].replace(".add_to_group(clean)", "").replace(".add_to_group(inner)", "")
        return cells

    response = client.post("/editor/notebook/push/", json=_body(graph, edit, changed=[node_cell]))
    assert response.status_code == 200, response.text
    assert graph._groups == {} and all(n.setting_input.group_id is None for n in graph.nodes)
