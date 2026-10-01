"""The canvas fallback of a notebook kernel session, through the ``kernel-sim`` manager (no Docker): rows of a
node the kernel cannot compute come from the canvas as parquet on the kernel's shared folder."""

from __future__ import annotations

import json

import pytest

from shared.notebook_display import TABLE_MIME
from tests.notebook.conftest import NOTEBOOK_OWNER_ID

LOOPBACK = ("127.0.0.1", 50123)


@pytest.fixture
def client(client_as, kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


@pytest.fixture
def coded_flow(open_as):
    """A flow whose last node is a Polars Code node: always deferred in a session, so its rows need the canvas."""
    import flowfile as fl

    orders = fl.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return open_as(orders.polars_code("input_df.with_columns(pl.col('amount') * 10)").flow_graph)


def _coded_id(flow) -> int:
    return next(node.node_id for node in flow.nodes if node.node_type == "polars_code")


def _bind(node_id: int) -> str:
    frames = "(v for v in list(globals().values()) if type(v).__name__ == 'FlowFrame')"
    return f"coded = next(v for v in {frames} if v.node_id == {node_id})\n"


def _execute(client, flow, kernel_sim, code: str) -> dict:
    body = {"flow_id": flow.flow_id, "kernel_id": kernel_sim.kernel.id, "cell_id": "cell-1", "code": code}
    response = client.post("/notebook/session/execute", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def _table(result: dict) -> dict:
    tables = [out for out in result["display_outputs"] if out["mime_type"] == TABLE_MIME]
    assert tables, result
    return json.loads(tables[0]["data"])


def _results_folder(kernel_sim, flow):
    return kernel_sim.shared_volume_path + f"/notebook/{flow.flow_id}"


def test_display_and_collect_on_a_deferred_canvas_node_show_the_canvas_rows(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    result = _execute(
        client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)\nprint(coded.collect()['amount'].to_list())"
    )
    assert result["success"], result
    assert "[100, 200, 300]" in result["stdout"]
    assert "100" in json.dumps(_table(result))
    assert len(kernel_sim.node_results) == 1, "the second read in the session reuses the path"


def test_a_reset_session_reuses_the_canvas_file(coded_flow, client, kernel_sim):
    from pathlib import Path

    node_id = _coded_id(coded_flow)
    assert _execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)")["success"]
    files = {path: path.stat().st_mtime_ns for path in Path(_results_folder(kernel_sim, coded_flow)).iterdir()}
    assert len(files) == 1

    body = {"flow_id": coded_flow.flow_id, "kernel_id": kernel_sim.kernel.id}
    assert client.post("/notebook/session/reset", json=body).status_code == 200
    assert _execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)")["success"]
    assert len(kernel_sim.node_results) == 2
    after = {path: path.stat().st_mtime_ns for path in Path(_results_folder(kernel_sim, coded_flow)).iterdir()}
    assert after == files


def test_a_new_frame_on_a_deferred_canvas_node_computes_here_on_the_canvas_rows(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + "big = coded.filter(fl.col('amount') > 100)\ndisplay(big)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert len(_rows(shown)) == 2 and len(kernel_sim.node_results) == 1, shown["display_outputs"]
    later = _execute(client, coded_flow, kernel_sim, "print(big.with_columns(x=fl.lit(1)).collect().height)")
    assert later["success"] and later["stdout"].strip() == "2", later
    assert len(kernel_sim.node_results) == 1


def test_new_deferred_work_says_push_first(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + "new = coded.polars_code('input_df.head(1)')\ndisplay(new)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    text = [out["data"] for out in shown["display_outputs"] if out["mime_type"] == "text/plain"]
    assert any("Push, then it runs on the canvas" in t for t in text), shown["display_outputs"]

    collected = _execute(client, coded_flow, kernel_sim, "new.collect()")
    assert not collected["success"] and "Push, then it runs on the canvas" in collected["error"]
    assert not kernel_sim.node_results


def test_a_running_flow_is_refused_with_a_message(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    coded_flow.flow_settings.is_running = True
    try:
        result = _execute(client, coded_flow, kernel_sim, _bind(node_id) + "coded.collect()")
    finally:
        coded_flow.flow_settings.is_running = False
    assert not result["success"]
    assert "running on the canvas" in result["error"], result


@pytest.fixture
def hidden_csv(open_as, tmp_path, monkeypatch):
    """Opens a flow built on a CSV the kernel cannot see: its folder maps to a kernel folder that does not exist."""
    import flowfile as fl

    folder = tmp_path / "host_data"
    folder.mkdir()
    path = folder / "orders.csv"
    path.write_text("id,amount\n1,10\n2,20\n3,30\n")
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_MOUNTS", json.dumps({str(folder): str(tmp_path / "not_mounted")}))
    return lambda build: open_as(build(fl.read_csv(str(path))).flow_graph)


@pytest.fixture
def hidden_csv_flow(hidden_csv):
    return hidden_csv(lambda source: source)


def test_an_unedited_read_of_a_file_the_kernel_cannot_see_shows_the_canvas_rows(hidden_csv_flow, client, kernel_sim):
    from flowfile_core.notebook.render import render

    source = next(cell for cell in render(hidden_csv_flow).cells if cell.kind == "node")
    name = source.code.split("=", 1)[0].strip()
    result = _execute(client, hidden_csv_flow, kernel_sim, source.code + f"\ndisplay({name})")
    assert result["success"], result
    table = _table(result)
    assert table["columns"] == ["id", "amount"] and "30" in json.dumps(table["data"]), table
    read_id = next(node.node_id for node in hidden_csv_flow.nodes if node.node_type == "read")
    assert [body["node_id"] for body in kernel_sim.node_results] == [read_id]


def test_a_new_read_of_a_file_the_kernel_cannot_see_has_no_columns_and_names_the_folders(
    hidden_csv_flow, client, kernel_sim, tmp_path
):
    other = tmp_path / "host_data" / "other.csv"
    other.write_text("a\n1\n")
    result = _execute(client, hidden_csv_flow, kernel_sim, f"new = fl.read_csv({str(other)!r})\ndisplay(new)")
    assert result["success"], result
    text = next(out["data"] for out in result["display_outputs"] if out["mime_type"] == "text/plain")
    assert "Folders this kernel can read" in text and "push, then it runs on the canvas" in text, text
    assert not text.startswith("Schema:"), text
    assert not kernel_sim.node_results


def test_an_unedited_push_through_the_kernel_of_a_file_it_cannot_see_changes_nothing(hidden_csv, kernel_sim):
    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.notebook.push import NotebookPushRequest, plan_push
    from flowfile_core.notebook.render import render
    from tests.notebook.conftest import cell_provenance

    import flowfile as fl

    hidden_csv_flow = hidden_csv(lambda source: source.filter(fl.col("amount") > 10))
    owner = PydanticUser(username="nb_kernel", id=NOTEBOOK_OWNER_ID, disabled=False, is_admin=True)
    rendering = render(hidden_csv_flow)
    request = NotebookPushRequest(
        flow_id=hidden_csv_flow.flow_id,
        cells=[(cell.cell_id, cell.code) for cell in rendering.cells],
        provenance=cell_provenance(hidden_csv_flow, rendering),
        code_fingerprint=rendering.code_fingerprint,
        client_max_node_id=max(node.node_id for node in hidden_csv_flow.nodes),
        kernel_id=kernel_sim.kernel.id,
    )
    plan, _ = plan_push(hidden_csv_flow, owner, request)
    assert not plan.operations, [op.model_dump(mode="json") for op in plan.operations]
    assert any('"op": "clean_run"' in r.code for r in kernel_sim.requests)


def _rows(result: dict) -> list:
    return _table(result)["data"]


def test_rerunning_the_read_of_a_file_the_kernel_cannot_see_keeps_the_canvas_rows(hidden_csv, client, kernel_sim):
    import flowfile as fl

    flow = hidden_csv(lambda source: source.filter(fl.col("amount") > 10))
    path = next(node.setting_input.received_file.path for node in flow.nodes if node.node_type == "read")
    source = f"src = fl.read_csv({path!r})"
    assert _execute(client, flow, kernel_sim, source)["success"]
    assert _execute(client, flow, kernel_sim, source)["success"]
    filtered = _execute(client, flow, kernel_sim, "big = src.filter(fl.col('amount') > 10)")
    assert filtered["success"], filtered
    assert len(_rows(_execute(client, flow, kernel_sim, "display(src)"))) == 3
    assert len(_rows(_execute(client, flow, kernel_sim, "display(big)"))) == 2


def test_run_all_of_a_flow_on_a_file_the_kernel_cannot_see_shows_the_canvas_rows(hidden_csv, client, kernel_sim):
    from flowfile_core.notebook.render import render

    import flowfile as fl

    flow = hidden_csv(lambda source: source.filter(fl.col("amount") > 10))
    cells = [cell for cell in render(flow).cells if cell.kind in ("imports", "node")]
    for cell in cells:
        result = _execute(client, flow, kernel_sim, cell.code)
        assert result["success"], (cell.code, result)
    name = cells[-1].code.split("=", 1)[0].strip()
    assert len(_rows(_execute(client, flow, kernel_sim, f"display({name})"))) == 2


@pytest.fixture
def editor_built_flow(open_as, tmp_path, monkeypatch):
    """CSV read -> basic filter ``quantity >= 8``, built from settings as the editor's API saves them."""
    from flowfile_core.flowfile import flow_graph as graph_module
    from flowfile_core.schemas import input_schema, transform_schema
    from flowfile_frame.utils import create_flow_graph

    folder = tmp_path / "host_data"
    folder.mkdir()
    path = folder / "orders.csv"
    path.write_text("id,quantity\n" + "".join(f"{i},{i % 12}\n" for i in range(1, 21)))
    graph = create_flow_graph()
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="read"))
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="filter"))
    graph.add_read(
        input_schema.NodeRead(
            flow_id=graph.flow_id,
            node_id=1,
            received_file=input_schema.ReceivedTable(
                name=path.name,
                path=str(path),
                directory=str(folder),
                file_type="csv",
                table_settings=input_schema.InputCsvTable(delimiter=",", has_headers=True),
            ),
        )
    )
    graph_module.add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    basic = transform_schema.BasicFilter(field="quantity", operator=">=", value="8")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="basic", basic_filter=basic),
        )
    )
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_MOUNTS", json.dumps({str(folder): str(tmp_path / "not_mounted")}))
    return open_as(graph)


def test_run_all_of_an_editor_built_read_and_filter_the_kernel_cannot_see(editor_built_flow, client, kernel_sim):
    from flowfile_core.notebook.render import render

    flow = editor_built_flow
    cells = [cell for cell in render(flow).cells if cell.kind in ("imports", "node")]
    body = {"flow_id": flow.flow_id, "kernel_id": kernel_sim.kernel.id}
    assert client.post("/notebook/session/reset", json=body).status_code == 200
    for cell in cells:
        result = _execute(client, flow, kernel_sim, cell.code)
        assert result["success"], (cell.code, result)
    name = cells[-1].code.split("=", 1)[0].strip()
    cell = f"priced = {name}.with_columns(fl.col('quantity') * 2)\ndisplay(priced)"
    priced = _execute(client, flow, kernel_sim, cell)
    assert priced["success"], priced
    assert len(_rows(priced)) == 5, _table(priced)


def test_two_reads_of_files_the_kernel_cannot_see_each_take_their_own_canvas_rows(
    open_as, tmp_path, monkeypatch, client, kernel_sim
):
    import flowfile as fl

    folder = tmp_path / "host_data"
    folder.mkdir()
    first, second = folder / "a.csv", folder / "b.csv"
    first.write_text("x\n1\n")
    second.write_text("y\n2\n3\n")
    source = fl.read_csv(str(first))
    fl.read_csv(str(second), flow_graph=source.flow_graph)
    flow = open_as(source.flow_graph)
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_MOUNTS", json.dumps({str(folder): str(tmp_path / "not_mounted")}))
    cell = f"a = fl.read_csv({str(first)!r})\nb = fl.read_csv({str(second)!r})"
    assert _execute(client, flow, kernel_sim, cell)["success"]
    assert _table(_execute(client, flow, kernel_sim, "display(a)"))["columns"] == ["x"]
    assert len(_rows(_execute(client, flow, kernel_sim, "display(b)"))) == 2


LOOP_CELL = """import re

priced = filtered_2
for name in priced.columns:
    if re.search(r"quantity$", name):
        priced = priced.with_columns((fl.col(name) * 2).alias(f"{name}_doubled"))
print(priced.columns)
display(priced)
"""


def _plan(flow, kernel_sim, *extra: str, rendered: bool = True):
    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.notebook.push import NotebookPushRequest, plan_push
    from flowfile_core.notebook.render import render
    from tests.notebook.conftest import cell_provenance

    owner = PydanticUser(username="nb_kernel", id=NOTEBOOK_OWNER_ID, disabled=False, is_admin=True)
    rendering = render(flow)
    cells = [(cell.cell_id, cell.code) for cell in rendering.cells if rendered or cell.kind == "imports"]
    request = NotebookPushRequest(
        flow_id=flow.flow_id,
        cells=cells + [(f"extra-{i}", code) for i, code in enumerate(extra)],
        provenance=cell_provenance(flow, rendering),
        code_fingerprint=rendering.code_fingerprint,
        client_max_node_id=max(node.node_id for node in flow.nodes),
        kernel_id=kernel_sim.kernel.id,
    )
    plan, _ = plan_push(flow, owner, request)
    return plan


def test_run_and_push_see_the_same_seeded_names(editor_built_flow, client, kernel_sim):
    shown = _execute(client, editor_built_flow, kernel_sim, "display(source_1)")
    assert shown["success"] and len(_rows(shown)) == 20, shown

    unedited = _plan(editor_built_flow, kernel_sim)
    assert not unedited.operations and not unedited.warnings, unedited
    referencing = _plan(editor_built_flow, kernel_sim, "display(source_1)")
    assert not referencing.operations and not referencing.warnings, referencing


def test_pushing_a_loop_cell_on_a_file_the_kernel_cannot_see_adds_its_node(editor_built_flow, client, kernel_sim):
    ran = _execute(client, editor_built_flow, kernel_sim, LOOP_CELL)
    assert ran["success"] and "quantity_doubled" in ran["stdout"] and len(_rows(ran)) == 5, ran

    plan = _plan(editor_built_flow, kernel_sim, LOOP_CELL)
    assert not plan.warnings, plan.warnings
    added = [op for op in plan.operations if op.op == "add_node"]
    assert [op.node_type for op in added] == ["formula"], [op.model_dump(mode="json") for op in plan.operations]


def test_a_seeded_name_no_cell_builds_adopts_its_canvas_node(editor_built_flow, kernel_sim):
    plan = _plan(editor_built_flow, kernel_sim, "big = source_1.filter(fl.col('quantity') > 10)", rendered=False)
    added = [op.model_dump(mode="json") for op in plan.operations if op.op == "add_node"]
    assert [op["node_type"] for op in added] == ["filter"], added
    assert added[0]["node_id"] > 2 and plan.deletions == [2], plan
    connects = [op.connection.output_connection.node_id for op in plan.operations if op.op == "connect"]
    assert connects == [1], plan
