"""The canvas fallback of a notebook kernel session, through the ``kernel-sim`` manager (no Docker): rows of a
node the kernel cannot compute come from the canvas as parquet on the kernel's shared folder."""

from __future__ import annotations

import json
import time
from pathlib import Path

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
    import flowfile as ff

    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
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


def test_showing_canvas_rows_commits_no_source_progress(coded_flow, client, kernel_sim):
    """The canvas run behind a ``display`` leaves a source's commit callback (a change-feed cursor, a Kafka
    offset) for the next run of the flow."""
    node_id = _coded_id(coded_flow)
    source = next(node for node in coded_flow.nodes if node.node_id != node_id)
    committed = []
    source._on_flow_complete = committed.append

    assert _execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)")["success"]

    assert len(kernel_sim.node_results) == 1
    assert committed == [] and source._on_flow_complete is not None


def test_rows_whose_file_is_gone_are_asked_again(coded_flow, client, kernel_sim):
    """Core removes a file a newer result of the node superseded; the session then asks for the node's rows again."""
    node_id = _coded_id(coded_flow)
    assert _execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)")["success"]
    for path in Path(_results_folder(kernel_sim, coded_flow)).iterdir():
        path.unlink()

    shown = _execute(client, coded_flow, kernel_sim, "display(coded)")

    assert shown["success"] and len(_rows(shown)) == 3, shown
    assert len(kernel_sim.node_results) == 2


def test_a_reset_session_reuses_the_canvas_file(coded_flow, client, kernel_sim):
    from pathlib import Path

    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.notebook import kernel_runner

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

    settings = coded_flow.get_node(node_id).setting_input.model_copy(deep=True)
    settings.polars_code_input.polars_code = "input_df.with_columns(pl.col('amount') * 100)"
    coded_flow.add_polars_code(settings)
    assert client.post("/notebook/session/reset", json=body).status_code == 200
    shown = _execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)")
    assert shown["success"] and "3000" in json.dumps(_rows(shown)), shown
    changed = list(Path(_results_folder(kernel_sim, coded_flow)).iterdir())
    assert len(changed) == 1 and changed[0] not in files, "the superseded file is removed"
    cached = [value for entry in kernel_runner._results.values() for pair in entry.values() for value in pair]
    assert cached and not any(isinstance(value, FlowDataEngine) for value in cached), cached


def test_a_new_canvas_result_under_unchanged_settings_is_handed_over(coded_flow, client, kernel_sim):
    """A re-run can change a node's rows without changing its hash (a subflow, a database read in Performance
    mode), so the file follows the canvas's result itself."""
    import polars as pl

    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

    node_id = _coded_id(coded_flow)
    assert len(_rows(_execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)"))) == 3
    node = coded_flow.get_node(node_id)
    digest = node.hash
    node.results.resulting_data = FlowDataEngine(pl.LazyFrame({"id": [1, 2, 3, 4], "amount": [1, 2, 3, 4]}))
    assert node.hash == digest

    body = {"flow_id": coded_flow.flow_id, "kernel_id": kernel_sim.kernel.id}
    assert client.post("/notebook/session/reset", json=body).status_code == 200
    assert len(_rows(_execute(client, coded_flow, kernel_sim, _bind(node_id) + "display(coded)"))) == 4
    assert len(list(Path(_results_folder(kernel_sim, coded_flow)).iterdir())) == 1


def test_a_new_frame_on_a_deferred_canvas_node_computes_here_on_the_canvas_rows(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + "big = coded.filter(ff.col('amount') > 100)\ndisplay(big)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert len(_rows(shown)) == 2 and len(kernel_sim.node_results) == 1, shown["display_outputs"]
    later = _execute(client, coded_flow, kernel_sim, "print(big.with_columns(x=ff.lit(1)).collect().height)")
    assert later["success"] and later["stdout"].strip() == "2", later
    assert len(kernel_sim.node_results) == 1


def test_new_work_only_the_canvas_runs_says_push_first(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + "new = ff.PythonScript(coded, code='x = 1', kernel='other-kernel').output\ndisplay(new)"
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    text = [out["data"] for out in shown["display_outputs"] if out["mime_type"] == "text/plain"]
    assert any("Push, then it runs on the canvas" in t for t in text), shown["display_outputs"]

    collected = _execute(client, coded_flow, kernel_sim, "new.collect()")
    assert not collected["success"] and "Push, then it runs on the canvas" in collected["error"]
    assert not kernel_sim.node_results


@pytest.fixture
def people_csv(tmp_path) -> Path:
    path = tmp_path / "people.csv"
    path.write_text("name,city,segment\na,x,s1\nb,x,s2\nc,y,s1\nd,y,s1\n")
    return path


def _pivot_cell(path: Path) -> str:
    return (
        f"df = ff.scan_csv({str(path)!r}, separator=',', has_header=True)\n"
        "pivoted = df.pivot(values='name', index=['city'], on='segment', aggregate_function='count')\n"
        "display(pivoted)"
    )


def test_a_new_pivot_over_a_file_the_kernel_reads_computes_here_without_a_canvas_ancestor(
    coded_flow, client, kernel_sim, people_csv
):
    for _ in range(2):
        shown = _execute(client, coded_flow, kernel_sim, _pivot_cell(people_csv))
        assert shown["success"], shown
        table = _table(shown)
        assert table["columns"] == ["city", "s1", "s2"] and len(table["data"]) == 2, table
    collected = _execute(client, coded_flow, kernel_sim, "print(pivoted.sort('city').collect().rows())")
    assert collected["success"] and collected["stdout"].strip() == "[('x', 1, 1), ('y', 2, 0)]", collected
    coded = _execute(client, coded_flow, kernel_sim, "display(df.polars_code('input_df.head(1)'))")
    assert coded["success"] and len(_rows(coded)) == 1, coded
    assert not kernel_sim.node_results


def test_a_new_pivot_below_a_deferred_canvas_node_computes_here_on_its_rows(coded_flow, client, kernel_sim):
    node_id = _coded_id(coded_flow)
    cell = _bind(node_id) + (
        "spread = coded.with_columns(g=ff.lit('all')).pivot(values='amount', index=['g'], on='id', "
        "aggregate_function='sum')\ndisplay(spread)"
    )
    shown = _execute(client, coded_flow, kernel_sim, cell)
    assert shown["success"], shown
    assert _rows(shown) == [{"g": "all", "1": 100, "2": 200, "3": 300}], _table(shown)
    assert [body["node_id"] for body in kernel_sim.node_results] == [node_id]


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
    import flowfile as ff

    folder = tmp_path / "host_data"
    folder.mkdir()
    path = folder / "orders.csv"
    path.write_text("id,amount\n1,10\n2,20\n3,30\n")
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_MOUNTS", json.dumps({str(folder): str(tmp_path / "not_mounted")}))
    return lambda build: open_as(build(ff.read_csv(str(path))).flow_graph)


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
    result = _execute(client, hidden_csv_flow, kernel_sim, f"new = ff.read_csv({str(other)!r})\ndisplay(new)")
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

    import flowfile as ff

    hidden_csv_flow = hidden_csv(lambda source: source.filter(ff.col("amount") > 10))
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



def test_a_push_through_the_kernel_stores_the_host_path_of_a_kernel_path(open_as, kernel_sim, tmp_path, monkeypatch):
    import flowfile as ff

    host, inside = (tmp_path / "host_data").resolve(), (tmp_path / "kernel_view").resolve()
    for folder in (host, inside):
        folder.mkdir()
        (folder / "orders.csv").write_text("id,amount\n1,10\n2,20\n")
    flow = open_as(ff.read_csv(str(host / "orders.csv")).flow_graph)
    monkeypatch.setattr(kernel_sim, "host_folders", lambda kernel_id: {str(inside): str(host)})
    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.notebook.push import NotebookPushRequest, plan_push
    from flowfile_core.notebook.render import render
    from tests.notebook.conftest import cell_provenance

    rendering = render(flow)
    cells = [(cell.cell_id, cell.code.replace("host_data", "kernel_view")) for cell in rendering.cells]
    edited = [cell_id for (cell_id, code), cell in zip(cells, rendering.cells) if code != cell.code]
    assert len(edited) == 1
    request = NotebookPushRequest(
        flow_id=flow.flow_id,
        cells=cells,
        changed_cell_ids=edited,
        provenance=cell_provenance(flow, rendering),
        code_fingerprint=rendering.code_fingerprint,
        client_max_node_id=max(node.node_id for node in flow.nodes),
        kernel_id=kernel_sim.kernel.id,
    )
    owner = PydanticUser(username="nb_kernel", id=NOTEBOOK_OWNER_ID, disabled=False, is_admin=True)
    plan, _ = plan_push(flow, owner, request)
    assert not plan.operations, [op.model_dump(mode="json") for op in plan.operations]
    assert any('"op": "clean_run"' in r.code for r in kernel_sim.requests)


def _rows(result: dict) -> list:
    return _table(result)["data"]


def test_rerunning_the_read_of_a_file_the_kernel_cannot_see_keeps_the_canvas_rows(hidden_csv, client, kernel_sim):
    import flowfile as ff

    flow = hidden_csv(lambda source: source.filter(ff.col("amount") > 10))
    path = next(node.setting_input.received_file.path for node in flow.nodes if node.node_type == "read")
    source = f"src = ff.read_csv({path!r})"
    assert _execute(client, flow, kernel_sim, source)["success"]
    assert _execute(client, flow, kernel_sim, source)["success"]
    filtered = _execute(client, flow, kernel_sim, "big = src.filter(ff.col('amount') > 10)")
    assert filtered["success"], filtered
    assert len(_rows(_execute(client, flow, kernel_sim, "display(src)"))) == 3
    assert len(_rows(_execute(client, flow, kernel_sim, "display(big)"))) == 2


def test_run_all_of_a_flow_on_a_file_the_kernel_cannot_see_shows_the_canvas_rows(hidden_csv, client, kernel_sim):
    from flowfile_core.notebook.render import render

    import flowfile as ff

    flow = hidden_csv(lambda source: source.filter(ff.col("amount") > 10))
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
    cell = f"priced = {name}.with_columns(ff.col('quantity') * 2)\ndisplay(priced)"
    priced = _execute(client, flow, kernel_sim, cell)
    assert priced["success"], priced
    assert len(_rows(priced)) == 5, _table(priced)


def test_two_reads_of_files_the_kernel_cannot_see_each_take_their_own_canvas_rows(
    open_as, tmp_path, monkeypatch, client, kernel_sim
):
    import flowfile as ff

    folder = tmp_path / "host_data"
    folder.mkdir()
    first, second = folder / "a.csv", folder / "b.csv"
    first.write_text("x\n1\n")
    second.write_text("y\n2\n3\n")
    source = ff.read_csv(str(first))
    ff.read_csv(str(second), flow_graph=source.flow_graph)
    flow = open_as(source.flow_graph)
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_MOUNTS", json.dumps({str(folder): str(tmp_path / "not_mounted")}))
    cell = f"a = ff.read_csv({str(first)!r})\nb = ff.read_csv({str(second)!r})"
    assert _execute(client, flow, kernel_sim, cell)["success"]
    assert _table(_execute(client, flow, kernel_sim, "display(a)"))["columns"] == ["x"]
    assert len(_rows(_execute(client, flow, kernel_sim, "display(b)"))) == 2


LOOP_CELL = """import re

priced = filtered_2
for name in priced.columns:
    if re.search(r"quantity$", name):
        priced = priced.with_columns((ff.col(name) * 2).alias(f"{name}_doubled"))
print(priced.columns)
display(priced)
"""


def _plan(flow, kernel_sim, *extra: str, rendered: bool = True, edit=lambda code: code):
    """Plan a push of the rendered cells (each through ``edit``) plus ``extra``; without ``kernel_sim``, in core."""
    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.notebook.push import NotebookPushRequest, plan_push
    from flowfile_core.notebook.render import render
    from tests.notebook.conftest import cell_provenance

    owner = PydanticUser(username="nb_kernel", id=NOTEBOOK_OWNER_ID, disabled=False, is_admin=True)
    rendering = render(flow)
    cells = [(cell.cell_id, edit(cell.code)) for cell in rendering.cells if rendered or cell.kind == "imports"]
    request = NotebookPushRequest(
        flow_id=flow.flow_id,
        cells=cells + [(f"extra-{i}", code) for i, code in enumerate(extra)],
        provenance=cell_provenance(flow, rendering),
        code_fingerprint=rendering.code_fingerprint,
        client_max_node_id=max(node.node_id for node in flow.nodes),
        kernel_id=kernel_sim.kernel.id if kernel_sim is not None else None,
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


def test_a_seeded_name_no_cell_builds_fails_on_its_cell_with_and_without_a_kernel(editor_built_flow, kernel_sim):
    from fastapi import HTTPException

    details = []
    for kernel in (kernel_sim, None):
        with pytest.raises(HTTPException) as failed:
            _plan(editor_built_flow, kernel, "big = source_1.filter(ff.col('quantity') > 10)", rendered=False)
        assert failed.value.status_code == 422
        details.append(failed.value.detail)
    assert details[0] == details[1], details
    assert (details[0]["cell_id"], details[0]["line"], details[0]["kind"]) == ("extra-0", 1, "error"), details[0]
    assert "name 'source_1' is not defined yet" in details[0]["message"], details[0]


@pytest.fixture
def pivoted_flow(open_as, people_csv):
    """``CSV read -> pivot`` on the canvas, never run there."""
    import flowfile as ff

    source = ff.scan_csv(str(people_csv), separator=",", has_header=True)
    return open_as(source.pivot(values="name", index=["city"], on="segment", aggregate_function="count").flow_graph)


def _rendered_name(flow, call: str) -> str:
    from flowfile_core.notebook.render import render

    return next(cell for cell in render(flow).cells if call in cell.code).code.split("=", 1)[0].strip()


def test_a_push_reads_the_rows_of_a_pivot_the_canvas_has_from_the_canvas(pivoted_flow, kernel_sim):
    name = _rendered_name(pivoted_flow, ".pivot(")
    shown = _plan(pivoted_flow, kernel_sim, f"display({name})")
    assert not shown.operations and not kernel_sim.node_results, "a push's display reads no rows"

    cell = f"rows = sorted({name}.collect().rows())\nassert rows == [('x', 1, 1), ('y', 2, 0)], rows"
    plan = _plan(pivoted_flow, kernel_sim, cell)
    assert not plan.operations and not plan.warnings, plan
    assert [body["node_id"] for body in kernel_sim.node_results] == [_node_id(pivoted_flow, "pivot")]


@pytest.mark.parametrize(
    "cell, edit, line, node_type",
    [
        ("x_only = {name}.filter(ff.col('city') == 'x')\nx_only.collect()", None, 2, "filter"),
        ("{name}.collect()", lambda code: code.replace("skip_rows=0", "skip_rows=1"), 1, "pivot"),
    ],
    ids=["new-node", "edited-above"],
)
def test_a_push_reading_rows_the_canvas_does_not_have_fails_on_that_cell(
    pivoted_flow, kernel_sim, cell, edit, line, node_type
):
    from fastapi import HTTPException

    name = _rendered_name(pivoted_flow, ".pivot(")
    with pytest.raises(HTTPException) as failed:
        _plan(pivoted_flow, kernel_sim, cell.format(name=name), edit=edit or (lambda code: code))
    detail = failed.value.detail
    assert failed.value.status_code == 422 and (detail["cell_id"], detail["line"]) == ("extra-0", line), detail
    assert f"The canvas has no rows yet for this {node_type} node" in detail["message"], detail
    assert not kernel_sim.node_results


@pytest.mark.parametrize("read", ["{name}.collect()", "display({name})"])
def test_a_push_without_a_kernel_never_computes_a_pivot(pivoted_flow, read):
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as failed:
        _plan(pivoted_flow, None, read.format(name=_rendered_name(pivoted_flow, ".pivot(")))
    assert failed.value.detail["kind"] == "needs_kernel" and failed.value.detail["cell_id"] == "extra-0"
    assert pivoted_flow.latest_run_info is None, "nothing ran on the canvas"


OTHER_KERNEL = "other-kernel"


@pytest.fixture
def locking_client(client_as, locking_kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


@pytest.fixture
def scripted_flow(open_as):
    """``from_dict -> Python Script on kernel -> filter``, opened; the filter's rows need the script run on the canvas."""
    import flowfile as ff

    def _build(kernel: str, *, mode: str = "Development", cache_results: bool = False, run: bool = True):
        orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
        script = ff.PythonScript(orders, code="x = 1", kernel=kernel)
        flow = open_as(script.output.filter(ff.col("amount") > 10).flow_graph)
        flow.flow_settings.execution_mode = mode
        if cache_results:
            node = flow.get_node(_node_id(flow, "python_script"))
            flow.add_python_script(node.setting_input.model_copy(update={"cache_results": True}))
        if run:
            assert all(result.success for result in flow.run_graph().node_step_result)
            _edit_filter(flow)
        return flow

    return _build


def _node_id(flow, node_type: str) -> int:
    return next(node.node_id for node in flow.nodes if node.node_type == node_type)


def _edit_filter(flow, upstream: str = "python_script") -> None:
    """Change the filter's settings as the editor does, so it has no current result and ``upstream`` still has one."""
    from flowfile_core.notebook.kernel_runner import _has_result

    node = flow.get_node(_node_id(flow, "filter"))
    settings = node.setting_input.model_copy(deep=True)
    settings.filter_input.advanced_filter = "[amount] > 15"
    flow.add_filter(settings)
    assert not _has_result(flow, flow.get_node(node.node_id))
    assert _has_result(flow, flow.get_node(_node_id(flow, upstream)))


def _collect_filter(client, flow, sim) -> tuple[dict, float]:
    started = time.monotonic()
    result = _execute(client, flow, sim, _bind(_node_id(flow, "filter")) + "print(coded.collect().height)")
    return result, time.monotonic() - started


@pytest.mark.parametrize(
    "setup",
    [{"mode": "Performance"}, {"cache_results": True}],
    ids=["performance-mode", "cache-results"],
)
def test_a_rerun_on_the_sessions_own_kernel_is_refused_at_once(
    scripted_flow, locking_client, locking_kernel_sim, setup
):
    flow = scripted_flow(locking_kernel_sim.kernel.id, **setup)
    result, took = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert took < locking_kernel_sim.LOCK_WAIT, result
    assert not result["success"], result
    assert "deadlock" not in result["error"], result
    script_id, filter_id = _node_id(flow, "python_script"), _node_id(flow, "filter")
    assert f"needs node(s) {script_id} to run on this notebook's own kernel" in result["error"], result
    assert f"use Run and preview on canvas for node {filter_id} first" in result["error"], result

    lineage = {filter_id, *flow._get_upstream_node_ids(filter_id)}
    assert all(step.success for step in flow.run_graph(node_ids=lineage).node_step_result)
    result, _ = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert result["success"] and result["stdout"].strip() == "2", "the advice works"


def test_a_never_run_node_on_the_sessions_own_kernel_is_refused_before_the_canvas_runs(
    scripted_flow, locking_client, locking_kernel_sim
):
    flow = scripted_flow(locking_kernel_sim.kernel.id, run=False)
    result, took = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert took < locking_kernel_sim.LOCK_WAIT and not result["success"], result
    assert "Run and preview on canvas" in result["error"], result
    assert flow.latest_run_info is None, "nothing ran on the canvas"


@pytest.mark.parametrize("mode", ["Development", "Performance"])
def test_a_node_on_another_kernel_still_runs(scripted_flow, locking_client, locking_kernel_sim, mode):
    flow = scripted_flow(OTHER_KERNEL, mode=mode)
    result, took = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert result["success"], result
    assert result["stdout"].strip() == "2" and took < locking_kernel_sim.LOCK_WAIT, result


def _node_cells(flow) -> list:
    from flowfile_core.notebook.render import render

    return [cell for cell in render(flow).cells if cell.kind == "node"]


def _script_cell(flow):
    """The rendered cell of the flow's Python Script, and the name it binds to the script's output."""
    script_id = _node_id(flow, "python_script")
    cell = next(cell for cell in _node_cells(flow) if script_id in cell.node_ids)
    return cell.code, cell.defines[-1]


DRAWER_SCRIPT = (
    'flowfile_ctx = globals().get("flowfile_ctx")\n'
    "if flowfile_ctx is not None:\n"
    "    flowfile_ctx.publish_output(flowfile_ctx.read_input())\n"
)
"""A script as the drawer stores one (it publishes itself, and ends in a newline) that also runs in the kernel
sim, which has no ``flowfile_ctx`` and passes the input through."""


@pytest.fixture
def drawer_script_flow(open_as):
    """``from_dict -> Python Script``, the script last, opened and run once on the canvas."""
    import flowfile as ff

    def _build(kernel: str):
        orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
        flow = open_as(ff.PythonScript(orders, cells=[DRAWER_SCRIPT], kernel=kernel).output.flow_graph)
        assert all(result.success for result in flow.run_graph().node_step_result)
        return flow

    return _build


@pytest.mark.parametrize("own_kernel", [False, True], ids=["another-kernel", "the-sessions-own-kernel"])
def test_rerunning_a_script_cell_unchanged_reads_the_canvas_rows(
    drawer_script_flow, locking_client, locking_kernel_sim, own_kernel
):
    """The cell builds the script again under a new id; with the canvas node's settings and inputs it stands for
    that node, so its rows are the canvas's (on the session's own kernel, the result the canvas already has)."""
    flow = drawer_script_flow(locking_kernel_sim.kernel.id if own_kernel else OTHER_KERNEL)
    code, name = _script_cell(flow)
    assert code.startswith("@ff.python_script("), code
    for _ in range(2):
        assert _execute(locking_client, flow, locking_kernel_sim, code)["success"]
        read = _execute(locking_client, flow, locking_kernel_sim, f"print({name}.collect().height)")
        assert read["success"] and read["stdout"].strip() == "3", read
    asked = [body["node_id"] for body in locking_kernel_sim.node_results]
    assert asked == [_node_id(flow, "python_script")], "the second run reuses the path of the first"


def test_run_all_of_a_scripted_flow_computes_below_the_script_on_its_canvas_rows(
    scripted_flow, locking_client, locking_kernel_sim
):
    flow = scripted_flow(OTHER_KERNEL)
    cells = _node_cells(flow)
    for cell in cells:
        result = _execute(locking_client, flow, locking_kernel_sim, cell.code)
        assert result["success"], (cell.code, result)
    read = _execute(locking_client, flow, locking_kernel_sim, f"print({cells[-1].defines[-1]}.collect().height)")
    assert read["success"] and read["stdout"].strip() == "2", read
    asked = [body["node_id"] for body in locking_kernel_sim.node_results]
    assert asked == [_node_id(flow, "python_script")], "only the script's rows come from the canvas"


@pytest.mark.parametrize("edited", ["script", "source"])
def test_an_edited_script_or_source_cell_still_says_push_first(
    scripted_flow, locking_client, locking_kernel_sim, edited
):
    flow = scripted_flow(OTHER_KERNEL)
    code, name = _script_cell(flow)
    if edited == "script":
        assert "x = 1" in code
        code = code.replace("x = 1", "x = 2")
    else:
        source = next(cell.code for cell in _node_cells(flow) if _node_id(flow, "manual_input") in cell.node_ids)
        assert "30" in source
        assert _execute(locking_client, flow, locking_kernel_sim, source.replace("30", "31"))["success"]
    assert _execute(locking_client, flow, locking_kernel_sim, code)["success"]
    read = _execute(locking_client, flow, locking_kernel_sim, f"{name}.collect()")
    assert not read["success"] and "Push, then it runs on the canvas" in read["error"], read
    assert not locking_kernel_sim.node_results


def _interrupted_cell(client, flow, sim) -> tuple[dict, float]:
    """Collect the filter while the script's kernel is busy, Stop the cell once its canvas run started, and
    return its result: the run waits on ``OTHER_KERNEL``, whose lock this holds meanwhile."""
    import threading

    held = sim._locks.setdefault(OTHER_KERNEL, threading.Lock())
    held.acquire()
    results: list = []
    try:
        cell = threading.Thread(target=lambda: results.append(_collect_filter(client, flow, sim)))
        cell.start()
        deadline = time.monotonic() + sim.LOCK_WAIT
        while not flow.flow_settings.is_running and time.monotonic() < deadline:
            time.sleep(0.01)
        assert flow.flow_settings.is_running, "the cell's canvas run started"
        answer = client.post("/notebook/session/interrupt", json={"flow_id": flow.flow_id, "kernel_id": sim.kernel.id})
        assert answer.status_code == 200 and answer.json()["status"] == "interrupted", answer.text
        cell.join(timeout=sim.LOCK_WAIT)
        assert not cell.is_alive(), "the cell came back once its canvas run was cancelled"
    finally:
        held.release()
    return results[0]


def test_interrupting_a_cell_cancels_the_canvas_run_it_waits_on(scripted_flow, locking_client, locking_kernel_sim):
    flow = scripted_flow(OTHER_KERNEL, mode="Performance")
    result, took = _interrupted_cell(locking_client, flow, locking_kernel_sim)
    assert not result["success"] and took < locking_kernel_sim.LOCK_WAIT, result
    assert "cancel" in result["error"].lower(), result


def test_after_stop_the_cell_runs_again(scripted_flow, locking_client, locking_kernel_sim):
    flow = scripted_flow(OTHER_KERNEL, run=False)
    flow.flow_settings.execution_location = "remote"
    result, _ = _interrupted_cell(locking_client, flow, locking_kernel_sim)
    assert not result["success"] and "cancel" in result["error"].lower(), result
    again, _ = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert again["success"] and again["stdout"].strip() == "2", again


def test_a_subflow_node_on_the_sessions_own_kernel_is_refused_at_once(open_as, locking_client, locking_kernel_sim):
    from uuid import uuid4

    import flowfile as ff

    catalog = ff.CatalogReference(f"NbHold_{uuid4().hex[:8]}", auto_create=True)
    child = ff.create_flow_graph()
    raw = ff.FlowInput("orders", schema={"id": ff.Int64, "amount": ff.Int64}, flow_graph=child)
    ff.PythonScript(raw, code="x = 1", kernel=locking_kernel_sim.kernel.id).output.to_flow_output("kept")
    ref = catalog.schema("flows", auto_create=True).register_flow(raw, name=f"child_{uuid4().hex[:8]}")
    run = ff.RunFlow(ref, orders=ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]}))
    flow = open_as(run["kept"].filter(ff.col("amount") > 10).flow_graph)
    assert all(result.success for result in flow.run_graph().node_step_result)
    _edit_filter(flow, upstream="run_flow")

    result, took = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert took < locking_kernel_sim.LOCK_WAIT and not result["success"], result
    assert "needs nodes of a flow it runs (a subflow" in result["error"], result
    assert "Run and preview on canvas" in result["error"] and "another kernel" in result["error"], result


def test_a_virtual_table_producer_node_on_the_sessions_own_kernel_is_refused_at_once(
    open_as, locking_client, locking_kernel_sim
):
    from uuid import uuid4

    import flowfile as ff

    schema = ff.CatalogReference(f"NbHold_{uuid4().hex[:8]}", auto_create=True).schema("tables", auto_create=True)
    table = f"vt_{uuid4().hex[:8]}"
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    produced = ff.PythonScript(orders, code="x = 1", kernel=locking_kernel_sim.kernel.id).output
    producer = produced.write_catalog_table(table, schema=schema, write_mode="virtual")
    assert all(result.success for result in producer.flow_graph.run_graph().node_step_result)
    reader = ff.read_catalog_table(table, schema=schema)
    flow = open_as(
        ff.PythonScript(reader, code="x = 2", kernel=OTHER_KERNEL).output.filter(ff.col("amount") > 10).flow_graph
    )

    result, took = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert took < locking_kernel_sim.LOCK_WAIT and not result["success"], result
    assert "deadlock" not in result["error"] and "a virtual table's producer" in result["error"], result


def test_a_subflow_of_a_virtual_table_producer_on_the_sessions_own_kernel_is_refused_at_once(
    open_as, locking_client, locking_kernel_sim
):
    """The producer runs outside ``run_graph``, so its subflow's run starts without a hold of its own and must
    take the one of the fallback run around it."""
    from uuid import uuid4

    import flowfile as ff

    catalog = ff.CatalogReference(f"NbHold_{uuid4().hex[:8]}", auto_create=True)
    child = ff.create_flow_graph()
    raw = ff.FlowInput("orders", schema={"id": ff.Int64, "amount": ff.Int64}, flow_graph=child)
    ff.PythonScript(raw, code="x = 1", kernel=locking_kernel_sim.kernel.id).output.to_flow_output("kept")
    ref = catalog.schema("flows", auto_create=True).register_flow(raw, name=f"child_{uuid4().hex[:8]}")
    run = ff.RunFlow(ref, orders=ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]}))
    schema = catalog.schema("tables", auto_create=True)
    table = f"vt_{uuid4().hex[:8]}"
    producer = run["kept"].write_catalog_table(table, schema=schema, write_mode="virtual")
    assert all(result.success for result in producer.flow_graph.run_graph().node_step_result)
    reader = ff.read_catalog_table(table, schema=schema)
    flow = open_as(
        ff.PythonScript(reader, code="x = 2", kernel=OTHER_KERNEL).output.filter(ff.col("amount") > 10).flow_graph
    )

    result, took = _collect_filter(locking_client, flow, locking_kernel_sim)
    assert took < locking_kernel_sim.LOCK_WAIT and not result["success"], result
    assert "deadlock" not in result["error"] and "a virtual table's producer" in result["error"], result


def test_a_file_another_kernel_was_handed_is_kept_when_superseded(coded_flow, kernel_sim, monkeypatch):
    """Two kernels' sessions on one flow: kernel B superseding the file kernel A's session reads leaves it, while a
    kernel superseding its own file (its earlier session is gone) removes it."""
    import polars as pl

    from flowfile_core.auth.models import User as PydanticUser
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.notebook import kernel_runner

    monkeypatch.setattr(kernel_sim, "get_kernel_owner", lambda kernel_id: kernel_sim.owner_id)
    kernel_runner._sessions[coded_flow.flow_id] = {"kernel-a": None, "kernel-b": None}
    user = PydanticUser(username="nb_owner", id=kernel_sim.owner_id, disabled=False)
    node_id = _coded_id(coded_flow)

    def fetch(kernel_id: str) -> Path:
        return Path(kernel_runner.node_result(kernel_id, user, coded_flow.flow_id, node_id, None)["path"])

    def rerun() -> None:
        coded_flow.get_node(node_id).results.resulting_data = FlowDataEngine(pl.LazyFrame({"id": [1], "amount": [1]}))

    for_a = fetch("kernel-a")
    rerun()
    for_b = fetch("kernel-b")
    assert for_a.exists() and for_b.exists() and for_a != for_b
    rerun()
    again_b = fetch("kernel-b")
    assert for_a.exists() and again_b.exists() and not for_b.exists()
