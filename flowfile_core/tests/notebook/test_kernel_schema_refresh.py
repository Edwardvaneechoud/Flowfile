"""A notebook kernel session learns the columns the canvas finds after the session was seeded, through the
``kernel-sim`` manager (no Docker)."""

from __future__ import annotations

import pytest

from tests.notebook.conftest import NOTEBOOK_OWNER_ID

LOOPBACK = ("127.0.0.1", 50123)
OTHER_KERNEL = "other-kernel"


@pytest.fixture
def client(client_as, locking_kernel_sim):
    return client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)


@pytest.fixture
def scripted_flow(open_as):
    """``from_dict -> Python Script on another kernel``, opened and not run: the script's columns are unknown."""
    import flowfile as ff

    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return open_as(ff.PythonScript(orders, code="x = 1", kernel=OTHER_KERNEL).output.flow_graph)


def _script_id(flow) -> int:
    return next(node.node_id for node in flow.nodes if node.node_type == "python_script")


def _bind(node_id: int) -> str:
    nodes = "(v for v in list(globals().values()) if type(v).__name__ == 'SeededNode')"
    return f"script = next(v for v in {nodes} if v.node_id == {node_id})\n"


def _execute(client, flow, sim, code: str, cell_id: str = "cell-1") -> dict:
    body = {"flow_id": flow.flow_id, "kernel_id": sim.kernel.id, "cell_id": cell_id, "code": code}
    response = client.post("/notebook/session/execute", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def _canvas_ran(flow, node_id: int, **columns: list) -> None:
    """Run ``node_id``'s lineage on the canvas and leave the node as a script publishing ``columns`` would."""
    import polars as pl

    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

    lineage = {node_id, *flow._get_upstream_node_ids(node_id)}
    assert all(step.success for step in flow.run_graph(node_ids=lineage, commit_sources=False).node_step_result)
    node = flow.get_node(node_id)
    node.results.resulting_data = FlowDataEngine(pl.LazyFrame(columns))
    node._named_schemas = {"output-0": node.results.resulting_data.schema}


def test_a_frame_knows_its_columns_once_the_canvas_ran_its_node(scripted_flow, client, locking_kernel_sim):
    node_id = _script_id(scripted_flow)
    seeded = _execute(client, scripted_flow, locking_kernel_sim, _bind(node_id) + "print(script.columns)")
    assert seeded["success"] and seeded["stdout"].strip() == "['id', 'amount']", seeded

    _canvas_ran(scripted_flow, node_id, column_0=[4950])

    known = _execute(client, scripted_flow, locking_kernel_sim, "print(script.columns)", "cell-2")
    assert known["success"] and known["stdout"].strip() == "['column_0']", known


def test_a_frame_built_below_the_node_follows(scripted_flow, client, locking_kernel_sim):
    node_id = _script_id(scripted_flow)
    built = _execute(
        client, scripted_flow, locking_kernel_sim, _bind(node_id) + "below = script.output.with_columns(x=ff.lit(1))"
    )
    assert built["success"], built

    _canvas_ran(scripted_flow, node_id, column_0=[4950])

    known = _execute(client, scripted_flow, locking_kernel_sim, "print(below.columns)", "cell-2")
    assert known["success"] and known["stdout"].strip() == "['column_0', 'x']", known


def test_a_script_cell_run_again_takes_the_columns_of_the_canvas_node_it_stands_for(
    scripted_flow, client, locking_kernel_sim
):
    node_id = _script_id(scripted_flow)
    source_id = next(node.node_id for node in scripted_flow.nodes if node.node_id != node_id)
    _canvas_ran(scripted_flow, node_id, column_0=[4950])
    frames = "(v for v in list(globals().values()) if type(v).__name__ == 'FlowFrame')"
    cell = (
        f"orders = next(v for v in {frames} if v.node_id == {source_id})\n"
        f"again = ff.PythonScript(orders, code='x = 1', kernel='{OTHER_KERNEL}')"
    )
    assert _execute(client, scripted_flow, locking_kernel_sim, cell)["success"]

    known = _execute(client, scripted_flow, locking_kernel_sim, "print(again.output.columns)", "cell-2")
    assert known["success"] and known["stdout"].strip() == "['column_0']", known


def test_the_schemas_view_follows_the_canvas_too(scripted_flow, client, locking_kernel_sim):
    node_id = _script_id(scripted_flow)
    assert _execute(client, scripted_flow, locking_kernel_sim, _bind(node_id) + "frame = script.output")["success"]
    _canvas_ran(scripted_flow, node_id, column_0=[4950])

    body = {"flow_id": scripted_flow.flow_id, "kernel_id": locking_kernel_sim.kernel.id}
    response = client.post("/notebook/session/schemas", json=body)
    assert response.status_code == 200, response.text
    frames = {frame["name"]: [c["name"] for c in frame["columns"]] for frame in response.json()["dataframes"]}
    assert frames["frame"] == ["column_0"], frames


def test_a_schemas_call_leaves_the_canvas_rows_of_a_file_the_kernel_cannot_see(
    open_as, tmp_path, monkeypatch, client, locking_kernel_sim
):
    """A read the kernel cannot open holds its canvas rows, not a placeholder, so frames built on it compute here;
    the canvas finding a new column must not swap those rows for an empty placeholder."""
    import json

    import flowfile as ff

    folder = tmp_path / "host_data"
    folder.mkdir()
    path = folder / "orders.csv"
    path.write_text("id,amount\n1,10\n2,20\n3,30\n")
    monkeypatch.setenv("FLOWFILE_NOTEBOOK_MOUNTS", json.dumps({str(folder): str(tmp_path / "not_mounted")}))
    flow = open_as(ff.read_csv(str(path)).flow_graph)
    cell = f"src = ff.read_csv({str(path)!r})\nbig = src.filter(ff.col('amount') > 10)"
    assert _execute(client, flow, locking_kernel_sim, cell)["success"]

    path.write_text("id,amount,extra\n1,10,a\n2,20,b\n3,30,c\n")
    assert flow.run_graph().success
    body = {"flow_id": flow.flow_id, "kernel_id": locking_kernel_sim.kernel.id}
    assert client.post("/notebook/session/schemas", json=body).status_code == 200

    heights = "print(src.select('id').collect().height, big.select('id').collect().height)"
    read = _execute(client, flow, locking_kernel_sim, heights, "cell-2")
    assert read["success"] and read["stdout"].strip() == "3 2", read
