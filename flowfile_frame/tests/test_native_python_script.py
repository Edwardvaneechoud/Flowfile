"""``ff.PythonScript``: a kernel script node built from Python, deferred until the flow runs.

No test here needs Docker: a script without a kernel fails the run before any kernel is
looked up, and building never touches the kernel manager.
"""

from types import SimpleNamespace

import polars as pl
import pytest

import flowfile_frame as ff

from .native_helpers import results_by_id, round_trip

ORDERS = {"order_id": [1, 2, 3], "amount": [5.0, 7.5, 2.5]}
CUSTOMERS = {"customer_id": [10, 20], "name": ["Ann", "Bob"]}


def _script_input(script: ff.PythonScript):
    return script.node.setting_input.python_script_input


# building


def test_cells_are_stored_with_minted_ids_and_the_joined_code():
    cells = ["import polars as pl", "", "df = flowfile_ctx.read_input()", "flowfile_ctx.publish_output(df)"]
    script = ff.PythonScript(ff.from_dict(ORDERS), cells=cells, kernel="ml-kernel", description="score")

    stored = _script_input(script)
    assert [cell.code for cell in stored.cells] == cells
    ids = [cell.id for cell in stored.cells]
    assert len(set(ids)) == len(ids) and all(len(i) == 32 and int(i, 16) >= 0 for i in ids)
    assert stored.code == "\n\n".join(cell for cell in cells if cell)
    assert stored.code == script.code
    assert script.cells == cells
    assert stored.kernel_id == "ml-kernel" == script.kernel
    assert script.node.node_type == "python_script"
    assert script.node.setting_input.description == "score"


def test_code_becomes_a_single_cell():
    script = ff.PythonScript(ff.from_dict(ORDERS), code="x = 1\ny = 2")
    stored = _script_input(script)
    assert [cell.code for cell in stored.cells] == ["x = 1\ny = 2"]
    assert stored.code == "x = 1\ny = 2"
    assert stored.kernel_id is None


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({}, "exactly one of code= or cells="),
        ({"code": "x", "cells": ["x"]}, "exactly one of code= or cells="),
        ({"cells": []}, "at least one cell"),
        ({"cells": ["x", 1]}, "list of strings"),
        ({"code": "x", "outputs": []}, "at least one output name"),
        ({"code": "x", "outputs": ["Main"]}, "Invalid python_script settings"),
        ({"code": "x", "kernel": 42}, "kernel= takes a kernel id"),
    ],
)
def test_invalid_arguments_raise(kwargs, match):
    source = ff.from_dict(ORDERS)
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.PythonScript(source, **kwargs)
    assert [n.node_id for n in source.flow_graph.nodes] == [source.node_id]


def test_kernel_object_contributes_its_id_and_is_not_validated():
    script = ff.PythonScript(ff.from_dict(ORDERS), code="x = 1", kernel=SimpleNamespace(id="no-such-kernel"))
    assert _script_input(script).kernel_id == "no-such-kernel"


def test_outputs_become_handles():
    script = ff.PythonScript(ff.from_dict(ORDERS), code="x = 1", outputs=["main", "metrics"])
    assert script.outputs == ["main", "metrics"]
    assert script.node.setting_input.output_names == ["main", "metrics"]
    assert (script["main"].output_handle, script["metrics"].output_handle) == ("output-0", "output-1")
    with pytest.raises(ff.NativeNodeError, match=r"outputs \['main', 'metrics'\]"):
        _ = script.output


def test_inputs_go_to_input_0_in_order():
    orders, customers = ff.from_dict(ORDERS), ff.from_dict(CUSTOMERS)
    script = ff.PythonScript(orders, customers, code="x = 1")
    assert orders.flow_graph is customers.flow_graph is script.flow_graph
    core = script.node
    assert [n.node_id for n in core.node_inputs.main_inputs] == [orders.node_id, customers.node_id]
    assert core.setting_input.depending_on_ids == [orders.node_id, customers.node_id]


# deferral


def test_outputs_are_deferred_with_the_first_inputs_schema():
    orders, customers = ff.from_dict(ORDERS), ff.from_dict(CUSTOMERS)
    script = ff.PythonScript(orders, customers, code="x = 1", kernel="ml-kernel", outputs=["main", "metrics"])

    core = script.node
    assert core.deferred_until_run is True
    expected = orders.data.collect_schema()
    for name in ("main", "metrics"):
        frame = script[name]
        assert frame._deferred is True
        assert frame.data.collect_schema() == expected
        assert frame.data.collect().height == 0
    downstream = script["main"].select("amount")
    assert downstream._deferred is True
    assert core.deferred_until_run is True
    assert core.results.errors is None


def test_zero_input_script_never_runs_at_placement():
    flow = ff.create_flow_graph()
    script = ff.PythonScript(code="flowfile_ctx.publish_output(pl.LazyFrame({'a': [1]}))", flow_graph=flow)
    core = script.node
    assert script.flow_graph is flow
    assert core.deferred_until_run is True
    assert core.results.errors is None
    assert core.node_stats.has_run_with_current_setup is False
    assert script.output._deferred is True
    assert script.output.data.collect_schema() == pl.Schema()


def test_without_a_kernel_the_run_fails_at_the_node():
    script = ff.PythonScript(ff.from_dict(ORDERS), code="x = 1")
    with pytest.raises(ff.NativeNodeError, match=f"node {script.node_id}: No kernel selected"):
        script.output.collect()

    run_info = script.flow_graph.run_graph()
    result = results_by_id(run_info)[script.node_id]
    assert result.success is False
    assert "kernel" in result.error


# save / open


def test_round_trip_keeps_cells_code_kernel_and_outputs():
    orders = ff.from_dict(ORDERS)
    script = ff.PythonScript(
        orders, cells=["a = 1", "b = 2"], kernel="ml-kernel", outputs=["main", "metrics"], description="kernel step"
    )
    out = script["metrics"].select("amount")

    reopened, _ = round_trip(out, "script_roundtrip.yaml")
    reopened_script = reopened.get_node(script.node_id)
    assert reopened_script.node_type == "python_script"
    assert reopened_script.setting_input.python_script_input == _script_input(script)
    assert reopened_script.setting_input.output_names == ["main", "metrics"]
    assert reopened_script.setting_input.description == "kernel step"
    assert reopened_script.node_inputs.main_inputs[0].node_id == orders.node_id
    assert reopened.get_node(out.node_id)._input_output_handles[script.node_id] == "output-1"
