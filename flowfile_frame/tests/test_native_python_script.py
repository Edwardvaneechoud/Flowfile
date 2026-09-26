"""``ff.PythonScript``: a kernel script node built from Python, deferred until the flow runs.

Only the ``kernel``-marked end-to-end test needs Docker: a script without a kernel fails the
run before any kernel is looked up, and building never touches the kernel manager.
"""

import ast
import asyncio
import code
import dataclasses
import functools
import inspect
import json
import subprocess
import sys
import types
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_frame.console_source import console_function_source

from .native_helpers import results_by_id, round_trip
from .utils import is_docker_available

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


# decorator: sample functions (module level: the decorator refuses nested functions)

MONTHLY = {"month": [1, 2, 3], "revenue": [10.0, 20.0, 30.0]}
GROWTH = 1.05
LOOKUP = None

# fmt: off
# The brief's example verbatim (no blank line after the import), a one-line def and a docstring sharing its line.


@ff.python_script(kernel="lite", returns={"month": ff.Int64, "revenue_forecast": ff.Float64})
def forecast(monthly: pl.LazyFrame) -> pl.DataFrame:
    """Revenue trend: a least-squares line through monthly revenue, extended three months."""
    import numpy as np
    df = monthly.collect()

    # %% [markdown]
    # ## Fit
    # One slope for the whole period; good enough for a demo, not for a quarter close.

    # %%
    slope, intercept = np.polyfit(df["month"], df["revenue"], deg=1)
    ahead = np.arange(df["month"].max() + 1, df["month"].max() + 4)

    # %% Forecast
    return pl.DataFrame({"month": ahead, "revenue_forecast": slope * ahead + intercept})


@ff.python_script()
def identity(orders): return orders


def docstring_shares_a_line(orders):
    """Only the first three orders."""; kept = orders.head(3)
    return kept


# fmt: on


FORECAST_CELLS = [
    "import polars as pl",
    '# flowfile: inputs\nmonthly = flowfile_ctx.read_inputs()["main"][0]',
    "# Revenue trend: a least-squares line through monthly revenue, extended three months.",
    "import numpy as np\ndf = monthly.collect()",
    "# ## Fit\n# One slope for the whole period; good enough for a demo, not for a quarter close.",
    'slope, intercept = np.polyfit(df["month"], df["revenue"], deg=1)\n'
    'ahead = np.arange(df["month"].max() + 1, df["month"].max() + 4)',
    "# Forecast\n"
    "# flowfile: outputs\n"
    '_result = pl.DataFrame({"month": ahead, "revenue_forecast": slope * ahead + intercept})\n'
    'flowfile_ctx.publish_output(_result, "main")',
]


@ff.python_script()
def projected(monthly):
    df = monthly.collect()
    return df.with_columns(projected=df["revenue"] * GROWTH ** np.arange(df.height))


@ff.python_script()
def first_rows(orders):
    head = orders.head(2)
    return head


@ff.python_script(
    outputs=["a", "b"],
    returns={"a": {"order_id": ff.Int64}, "b": {"name": ff.String, "score": ff.Float64}},
)
def split_pair(left, right):
    return {"a": left, "b": right}


@ff.python_script()
def nested_blocks(orders):
    df = orders.collect()
    total = 0.0
    for amount in df["amount"]:
        if amount > 3:
            total += amount
    # %%
    return df.head(1)


@ff.python_script()
def marker_in_string(orders):
    label = "# %% not a marker"
    notes = """
    # %% not a marker either
    """
    return orders.head(1)


@ff.python_script()
def titled_cells(orders):
    df = orders.collect()

    # %%
    # %%

    # %% Summarise
    summary = df.head(1)
    return summary


@ff.python_script()
def note_then_code(orders):
    # %% [markdown] Background
    # Only the first three orders.
    kept = orders.head(3)
    return kept


@ff.python_script(returns={"month": ff.Int64, "revenue_forecast": ff.Float64, "growing": ff.Boolean})
def growth(monthly):
    df = monthly.collect()
    return df.select("month", revenue_forecast=pl.col("revenue") * 1.1, growing=pl.col("revenue").diff() > 0)


@ff.python_script()
def unseeded(orders):
    return orders.head(1)


def make_rows():
    return pl.LazyFrame({"a": [1, 2]})


def with_star_args(*frames):
    return frames[0]


def with_keyword_only(orders, *, limit):
    return orders.head(limit)


def no_return(orders):
    orders.head(1)


def return_inside_if(orders):
    if orders is not None:
        return orders


def two_returns(orders):
    head = orders.head(1)
    if head is None:
        return orders
    return head


def returns_a_dict(left, right):
    return {"a": left, "b": right}


def returns_a_tuple(left, right):
    return left, right


def marker_in_loop(orders):
    for _ in range(1):
        # %%
        orders = orders.head(1)
    return orders


def _helper(frame):
    return frame.head(1)


def uses_helper(orders):
    return _helper(orders)


def uses_lookup(orders):
    return LOOKUP


def uses_later_helper(orders):
    return later_helper(orders)


def later_helper(frame):
    return frame.head(1)


def _timed(fn):
    @functools.wraps(fn)
    def wrapper(*frames):
        return fn(*frames)

    return wrapper


@_timed
def wrapped(orders):
    return orders


def with_default(orders, limit=3):
    return orders.head(limit)


def rebinds_outer(orders):
    seen = 0

    def count(frame):
        nonlocal seen
        seen += 1
        return frame

    return count(orders)


@ff.python_script()
def body_class(orders):
    from datetime import date

    @dataclasses.dataclass
    class Cutoff:
        day: date

    cutoff: date = Cutoff(date(2024, 1, 1)).day
    return orders.with_columns(pl.lit(cutoff).alias("cutoff"))


# decorator: cells


def test_forecast_example_produces_the_documented_cells():
    assert forecast.cells == FORECAST_CELLS


def test_used_module_and_constant_become_the_prelude_imports_first():
    assert projected.cells == [
        "import numpy as np\nGROWTH = 1.05",
        '# flowfile: inputs\nmonthly = flowfile_ctx.read_inputs()["main"][0]',
        "df = monthly.collect()\n"
        "# flowfile: outputs\n"
        '_result = df.with_columns(projected=df["revenue"] * GROWTH ** np.arange(df.height))\n'
        'flowfile_ctx.publish_output(_result, "main")',
    ]


def test_no_markers_and_no_free_names_give_the_inputs_cell_and_one_body_cell():
    assert first_rows.cells == [
        '# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]',
        'head = orders.head(2)\n# flowfile: outputs\n_result = head\nflowfile_ctx.publish_output(_result, "main")',
    ]


def test_one_line_function_is_unwrapped():
    assert identity.cells == [
        '# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]',
        '# flowfile: outputs\n_result = orders\nflowfile_ctx.publish_output(_result, "main")',
    ]


def test_two_inputs_and_a_dict_return_publish_each_output_with_a_key_check():
    assert split_pair.cells == [
        "# flowfile: inputs\n"
        'left = flowfile_ctx.read_inputs()["main"][0]\n'
        'right = flowfile_ctx.read_inputs()["main"][1]',
        "# flowfile: outputs\n"
        '_result = {"a": left, "b": right}\n'
        'if not isinstance(_result, dict) or set(_result) != {"a", "b"}:\n'
        "    raise ValueError(\"split_pair must return a dict with the keys ['a', 'b']\")\n"
        'for _name in ["a", "b"]:\n'
        "    flowfile_ctx.publish_output(_result[_name], _name)",
    ]


def test_nested_blocks_keep_their_relative_indentation():
    assert nested_blocks.cells == [
        '# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]',
        "df = orders.collect()\n"
        "total = 0.0\n"
        'for amount in df["amount"]:\n'
        "    if amount > 3:\n"
        "        total += amount",
        '# flowfile: outputs\n_result = df.head(1)\nflowfile_ctx.publish_output(_result, "main")',
    ]


def test_marker_inside_a_string_literal_is_not_a_marker():
    assert marker_in_string.cells == [
        '# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]',
        'label = "# %% not a marker"\n'
        'notes = """\n'
        "    # %% not a marker either\n"
        '    """\n'
        "# flowfile: outputs\n"
        "_result = orders.head(1)\n"
        'flowfile_ctx.publish_output(_result, "main")',
    ]


def test_titled_marker_keeps_its_title_and_consecutive_markers_leave_no_empty_cell():
    assert titled_cells.cells == [
        '# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]',
        "df = orders.collect()",
        "# Summarise\n"
        "summary = df.head(1)\n"
        "# flowfile: outputs\n"
        "_result = summary\n"
        'flowfile_ctx.publish_output(_result, "main")',
    ]


def test_note_ends_at_the_first_code_line_and_keeps_its_title():
    assert note_then_code.cells == [
        '# flowfile: inputs\norders = flowfile_ctx.read_inputs()["main"][0]',
        "# Background\n# Only the first three orders.",
        'kept = orders.head(3)\n# flowfile: outputs\n_result = kept\nflowfile_ctx.publish_output(_result, "main")',
    ]


def test_inner_class_and_names_the_body_binds_stay_out_of_the_prelude():
    assert body_class.cells[0] == "import dataclasses\nimport polars as pl"
    import datetime

    assert body_class.fn(pl.LazyFrame(ORDERS)).collect()["cutoff"].to_list() == [datetime.date(2024, 1, 1)] * 3


def test_function_without_parameters_has_no_inputs_cell():
    flow = ff.create_flow_graph()
    rows = ff.python_script(flow_graph=flow)(make_rows)
    assert rows.cells == [
        "import polars as pl",
        '# flowfile: outputs\n_result = pl.LazyFrame({"a": [1, 2]})\nflowfile_ctx.publish_output(_result, "main")',
    ]
    out = rows()
    assert out.flow_graph is flow
    assert out._deferred is True
    assert out.data.collect_schema() == pl.Schema()


def test_single_frame_return_publishes_to_the_one_named_output():
    renamed = ff.python_script(outputs=["scored"])(first_rows.fn)
    assert renamed.cells[-1] == (
        'head = orders.head(2)\n# flowfile: outputs\n_result = head\nflowfile_ctx.publish_output(_result, "scored")'
    )


# decorator: the placed node


def test_decorated_function_places_a_python_script_node():
    monthly = ff.from_dict(MONTHLY)
    script = forecast.node(monthly)

    assert isinstance(script, ff.PythonScript)
    stored = _script_input(script)
    assert [cell.code for cell in stored.cells] == FORECAST_CELLS
    assert stored.code == "\n\n".join(FORECAST_CELLS) == script.code
    assert stored.kernel_id == "lite"
    assert script.node.node_type == "python_script"
    assert script.node.setting_input.output_names == ["main"]
    assert script.node.setting_input.description == "forecast"
    assert [n.node_id for n in script.node.node_inputs.main_inputs] == [monthly.node_id]
    assert script.node.deferred_until_run is True
    assert script.output._deferred is True
    assert script.node.results.errors is None


def test_calling_the_function_returns_the_deferred_output_frame():
    monthly = ff.from_dict(MONTHLY)
    out = forecast(monthly)
    assert isinstance(out, ff.FlowFrame)
    assert out._deferred is True
    assert out.flow_graph is monthly.flow_graph
    assert out.flow_graph.get_node(out.node_id).node_type == "python_script"
    assert out.data.collect().height == 0


def test_function_wrapper_keeps_name_doc_and_the_original_function():
    assert isinstance(forecast, ff.PythonScriptFunction)
    assert forecast.__name__ == "forecast"
    assert forecast.__doc__ == "Revenue trend: a least-squares line through monthly revenue, extended three months."
    assert forecast.__wrapped__ is forecast.fn
    assert not isinstance(forecast.fn, ff.PythonScriptFunction)


def test_kernel_object_and_description_are_passed_to_the_node():
    labelled = ff.python_script(kernel=SimpleNamespace(id="ml-kernel"), description="first two")(first_rows.fn)
    script = labelled.node(ff.from_dict(ORDERS))
    assert _script_input(script).kernel_id == "ml-kernel"
    assert script.node.setting_input.description == "first two"


def test_decorated_node_round_trips():
    monthly = ff.from_dict(MONTHLY)
    script = forecast.node(monthly)
    out = script.output.select("month")

    reopened, _ = round_trip(out, "decorated_script.yaml")
    reopened_script = reopened.get_node(script.node_id)
    assert reopened_script.node_type == "python_script"
    assert reopened_script.setting_input.python_script_input == _script_input(script)
    assert [cell.code for cell in reopened_script.setting_input.python_script_input.cells] == FORECAST_CELLS
    assert reopened_script.setting_input.output_names == ["main"]
    assert reopened_script.setting_input.description == "forecast"
    assert reopened_script.node_inputs.main_inputs[0].node_id == monthly.node_id


def test_function_runs_locally_on_polars_frames():
    result = forecast.fn(pl.LazyFrame(MONTHLY))
    expected = pl.DataFrame({"month": [4, 5, 6], "revenue_forecast": [40.0, 50.0, 60.0]})
    assert_frame_equal(result, expected, check_exact=False)


# decorator: returns= and schemas=


def test_returns_seeds_the_declared_schema_for_frames_built_on_the_output():
    declared = pl.Schema({"month": pl.Int64, "revenue_forecast": pl.Float64, "growing": pl.Boolean})
    monthly = ff.from_dict(MONTHLY)
    out = growth(monthly)
    assert "growing" not in monthly.data.collect_schema()
    assert out.data.collect_schema() == declared

    growing = out.filter(ff.col("growing"))
    doubled = out.with_columns(ff.col("revenue_forecast") * 2)
    for frame in (growing, doubled):
        assert frame._deferred is True
        assert frame.data.collect_schema() == declared
        assert frame.data.collect().height == 0


def test_without_returns_the_placeholder_keeps_the_first_inputs_columns():
    orders, customers = ff.from_dict(ORDERS), ff.from_dict(CUSTOMERS)
    assert unseeded(orders).data.collect_schema() == orders.data.collect_schema()
    script = ff.python_script(outputs=["a", "b"])(returns_a_dict).node(customers, orders)
    for name in ("a", "b"):
        assert script[name].data.collect_schema() == customers.data.collect_schema()


def test_multi_output_function_is_placed_with_node_and_each_output_is_seeded():
    orders, customers = ff.from_dict(ORDERS), ff.from_dict(CUSTOMERS)
    script = split_pair.node(orders, customers)

    assert script.outputs == ["a", "b"]
    assert script.node.setting_input.output_names == ["a", "b"]
    assert [n.node_id for n in script.node.node_inputs.main_inputs] == [orders.node_id, customers.node_id]
    assert (script["a"].output_handle, script["b"].output_handle) == ("output-0", "output-1")
    assert script["a"].data.collect_schema() == pl.Schema({"order_id": pl.Int64})
    assert script.get_output("b").data.collect_schema() == pl.Schema({"name": pl.String, "score": pl.Float64})
    assert script["a"]._deferred is script["b"]._deferred is True


def test_schemas_declare_an_outputs_columns_and_the_rest_keep_the_first_inputs():
    orders = ff.from_dict(ORDERS)
    script = ff.PythonScript(
        orders,
        code="x = 1",
        outputs=["main", "metrics"],
        schemas={"metrics": {"rows": ff.Int64, "total": ff.Float64, "tags": pl.List(pl.String)}},
    )
    assert script["metrics"].data.collect_schema() == pl.Schema(
        {"rows": pl.Int64, "total": pl.Float64, "tags": pl.List(pl.String)}
    )
    assert script["main"].data.collect_schema() == orders.data.collect_schema()
    assert script["metrics"]._deferred is True


@pytest.mark.parametrize(
    "schemas, match",
    [
        ({"other": {"a": ff.Int64}}, r"schemas= declares \['other'\], which are not outputs of the node"),
        ({"main": {"a": "Int64"}}, "schemas= maps column names to Polars dtypes"),
        ({"main": ["a"]}, "schemas= maps each output to"),
    ],
)
def test_invalid_schemas_raise_and_leave_no_node(schemas, match):
    source = ff.from_dict(ORDERS)
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.PythonScript(source, code="x = 1", schemas=schemas)
    assert [n.node_id for n in source.flow_graph.nodes] == [source.node_id]


# decorator: errors


def test_lambda_is_refused():
    with pytest.raises(ff.NativeNodeError, match="a lambda has no body"):
        ff.python_script()(lambda orders: orders)


def test_nested_function_is_refused():
    def inner(orders):
        return orders

    with pytest.raises(ff.NativeNodeError, match="`inner` is defined inside another function"):
        ff.python_script()(inner)


@pytest.mark.parametrize(
    "fn, kwargs, match",
    [
        (with_star_args, {}, r"`frames` is \*args"),
        (with_keyword_only, {}, "`limit` is keyword-only"),
        (no_return, {}, r"`no_return` must end with `return <frame>`"),
        (return_inside_if, {}, r"`return_inside_if` must return exactly once.*found 1 return"),
        (two_returns, {}, r"`two_returns` must return exactly once.*found 2 return"),
        (returns_a_dict, {}, r"`returns_a_dict` returns a dict; name its outputs with outputs="),
        (returns_a_dict, {"outputs": ["a", "c"]}, r"returns the keys \['a', 'b'\], but outputs= is \['a', 'c'\]"),
        (returns_a_tuple, {}, r"`returns_a_tuple` returns a tuple; return one frame, or a dict of frames"),
        (returns_a_tuple, {"outputs": ["a", "b"]}, r"`returns_a_tuple` returns a tuple; return one frame"),
        (docstring_shares_a_line, {}, "The docstring of `docstring_shares_a_line` must end on its own line"),
        (marker_in_loop, {}, "cell markers must sit between top-level statements of `marker_in_loop`"),
        (uses_helper, {}, "`_helper` is used inside `uses_helper` but lives outside it"),
        (
            returns_a_dict,
            {"outputs": ["a", "b"], "returns": {"a": {"x": ff.Int64}, "c": {"y": ff.Int64}}},
            r"returns= declares \['c'\], which are not outputs of the node",
        ),
        (returns_a_dict, {"outputs": ["a", "b"], "returns": {"x": ff.Int64}}, r"returns= of a function with outputs"),
        (first_rows.fn, {"returns": {"x": "Int64"}}, "returns= maps column names to Polars dtypes"),
        (first_rows.fn, {"returns": {"x": pl.List}}, "incomplete dtype"),
        (wrapped, {}, "`wrapped` is wrapped by another decorator"),
        (with_default, {}, "parameter `limit` has a default"),
        (rebinds_outer, {}, "does not run as top-level notebook cells: no binding for nonlocal 'seen'"),
    ],
)
def test_invalid_function_is_refused_at_decoration(fn, kwargs, match):
    with pytest.raises(ff.NativeNodeError, match=match):
        ff.python_script(**kwargs)(fn)


def test_marker_error_names_the_line_of_the_marker():
    line = marker_in_loop.__code__.co_firstlineno + 2
    with pytest.raises(ff.NativeNodeError, match=f"the one on line {line} is inside a block"):
        ff.python_script()(marker_in_loop)


def test_closure_over_a_flow_frame_is_refused(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOOKUP", ff.from_dict(ORDERS))
    with pytest.raises(ff.NativeNodeError, match="`LOOKUP` is used inside `uses_lookup` but lives outside it"):
        ff.python_script()(uses_lookup)


def test_helper_defined_below_the_function_is_refused(monkeypatch):
    monkeypatch.delattr(sys.modules[__name__], "later_helper")
    with pytest.raises(
        ff.NativeNodeError, match="`later_helper` is used inside `uses_later_helper` but is not defined"
    ):
        ff.python_script()(uses_later_helper)


def test_wrong_number_of_frames_lists_the_parameters_and_leaves_no_node():
    orders = ff.from_dict(ORDERS)
    with pytest.raises(ff.NativeNodeError, match=r"`split_pair` takes 2 input frame\(s\) \(left, right\), got 1"):
        split_pair.node(orders)
    with pytest.raises(ff.NativeNodeError, match=r"`forecast` takes 1 input frame\(s\) \(monthly\), got 2"):
        forecast(orders, ff.from_dict(CUSTOMERS))
    assert [n.node_id for n in orders.flow_graph.nodes] == [orders.node_id]


def test_non_frame_argument_is_refused():
    with pytest.raises(ff.NativeNodeError, match="`monthly` got LazyFrame"):
        forecast(pl.LazyFrame(MONTHLY))


def test_calling_a_multi_output_function_points_at_node_and_leaves_no_node():
    orders, customers = ff.from_dict(ORDERS), ff.from_dict(CUSTOMERS)
    with pytest.raises(ff.NativeNodeError, match=r"outputs \['a', 'b'\]; place it with split_pair\.node\(\.\.\.\)"):
        split_pair(orders, customers)
    assert [n.node_id for n in orders.flow_graph.nodes] == [orders.node_id]
    assert [n.node_id for n in customers.flow_graph.nodes] == [customers.node_id]


# decorator: source from a console (PyCharm runs a selection through the `code` module, which keeps no source)


class _PyCharmConsole(code.InteractiveConsole):
    """``code.InteractiveConsole`` as PyCharm's ``pydevconsole._AsyncioInteractiveConsole`` runs it; errors propagate."""

    def __init__(self, namespace: dict) -> None:
        super().__init__(namespace)
        self.compile.compiler.flags |= ast.PyCF_ALLOW_TOP_LEVEL_AWAIT

    def runcode(self, code_object) -> None:
        types.FunctionType(code_object, self.locals)()


def _console(*fragments: str, console=_PyCharmConsole, filename: str = "<input>") -> dict[str, Any]:
    """Run each fragment as PyCharm's "execute selection" does: ``runsource(text, "<input>", "exec")``."""
    namespace: dict[str, Any] = {}
    interpreter = console(namespace)
    for fragment in fragments:
        assert interpreter.runsource(fragment, filename, "exec") is False  # complete input, not waiting for more
    return namespace


@pytest.mark.parametrize("console, filename", [(_PyCharmConsole, "<input>"), (code.InteractiveConsole, "<console>")])
def test_function_decorated_in_a_console_gets_the_cells_of_the_same_code_in_a_file(console, filename):
    selection = "import flowfile_frame as ff\nimport polars as pl\n\n" + inspect.getsource(forecast.fn)
    namespace = _console(selection, console=console, filename=filename)

    with pytest.raises(OSError):
        inspect.getsource(namespace["forecast"].fn)
    assert console_function_source(namespace["forecast"].fn) == inspect.getsource(forecast.fn)
    assert namespace["forecast"].cells == FORECAST_CELLS


def test_selection_that_imports_flowfile_itself_works_on_its_first_run():
    """PyCharm's usual case: the whole script is one selection, compiled before flowfile (and its hook) was imported."""
    selection = (
        "import flowfile_frame as ff\nimport polars as pl\n\n"
        + inspect.getsource(forecast.fn)
        + '\nout = ff.from_dict({"amount": [1.0]}).with_columns('
        'ff.col("amount").map_elements(lambda x: x * 2, return_dtype=pl.Float64).alias("doubled"))\n'
    )
    script = "\n".join(
        [
            "import ast, code, json, sys, types",
            inspect.getsource(_PyCharmConsole),
            "assert 'flowfile_frame' not in sys.modules",
            "namespace = {}",
            "_PyCharmConsole(namespace).runsource(sys.stdin.read(), '<input>', 'exec')",
            "polars_code = namespace['out'].get_node_settings().setting_input.polars_code_input.polars_code",
            "print(json.dumps({'cells': namespace['forecast'].cells, 'polars_code': polars_code}))",
        ]
    )
    run = subprocess.run([sys.executable, "-c", script], input=selection, capture_output=True, text=True, timeout=300)

    assert run.returncode == 0, run.stderr[-3000:]
    result = json.loads(run.stdout.splitlines()[-1])
    assert result["cells"] == FORECAST_CELLS
    assert "serialized_value" not in result["polars_code"] and "return x * 2" in result["polars_code"]


def test_function_defined_in_one_fragment_and_decorated_in_a_later_one_is_found():
    namespace = _console(
        "def first_rows(orders):\n    head = orders.head(2)\n    return head\n",
        "import flowfile_frame as ff\nfirst_rows = ff.python_script()(first_rows)\n",
    )
    assert namespace["first_rows"].cells == first_rows.cells


def test_same_named_functions_starting_on_different_lines_each_get_their_own_body():
    namespace = _console(
        "def pick(orders):\n    return orders.head(1)\n",
        "older = pick\n\n\ndef pick(orders):\n    return orders.head(2)\n",
    )
    older, newer = ff.python_script()(namespace["older"]), ff.python_script()(namespace["pick"])
    assert "_result = orders.head(1)" in older.cells[-1]
    assert "_result = orders.head(2)" in newer.cells[-1]


def test_a_redefinition_on_the_same_line_does_not_answer_for_the_function_it_replaced():
    namespace = _console(
        "def pick(orders):\n    return orders.head(1)\n",
        "older = pick\n",
        "def pick(orders):\n    return orders.head(2)\n",
    )
    assert "_result = orders.head(1)" in ff.python_script()(namespace["older"]).cells[-1]


def test_function_exec_d_from_a_string_is_still_refused():
    namespace: dict[str, Any] = {}
    exec("def made(orders):\n    return orders\n", namespace)
    with pytest.raises(ff.NativeNodeError, match="python_script needs the source of `made`"):
        ff.python_script()(namespace["made"])


def test_map_elements_lambda_from_a_console_becomes_code_not_a_serialized_frame():
    namespace = _console(
        "double = lambda x: x * 2\n",
        "triple = lambda x: x * 3\n",
        "import flowfile_frame as ff\nimport polars as pl\n"
        'out = ff.from_dict({"amount": [1.0, 2.5]}).with_columns('
        'ff.col("amount").map_elements(double, return_dtype=pl.Float64).alias("doubled"))\n',
    )
    polars_code = namespace["out"].get_node_settings().setting_input.polars_code_input.polars_code
    assert "serialized_value" not in polars_code
    assert "return x * 2" in polars_code
    assert namespace["out"].collect()["doubled"].to_list() == [2.0, 5.0]


# decorator: kernel end-to-end (Docker)


E2E_KERNEL_ID = f"ff-python-script-e2e-{uuid4().hex[:8]}"


@ff.python_script(kernel=E2E_KERNEL_ID, outputs=["a", "b"])
def orders_by_customer(orders, customers):
    """Large orders with their customer's name, and the customers without an order."""
    named = orders.join(customers, on="customer_id", how="left").filter(pl.col("amount") >= 5).sort("order_id")

    # %%
    return {"a": named, "b": customers.join(orders, on="customer_id", how="anti").collect()}


@pytest.fixture
def e2e_kernel(monkeypatch, tmp_path):
    """The kernel ``orders_by_customer`` names, on a manager of its own; deleted afterwards, and nothing else touched.

    Skips without Docker or without a kernel image present locally: it never pulls or builds one.
    """
    if not is_docker_available():
        pytest.skip("Docker is not available")
    import flowfile_core.kernel as kernel_module
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import ImageFlavour, KernelConfig

    # This manager's registry lacks the developer's kernels, so its startup GC must not reap their containers.
    monkeypatch.setenv("FLOWFILE_KERNEL_GC", "0")
    manager = KernelManager(shared_volume_path=str(tmp_path))
    if manager.resolve_local_image(ImageFlavour.BASE) is None:
        pytest.skip("No kernel image present locally; this test never pulls or builds one")
    monkeypatch.setattr(kernel_module, "_manager", manager)
    asyncio.run(manager.create_kernel(KernelConfig(id=E2E_KERNEL_ID, name="python_script e2e"), user_id=1))
    try:
        yield E2E_KERNEL_ID
    finally:
        asyncio.run(manager.delete_kernel(E2E_KERNEL_ID))


@pytest.mark.kernel
def test_decorated_function_runs_on_a_kernel(e2e_kernel):
    orders = ff.from_dict({"order_id": [1, 2, 3], "customer_id": [10, 20, 10], "amount": [5.0, 7.5, 2.5]})
    customers = ff.from_dict({"customer_id": [10, 20, 30], "name": ["Ann", "Bob", "Cid"]})

    script = orders_by_customer.node(orders, customers)

    assert script.node.setting_input.python_script_input.kernel_id == e2e_kernel
    assert script["a"].collect().to_dicts() == [
        {"order_id": 1, "customer_id": 10, "amount": 5.0, "name": "Ann"},
        {"order_id": 2, "customer_id": 20, "amount": 7.5, "name": "Bob"},
    ]
    assert script["b"].collect().to_dicts() == [{"customer_id": 30, "name": "Cid"}]
