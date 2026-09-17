"""How `with_columns` lowers several expressions: one multi-entry Formula node, or Polars code.

Independent expressions (none reads a column another writes) mean the same thing under the
Formula node's sequential evaluation and Polars' parallel evaluation, so they collapse into one
node. Dependent ones do not, and stay on the Polars-code path with Polars' semantics.
"""

import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe
from flowfile_frame import col

DATA = {"a": [1, 2, 3], "b": [10, 20, 30], "name": ["ann", "bob", "cid"]}


def _frame() -> ff.FlowFrame:
    return ff.from_dict(DATA)


def _pl() -> pl.LazyFrame:
    return pl.LazyFrame(DATA)


def _node(frame: ff.FlowFrame):
    return frame.flow_graph.get_node(frame.node_id)


def _entries(frame: ff.FlowFrame) -> list[tuple[str, str, str]]:
    return [
        (entry.field.name, entry.function, entry.field.data_type)
        for entry in _node(frame).setting_input.entries
    ]


# --- 20: independent expressions become one node ---------------------------------------------


def test_three_independent_expressions_emit_one_formula_node():
    out = _frame().with_columns(
        (col("a") + 1).alias("x"),
        (col("b") * 2).alias("y"),
        (col("a") - col("b")).alias("z"),
    )
    assert _node(out).node_type == "formula"
    assert [name for name, _, _ in _entries(out)] == ["x", "y", "z"]
    assert [formula for _, formula, _ in _entries(out)] == ["([a] + 1)", "([b] * 2)", "([a] - [b])"]
    assert_frame_equal(
        out.collect(),
        _pl().with_columns(
            (pl.col("a") + 1).alias("x"),
            (pl.col("b") * 2).alias("y"),
            (pl.col("a") - pl.col("b")).alias("z"),
        ).collect(),
    )


def test_an_expression_reading_the_column_it_writes_is_still_independent():
    out = _frame().with_columns((col("a") + 1).alias("a"), (col("b") * 2).alias("y"))
    assert _node(out).node_type == "formula"
    assert [name for name, _, _ in _entries(out)] == ["a", "y"]


def test_single_expression_matches_the_single_formula_helper():
    expr = (col("a") + 1).alias("x")
    through_with_columns = _frame().with_columns(expr)
    direct = _frame()._with_flowfile_formula("([a] + 1)", "x")
    assert _node(through_with_columns).node_type == "formula"
    assert _entries(through_with_columns) == _entries(direct)
    assert _node(through_with_columns).setting_input.function is not None


# --- 21: dependent expressions keep Polars' parallel semantics --------------------------------


def test_conflicting_expressions_emit_a_polars_code_node():
    out = _frame().with_columns((col("a") + 1).alias("a"), (col("a") * 2).alias("y"))
    assert _node(out).node_type == "polars_code"
    assert not any(node.node_type == "formula" for node in out.flow_graph.nodes)
    assert_frame_equal(
        out.collect(),
        _pl().with_columns((pl.col("a") + 1).alias("a"), (pl.col("a") * 2).alias("y")).collect(),
    )
    # The parallel reading is the one that matters: y is built from the ORIGINAL a.
    assert out.collect()["y"].to_list() == [2, 4, 6]


def test_two_expressions_writing_the_same_column_are_rejected_like_polars():
    """Never a Formula node, where duplicate outputs would silently resolve to last-wins."""
    with pytest.raises(pl.exceptions.ComputeError):
        _frame().with_columns((col("a") + 1).alias("dup"), (col("b") + 1).alias("dup"))
    with pytest.raises(pl.exceptions.ComputeError):
        _pl().with_columns((pl.col("a") + 1).alias("dup"), (pl.col("b") + 1).alias("dup")).collect()


# --- flowfile_formulas= is the sequential contract --------------------------------------------


def test_flowfile_formulas_referencing_an_earlier_output_is_one_sequential_node():
    out = _frame().with_columns(
        flowfile_formulas=["[a] * 10", "[doubled] + 1"],
        output_column_names=["doubled", "next"],
    )
    assert _node(out).node_type == "formula"
    assert [name for name, _, _ in _entries(out)] == ["doubled", "next"]
    result = out.collect()
    assert result["doubled"].to_list() == [10, 20, 30]
    assert result["next"].to_list() == [11, 21, 31]


def test_flowfile_formulas_honour_per_entry_output_datatypes():
    out = _frame().with_columns(
        flowfile_formulas=["[a] * 2", "[b] * 2"],
        output_column_names=["x", "y"],
        output_column_datatypes=["Auto", "Double"],
    )
    assert _node(out).node_type == "formula"
    assert [data_type for _, _, data_type in _entries(out)] == ["Auto", "Double"]
    assert out.collect().schema["y"] == pl.Float64


def test_independent_flowfile_formulas_still_land_on_one_node():
    out = _frame().with_columns(flowfile_formulas=["[a] * 2", "[b] * 2"], output_column_names=["x", "y"])
    assert _node(out).node_type == "formula"
    assert len(_node(out).setting_input.entries) == 2


# --- 22: a multi-entry node survives the export/re-import round trip ---------------------------


def _reimport(frame: ff.FlowFrame) -> ff.FlowFrame:
    """Export the graph as FlowFrame code, execute it, and hand back the rebuilt frame."""
    exec_globals: dict = {}
    exec(export_flow_to_flowframe(frame.flow_graph), exec_globals)
    return exec_globals["run_etl_pipeline"]()


@pytest.mark.parametrize(
    ("formulas", "names"),
    [
        (["[a] * 10", "[doubled] + 1", "[next] * 2"], ["doubled", "next", "tripled"]),
        (["[a] * 10", "[b] + 1", "[a] - [b]"], ["p", "q", "r"]),
    ],
    ids=["dependent", "independent"],
)
def test_multi_entry_node_round_trips_as_one_node(formulas, names):
    original = _frame().with_columns(flowfile_formulas=formulas, output_column_names=names)
    rebuilt = _reimport(original)

    formula_nodes = [node for node in rebuilt.flow_graph.nodes if node.node_type == "formula"]
    assert len(formula_nodes) == 1
    assert [entry.field.name for entry in formula_nodes[0].setting_input.entries] == names
    assert_frame_equal(rebuilt.collect(), original.collect())


if __name__ == "__main__":
    pytest.main([os.path.abspath(__file__)])
