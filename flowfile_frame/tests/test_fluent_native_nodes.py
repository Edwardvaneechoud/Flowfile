"""Fluent calls that place exactly one canvas node of the canvas's own type (the exactness ledger's rows):
``polars_code``, ``with_row_index``, ``unpivot`` and ``std``/``var`` aggregations, plus the rule that a
fluent call writes no description of its own."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_frame.native import NativeNodeError

from .native_helpers import core_node, round_trip

DATA = {"g": ["a", "a", "b", "b"], "x": [1.0, 2.0, 3.0, 5.0], "y": [1, 2, 3, 4]}


def _frame() -> ff.FlowFrame:
    return ff.from_dict(DATA)


def _code(frame: ff.FlowFrame) -> str:
    return core_node(frame).setting_input.polars_code_input.polars_code


def _new_nodes(known: set[int], after: ff.FlowFrame) -> list[str]:
    return [n.node_type for n in after.flow_graph.nodes if n.node_id not in known]


def _ids(frame: ff.FlowFrame) -> set[int]:
    return {n.node_id for n in frame.flow_graph.nodes}


def test_polars_code_stores_a_string_dedented_and_stripped():
    frame = _frame()
    known = _ids(frame)
    out = frame.polars_code(
        """
            output_df = input_df.filter(pl.col("y") > 1)
            output_df = output_df.head(2)
        """
    )
    assert _new_nodes(known, out) == ["polars_code"]
    assert _code(out) == 'output_df = input_df.filter(pl.col("y") > 1)\noutput_df = output_df.head(2)'
    assert out.collect()["y"].to_list() == [2, 3]


def _one_return(input_df: ff.FlowFrame):
    return input_df.with_columns((pl.col("x") * 2).alias("x2"))


# Keep the big ones.
def big_ones(orders: pl.LazyFrame) -> pl.LazyFrame:
    output_df = orders.filter(pl.col("x") > 1.5)
    return output_df.select("g", "x")


def _polars_code_3(input_df):
    # keep the big ones
    output_df = input_df.filter(pl.col("x") > 1.5)
    output_df = output_df.select("g", "x")
    return output_df


def _free_form(input_df):
    doubled = input_df.with_columns(pl.col("y") * 2)
    output_df = doubled.head(1)


@pytest.mark.parametrize(
    "fn, code, height",
    [
        (
            _one_return,
            'def _one_return(input_df: ff.FlowFrame):\n    return input_df.with_columns((pl.col("x") * 2).alias("x2"))',
            4,
        ),
        (
            big_ones,
            "# Keep the big ones.\ndef big_ones(orders: pl.LazyFrame) -> pl.LazyFrame:\n"
            '    output_df = orders.filter(pl.col("x") > 1.5)\n    return output_df.select("g", "x")',
            3,
        ),
    ],
)
def test_polars_code_stores_a_function_as_written(fn, code, height):
    out = _frame().polars_code(fn)
    assert _code(out) == code
    assert out.collect().height == height


def test_polars_code_stores_a_snippet_functions_body():
    """``_polars_code_<n>`` over the standard input names is how the notebook shows a snippet: its body is stored."""
    out = _frame().polars_code(_polars_code_3)
    assert _code(out) == (
        '# keep the big ones\noutput_df = input_df.filter(pl.col("x") > 1.5)\noutput_df = output_df.select("g", "x")'
    )
    assert out.collect().height == 3


def test_polars_code_refuses_a_function_that_returns_nothing_or_reads_outside_names():
    frame = _frame()
    with pytest.raises(NativeNodeError, match="`_free_form` returns nothing"):
        frame.polars_code(_free_form)

    def uses_frame(input_df):
        return input_df.join(frame, on="g")

    with pytest.raises(NativeNodeError, match="reads `frame` from outside the function"):
        frame.polars_code(uses_frame)

    def biggest(input_df):
        return input_df.head(max(1, 2))

    with pytest.raises(NativeNodeError, match="uses `max`, which Polars Code does not provide"):
        frame.polars_code(biggest)


def test_polars_code_honours_parameter_names_over_several_inputs():
    def stacked(top: pl.LazyFrame, bottom: pl.LazyFrame) -> pl.LazyFrame:
        return pl.concat([bottom, top])

    left, right = _frame(), ff.from_dict({"g": ["c", "d"], "x": [0.0, 1.0], "y": [9, 8]})
    assert left.polars_code(stacked, right).collect()["y"].to_list() == [9, 8, 1, 2, 3, 4]
    with pytest.raises(NativeNodeError, match="`stacked` takes 2 frames but the node has 1 input"):
        left.polars_code(stacked)


def test_polars_code_refuses_a_lone_decorated_def_given_as_text():
    with pytest.raises(NativeNodeError, match="`f` is decorated: Polars Code runs a plain `def`"):
        _frame().polars_code("@staticmethod\ndef f(rows):\n    return rows")


def test_polars_code_takes_more_inputs_in_order_and_a_description():
    left, right = _frame(), ff.from_dict({"g": ["c", "d"], "x": [0.0, 1.0], "y": [9, 8]})
    out = left.polars_code("pl.concat([input_df_1, input_df_2])", right, description="stack")
    node = core_node(out)
    assert node.node_type == "polars_code" and node.setting_input.description == "stack"
    assert [n.node_id for n in node.all_inputs] == [left.node_id, right.node_id]
    assert out.collect()["y"].to_list() == [1, 2, 3, 4, 9, 8]


def test_polars_code_writes_no_description_of_its_own():
    assert not core_node(_frame().polars_code("input_df.head(1)")).setting_input.description


def test_polars_code_round_trips(tmp_path):
    out = _frame().polars_code(_one_return)
    reopened, _ = round_trip(out, "polars_code.yaml")
    assert reopened.get_node(out.node_id).setting_input.polars_code_input.polars_code == _code(out)


def test_polars_code_refuses_a_lambda_bad_code_and_a_repeated_input():
    frame = _frame()
    with pytest.raises(NativeNodeError, match="a string or a def function"):
        frame.polars_code(lambda input_df: input_df)
    with pytest.raises(NativeNodeError, match="polars_code node"):
        frame.polars_code("output_df = (")
    assert [n.node_type for n in frame.flow_graph.nodes] == ["manual_input"]
    with pytest.raises(NativeNodeError, match="each input node once"):
        frame.polars_code("input_df_1", frame)


@pytest.mark.parametrize(
    "kwargs, settings",
    [
        ({}, ("index", 0, False, [])),
        ({"name": "rid", "offset": 1}, ("rid", 1, False, [])),
        ({"name": "n", "offset": 1, "group_by": ["g"]}, ("n", 1, True, ["g"])),
    ],
)
def test_with_row_index_is_always_one_record_id_node(kwargs, settings):
    frame = _frame()
    known = _ids(frame)
    out = frame.with_row_index(**kwargs)
    assert _new_nodes(known, out) == ["record_id"]
    record = core_node(out).setting_input.record_id_input
    assert (record.output_column_name, record.offset, record.group_by, record.group_by_columns) == settings


def test_with_row_index_matches_polars():
    assert_frame_equal(_frame().with_row_index().collect(), pl.DataFrame(DATA).with_row_index(), check_dtypes=False)
    grouped = _frame().with_row_index("n", 1, group_by=["g"]).collect()
    assert grouped.columns == ["n", "g", "x", "y"] and grouped["n"].to_list() == [1, 2, 1, 2]


def test_unpivot_with_a_list_places_the_native_node():
    frame = _frame()
    known = _ids(frame)
    out = frame.unpivot(["x", "y"], index="g")
    assert _new_nodes(known, out) == ["unpivot"]
    unpivot = core_node(out).setting_input.unpivot_input
    assert (unpivot.value_columns, unpivot.index_columns) == (["x", "y"], ["g"])
    expected = pl.DataFrame(DATA).unpivot(["x", "y"], index="g")
    assert_frame_equal(out.collect(), expected, check_row_order=False, check_dtypes=False)


def test_unpivot_with_a_selector_or_renamed_columns_keeps_polars_code():
    frame = _frame()
    assert core_node(frame.unpivot([ff.selectors.numeric()], index="g")).node_type == "polars_code"
    assert core_node(frame.unpivot(["x"], index="g", value_name="v")).node_type == "polars_code"


def test_std_and_var_aggregate_on_the_group_by_node():
    frame = _frame()
    known = _ids(frame)
    out = frame.group_by("g").agg(ff.col("x").std().alias("s"), ff.col("x").var().alias("v"))
    assert _new_nodes(known, out) == ["group_by"]
    aggs = [(a.old_name, a.agg, a.new_name) for a in core_node(out).setting_input.groupby_input.agg_cols]
    assert aggs == [("g", "groupby", "g"), ("x", "std", "s"), ("x", "var", "v")]
    expected = pl.DataFrame(DATA).group_by("g").agg(pl.col("x").std().alias("s"), pl.col("x").var().alias("v"))
    assert_frame_equal(out.collect(), expected, check_row_order=False)


def test_std_with_another_ddof_keeps_polars_code():
    out = _frame().group_by("g").agg(ff.col("x").std(ddof=0))
    assert core_node(out).node_type == "polars_code"
    expected = pl.DataFrame(DATA).group_by("g").agg(pl.col("x").std(ddof=0))
    assert_frame_equal(out.collect(), expected, check_row_order=False)


def test_fluent_calls_write_no_description_of_their_own():
    frame, other = _frame(), ff.from_dict({"g": ["a", "b"], "z": [1, 2]})
    placed = [
        frame,
        frame.sort("x"),
        frame.join(other, on="g"),
        frame.join(other, how="cross"),
        frame.group_by("g").agg(ff.col("x").sum()),
        frame.pivot(on="g", index="y", values="x", aggregate_function="sum"),
        frame.unpivot(["x"], index="g"),
        frame.with_row_index(),
        frame.unique(["g"]),
        frame.text_to_rows("g", delimiter=","),
        ff.concat([frame, frame.head(1)], how="diagonal_relaxed"),
        ff.sql("SELECT * FROM input_1", frame),
    ]
    described = {core_node(p).node_type: core_node(p).setting_input.description for p in placed}
    assert not any(described.values()), described
    assert core_node(frame.sort("x", description="mine")).setting_input.description == "mine"
