"""The per-type settings normaliser a push compares a rebuilt node with its canvas twin through."""

import pytest

import flowfile_frame as ff
from flowfile_core.notebook.compare import (
    normalise,
    param_comparison_filter,
    parameters_equal,
    script_cells,
    settings_equal,
)


def test_normalise_drops_record_fields_and_compares_formulas_through_the_translator():
    base = {"flow_id": 1, "node_id": 4, "pos_x": 1.0, "description": "x", "node_reference": "r", "user_id": 3}
    a = {**base, "functions": [{"field": {"name": "b", "data_type": "Auto"}, "function": "[a] * 2"}]}
    b = {"node_id": 4, "function": {"field": {"name": "b", "data_type": "Auto"}, "function": "([a] * 2)"}}
    assert settings_equal(a, b, "formula")
    c = {"node_id": 4, "functions": [{"field": {"name": "b", "data_type": "Auto"}, "function": "[a] * 3"}]}
    assert not settings_equal(a, c, "formula")
    assert "flow_id" not in normalise(a, "formula") and "description" not in normalise(a, "formula")


def test_a_basic_filter_equals_its_advanced_spelling():
    basic = {
        "filter_input": {"mode": "basic", "basic_filter": {"field": "q", "operator": "greater_than", "value": "7"}}
    }
    advanced = {"filter_input": {"mode": "advanced", "advanced_filter": "([q] > 7)", "basic_filter": None}}
    assert settings_equal(basic, advanced, "filter")
    other = {"filter_input": {"mode": "advanced", "advanced_filter": "[q] > 8"}}
    assert not settings_equal(basic, other, "filter")


def test_select_compares_the_ordered_projection():
    entry = {"old_name": "a", "new_name": "b", "keep": True, "data_type": "Int64", "data_type_change": False}
    a = {"keep_missing": False, "select_input": [{**entry, "position": 0, "original_position": 3, "is_altered": True}]}
    b = {"keep_missing": False, "select_input": [entry, {"old_name": "c", "new_name": "c", "keep": False}]}
    assert settings_equal(a, b, "select")
    assert not settings_equal({**a, "keep_missing": True}, {**b, "keep_missing": True}, "select")


def test_python_script_cells_compare_by_code_and_gate_by_its_active_source():
    script = {"python_script_input": {"code": "x", "cells": [{"id": "one", "code": "x"}]}}
    same = {"python_script_input": {"code": "x", "cells": [{"id": "two", "code": "x"}]}}
    assert settings_equal(script, same, "python_script")
    gate = {"gate_input": {"condition_source": "formula", "formula": "[a] > 1", "parameter": "p", "value": "v"}}
    fresh = {"gate_input": {"condition_source": "formula", "formula": "([a] > 1)", "parameter": "", "value": ""}}
    assert settings_equal(gate, fresh, "gate")


def _script(cells: list[str], code: str | None = None) -> dict:
    joined = "\n\n".join(cell for cell in cells if cell) if code is None else code
    return {"python_script_input": {"code": joined, "cells": [{"id": str(i), "code": c} for i, c in enumerate(cells)]}}


def test_blank_lines_around_a_script_cell_and_blank_cells_are_cosmetic():
    """The drawer keeps a cell's trailing newline and its empty cells; the frame's cells have neither."""
    drawer = _script(["\n  \nimport polars as pl\n\ndf = 1\n", "", " \n", "publish(df)\n\n"])
    frame = _script(["import polars as pl\n\ndf = 1", "publish(df)"])
    assert settings_equal(drawer, frame, "python_script")
    code = normalise(drawer, "python_script")["python_script_input"]["code"]
    assert code == "import polars as pl\n\ndf = 1\n\npublish(df)"

    assert not settings_equal(_script(["a = 1\n\nb = 2"]), _script(["a = 1\nb = 2"]), "python_script")
    assert not settings_equal(_script(["  a = 1"]), _script(["a = 1"]), "python_script"), "indentation is code"
    assert not settings_equal(_script(["a", "b"]), _script(["a\n\nb"]), "python_script"), "the split is the cells"


def test_a_scripts_code_follows_its_cells_only_while_it_is_their_join():
    drawer, frame = _script(["x = 1\n"]), _script(["x = 1"])
    assert drawer["python_script_input"]["code"] == "x = 1\n" and settings_equal(drawer, frame, "python_script")
    stale = _script(["x = 1\n"], code="x = 2")
    assert normalise(stale, "python_script")["python_script_input"]["code"] == "x = 2"
    assert not settings_equal(stale, frame, "python_script")


@ff.python_script
def noted_script():
    """A note."""
    rows = flowfile_ctx.read_input()

    # %% Publish

    flowfile_ctx.publish_output(rows)


def test_the_frames_cells_are_already_as_they_compare():
    expected = ["# A note.", "rows = flowfile_ctx.read_input()", "# Publish\nflowfile_ctx.publish_output(rows)"]
    assert script_cells(noted_script.cells) == noted_script.cells == expected
    assert script_cells(["\n", "a\n", ""]) == ["a"]


def test_output_directory_spelled_as_the_file_path_is_the_same_target():
    canvas = {"output_settings": {"name": "o.csv", "directory": "/tmp/x", "abs_file_path": "/tmp/x/o.csv"}}
    frame = {"output_settings": {"name": "o.csv", "directory": "/tmp/x/o.csv", "abs_file_path": "/tmp/x/o.csv"}}
    assert settings_equal(canvas, frame, "output")


def test_parameters_compare_as_models():
    model = ff.Parameter("n", default=3, type="integer")
    graph = ff.create_flow_graph()
    ff.add_flow_parameter(graph, model)
    stored = graph.flow_settings.parameters
    assert parameters_equal(stored, [p.model_dump(mode="json") for p in stored])
    assert not parameters_equal(stored, [])


def test_a_description_difference_never_makes_a_row_differ():
    sort = {"sort_input": [{"column": "a", "how": "asc"}]}
    canvas = {**sort, "description": "Sort by a", "description_is_auto_generated": True}
    assert settings_equal(canvas, sort, "sort")
    assert settings_equal({**sort, "description": "mine"}, {**sort, "description": ""}, "sort")


def test_a_parameter_comparison_equals_its_basic_filter():
    basic = {"filter_input": {"mode": "basic", "basic_filter": {"field": "x", "operator": ">", "value": "${p}"}}}
    advanced = {"filter_input": {"mode": "advanced", "advanced_filter": "([x] > ${p})"}}
    assert settings_equal(basic, advanced, "filter")
    between = param_comparison_filter("(([x] >= ${a}) and ([x] <= ${b}))")
    assert between["basic_filter"] == {"field": "x", "operator": "between", "value": "${a}", "value2": "${b}"}
    assert param_comparison_filter("([x] > ${p}) and ([y] < 2)") is None


@pytest.mark.parametrize(
    "snippet",
    [
        "a = input_df.filter(pl.col('x') > 1)\n\noutput_df = a.select('x')",
        "output_df = input_df\n# keep all rows for now",
        "# top 5\ninput_df.head(5)",
        "a = input_df.head()\na.select('x')",
        "input_df.head(5)",
    ],
    ids=["blank_line", "trailing_comment", "leading_comment", "expression_after_assignment", "one_liner"],
)
def test_a_snippet_pushed_back_unedited_from_its_notebook_def_is_unchanged(snippet):
    from flowfile_core.flowfile.code_generator.code_generator import snippet_function_lines
    from flowfile_frame.flow_frame import _polars_code_text

    pushed = _polars_code_text("\n".join(snippet_function_lines(snippet, "_polars_code_3", ["input_df"])))
    canvas = {"polars_code_input": {"polars_code": snippet}}
    assert settings_equal(canvas, {"polars_code_input": {"polars_code": pushed}}, "polars_code")
    edited = {"polars_code_input": {"polars_code": pushed + ".head(1)"}}
    assert not settings_equal(canvas, edited, "polars_code")


def test_a_function_compares_by_its_text():
    code = "# note\ndef f(rows):\n\n    return rows"
    canvas = {"polars_code_input": {"polars_code": code}}
    assert settings_equal(canvas, {"polars_code_input": {"polars_code": "  " + code + "\n"}}, "polars_code")
    assert not settings_equal(canvas, {"polars_code_input": {"polars_code": code.replace("# note\n", "")}}, "polars_code")


def test_a_closing_return_output_df_and_stale_group_columns_are_cosmetic():
    code = {"polars_code_input": {"polars_code": "output_df = input_df\nreturn output_df"}}
    assert settings_equal(code, {"polars_code_input": {"polars_code": "  output_df = input_df"}}, "polars_code")
    grouped = {"record_id_input": {"output_column_name": "n", "group_by": False, "group_by_columns": ["a"]}}
    plain = {"record_id_input": {"output_column_name": "n", "group_by": False, "group_by_columns": []}}
    assert settings_equal(grouped, plain, "record_id")
