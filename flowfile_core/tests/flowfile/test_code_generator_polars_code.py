"""Polars-code node export: runtime-parity return detection and a token-level ``pl`` -> ``ff`` rename."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.flowfile.code_generator.code_generator import (
    _polars_code_function_body,
    _polars_code_to_flowframe,
    export_flow_to_flowframe,
    export_flow_to_polars,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, transform_schema
from tests.flowfile.test_project_exporter import create_basic_flow
from tests.flowfile_core_test_utils import exec_script

_EXPORTS = [
    pytest.param(export_flow_to_polars, id="polars"),
    pytest.param(export_flow_to_flowframe, id="flowframe"),
]


pytestmark = pytest.mark.usefixtures("keep_single_file_env")


def _run_export(code: str) -> pl.DataFrame:
    return exec_script(code)["run_etl_pipeline"]().collect()


def _polars_code_flow(polars_code: str) -> FlowGraph:
    flow = create_basic_flow(name="polars_code_export")
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="a", data_type="Integer")], data=[[1, 2, 3]]
            ),
        )
    )
    flow.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=2,
            depending_on_ids=[1],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=polars_code),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return flow


def _concat_sources():
    import flowfile_frame as ff

    left = ff.from_dict({"a": [1, 2], "b": ["x", "y"]})
    right = ff.from_dict({"a": [3, 4], "b": ["z", "w"]})
    return ff, left, right


@pytest.mark.parametrize("export", _EXPORTS)
@pytest.mark.parametrize(
    "build, how",
    [
        pytest.param(lambda ff, a, b: ff.concat([a, b]), "vertical", id="module_concat_default"),
        pytest.param(lambda ff, a, b: a.concat(b, how="vertical"), "vertical", id="method_vertical"),
        pytest.param(lambda ff, a, b: a.concat(b, how="diagonal"), "diagonal", id="method_diagonal"),
    ],
)
def test_flowframe_concat_exports_run(export, build, how):
    ff, left, right = _concat_sources()
    combined = build(ff, left, right)

    result = _run_export(export(combined.flow_graph))

    expected = pl.concat([left.collect(), right.collect()], how=how)
    assert_frame_equal(result, expected, check_row_order=False)


@pytest.mark.parametrize("export", _EXPORTS)
def test_pl_text_inside_strings_and_names_survives_flowframe_rename(export):
    code = 'df_pl = input_df\noutput_df = df_pl.with_columns(pl.lit("pl.col").alias("note"))  # pl.lit stays'
    result = _run_export(export(_polars_code_flow(code)))

    assert result["note"].to_list() == ["pl.col"] * 3


@pytest.mark.parametrize("export", _EXPORTS)
def test_indented_body_with_explicit_return_exports(export):
    code = (
        "        output_df = input_df.with_columns(\n"
        "            (pl.col('a') * 2).alias('doubled'),\n"
        "        )\n"
        "\n"
        "        return output_df\n"
    )
    result = _run_export(export(_polars_code_flow(code)))

    assert result["doubled"].to_list() == [2, 4, 6]


@pytest.mark.parametrize("export", _EXPORTS)
@pytest.mark.parametrize(
    "code",
    [
        pytest.param(
            "# Double it.\ndef doubled(rows: pl.LazyFrame) -> pl.LazyFrame:\n\n"
            "    return rows.with_columns((pl.col('a') * 2).alias('doubled'))",
            id="annotated",
        ),
        pytest.param(
            "def doubled(rows: ff.FlowFrame):\n    def twice(c):\n        return c * 2\n"
            "    return rows.with_columns(twice(pl.col('a')).alias('doubled'))",
            id="flowframe_hint_and_nested_helper",
        ),
    ],
)
def test_function_form_exports_as_written_and_runs(export, code):
    flow = _polars_code_flow(code)
    exported = export(flow)

    assert code.split("\n", 1)[1] in exported.replace("\n    ", "\n")
    assert "_polars_code_2" not in exported
    assert _run_export(exported)["doubled"].to_list() == [2, 4, 6]
    flow.run_graph()
    assert flow.get_node(2).get_resulting_data().data_frame.collect()["doubled"].to_list() == [2, 4, 6]


@pytest.mark.parametrize("export", _EXPORTS)
@pytest.mark.parametrize(
    "code",
    [
        pytest.param("def kept(rows: LazyFrame) -> LazyFrame:\n    return rows", id="signature"),
        pytest.param("def kept(rows):\n    def same(c: Expr) -> Expr:\n        return c\n    return rows", id="nested_def"),
    ],
)
def test_annotations_without_an_import_put_the_future_import_first(export, code):
    exported = export(_polars_code_flow(code))

    assert exported.startswith("from __future__ import annotations\n")
    assert len(_run_export(exported)) == 3


def test_snippet_renders_its_inputs_as_lazyframes_in_the_flowframe_export():
    exported = export_flow_to_flowframe(_polars_code_flow("input_df.head(1)"))

    assert "def _polars_code_2(input_df: pl.LazyFrame):" in exported
    assert "import polars as pl" in exported
    assert len(_run_export(exported)) == 1


@pytest.mark.parametrize(
    "code, expected",
    [
        ("input_df", ([], "input_df")),
        ("output_df = input_df", (["output_df = input_df"], "output_df")),
        ("output_df = input_df\nreturn output_df", (["output_df = input_df", "return output_df"], None)),
        ("# keep\ninput_df.head(1)", (["# keep"], "input_df.head(1)")),
        (
            "out = input_df.head(\n    1,\n    parallel=True\n)",
            (["out = input_df.head(", "    1,", "    parallel=True", ")"], "out"),
        ),
        ("output_df = x\nresult = output_df.head()", (["output_df = x", "result = output_df.head()"], "output_df")),
        ("output_df = (", (["output_df = ("], "output_df")),
    ],
    ids=[
        "expression", "assign", "explicit_return", "leading_comment", "multi_line_call", "output_df_wins", "unparsable",
    ],
)
def test_polars_code_function_body(code, expected):
    assert _polars_code_function_body(code) == expected


@pytest.mark.parametrize(
    "code, expected",
    [
        ("pl.col('a')", "ff.col('a')"),
        ('pl.lit("pl.col")', 'ff.lit("pl.col")'),
        ("df_pl.select(x.pl.y)", "df_pl.select(x.pl.y)"),
        ("x: pl.LazyFrame = pl.DataFrame({})", "x: ff.FlowFrame = ff.FlowFrame({})"),
        ("frame.LazyFrame", "frame.LazyFrame"),
        ("a = 1  # pl.col", "a = 1  # pl.col"),
    ],
    ids=["module_ref", "string_literal", "other_names", "frame_classes", "unrelated_attr", "comment"],
)
def test_polars_code_to_flowframe(code, expected):
    assert _polars_code_to_flowframe(code) == expected
