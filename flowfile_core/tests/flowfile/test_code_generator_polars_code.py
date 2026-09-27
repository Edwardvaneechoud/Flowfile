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
from flowfile_core.schemas import input_schema, schemas, transform_schema

_EXPORTS = [
    pytest.param(export_flow_to_polars, id="polars"),
    pytest.param(export_flow_to_flowframe, id="flowframe"),
]


@pytest.fixture(autouse=True)
def _keep_single_file_env(monkeypatch):
    """Exec'ing a FlowFrame export imports ``flowfile``, which rewrites these for the process."""
    import os

    for key in ("FLOWFILE_SINGLE_FILE_MODE", "FLOWFILE_WORKER_PORT"):
        if key in os.environ:
            monkeypatch.setenv(key, os.environ[key])
        else:
            monkeypatch.delenv(key, raising=False)


def _run_export(code: str) -> pl.DataFrame:
    compile(code, "<export>", "exec")
    namespace: dict = {}
    exec(code, namespace)
    return namespace["run_etl_pipeline"]().collect()


def _polars_code_flow(polars_code: str) -> FlowGraph:
    settings = schemas.FlowSettings(
        flow_id=1, execution_mode="Performance", execution_location="local", path="/tmp/test_flow"
    )
    flow = FlowGraph(flow_settings=settings, name="polars_code_export")
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
