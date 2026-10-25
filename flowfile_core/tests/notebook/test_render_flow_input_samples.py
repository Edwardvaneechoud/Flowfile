"""A Flow Input sample with no plain literal form is a placeholder in the notebook and code in the FlowFrame export."""

import datetime

import polars as pl
import pytest

import flowfile as ff
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe
from flowfile_core.notebook import render


@pytest.mark.parametrize(
    "values", [[datetime.date(2020, 1, 2)], [1.0, float("nan")], [datetime.time(1, 2)]], ids=["date", "nan", "time"]
)
def test_a_sample_without_a_plain_literal_is_a_notebook_placeholder(values):
    graph = ff.FlowInput("x", sample=pl.DataFrame({"a": values})).flow_graph

    cells = [cell.code for cell in render.render(graph).cells]
    node_cell = next(code for code in cells if "flow_input" in code)

    assert "ff.canvas_node(" in node_cell
    assert "no literal form" in node_cell
    assert "pl.DataFrame(" in export_flow_to_flowframe(graph)


def test_a_plain_sample_still_renders_as_a_flow_input():
    graph = ff.FlowInput("x", sample=pl.DataFrame({"a": [1, 2]})).flow_graph

    cells = [cell.code for cell in render.render(graph).cells]

    assert any("ff.FlowInput(" in code and "sample=pl.DataFrame(" in code for code in cells)
