"""Each Polars Code executable runs in globals of its own, so no task changes a name for the tasks after it."""

import polars as pl
import pytest

from shared.polars_code_parser import polars_code_parser

POISON_SNIPPET = """
global col
def col(name):
    return lit("POISONED").alias(name)
output_df = input_df.select(col("a"))
"""

POISON_FUNCTION = """
def transform(input_df):
    global col
    col = lambda name: lit("POISONED").alias(name)
    return input_df.select(col("a"))
"""


@pytest.mark.parametrize("code", [POISON_SNIPPET, POISON_FUNCTION], ids=["snippet", "function form"])
def test_rebinding_a_global_reaches_no_later_task(code):
    frame = pl.LazyFrame({"a": [1]})
    assert polars_code_parser.get_executable(code, 1)(frame).collect()["a"].to_list() == ["POISONED"]

    later = polars_code_parser.get_executable('input_df.select(col("a"))', 1)

    assert later(frame).collect()["a"].to_list() == [1]
    assert polars_code_parser.safe_globals["col"] is pl.col
