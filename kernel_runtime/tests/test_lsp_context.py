"""Unit tests for the cell<->Jedi position mapping (the one off-by-one-risk piece)."""

from kernel_runtime.lsp.context import from_jedi_position, to_jedi_position


def test_identity_for_valid_position():
    code = "import polars as pl\ndf = pl.LazyFrame({})\ndf."
    # line 3, col 3 (after "df.") is valid -> unchanged
    assert to_jedi_position(code, 3, 3) == (3, 3)


def test_clamps_line_past_end():
    code = "ab\ncd"
    assert to_jedi_position(code, 99, 1) == (2, 1)


def test_clamps_column_past_line_length():
    code = "ab\ncd"
    assert to_jedi_position(code, 1, 99) == (1, 2)  # line "ab" has length 2


def test_empty_code():
    assert to_jedi_position("", 1, 5) == (1, 0)
    assert to_jedi_position("", 5, 5) == (1, 0)


def test_zero_and_negative_clamped_to_first_line():
    code = "abc"
    assert to_jedi_position(code, 0, -3) == (1, 0)


def test_from_jedi_is_identity_v1():
    assert from_jedi_position(3, 7) == (3, 7)
