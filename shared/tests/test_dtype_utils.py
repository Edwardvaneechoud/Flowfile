import polars as pl

from shared.dtype_utils import create_pl_df_type_save, make_column_constructible, standardize_col_dtype


def test_standardize_still_leaves_numeric_columns_mixed():
    """The documented contract RawData._infer_data_type relies on — do not tighten this here."""
    assert standardize_col_dtype([1, None, 2.5]) == [1, None, 2.5]


def test_whole_and_fractional_numbers_widen_to_float():
    """Polars types a column from its leading values, so ints ahead of a float used to reject it."""
    column = [141000, 87500, None, 162500.25, 69500]

    df = create_pl_df_type_save([column], orient="col")

    assert df.dtypes == [pl.Float64]
    assert 162500.25 in df.to_series().to_list()


def test_numbers_mixed_with_an_excel_error_value_become_text():
    """openpyxl hands back 45.0 as int, so unit_cost is int+float+str: not a numeric column at all."""
    column = [12.5, "#N/A", 3.99, 0.0001, None, 45]

    df = create_pl_df_type_save([column], orient="col")

    assert df.dtypes == [pl.String]
    assert "#N/A" in df.to_series().to_list()


def test_single_type_columns_are_untouched():
    assert make_column_constructible([1, 2, None, 3]) == [1, 2, None, 3]
    assert make_column_constructible(["a", None, "b"]) == ["a", None, "b"]
    assert make_column_constructible([None, None]) == [None, None]


def test_row_oriented_input_still_transposes():
    df = create_pl_df_type_save([[1, "a"], [2.5, "b"]])

    assert df.dtypes == [pl.Float64, pl.String]
    assert df.rows() == [(1.0, "a"), (2.5, "b")]
