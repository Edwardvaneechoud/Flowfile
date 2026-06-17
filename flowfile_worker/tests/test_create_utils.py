"""Tests for create/utils module."""

import polars as pl

from flowfile_worker.create.utils import (
    convert_to_string,
    create_pl_df_type_save,
    standardize_col_dtype,
)


class TestConvertToString:
    """Test convert_to_string function."""

    def test_int(self):
        assert convert_to_string(42) == "42"

    def test_float(self):
        assert convert_to_string(3.14) == "3.14"

    def test_string(self):
        assert convert_to_string("hello") == "hello"

    def test_none(self):
        assert convert_to_string(None) == "None"

    def test_bool(self):
        assert convert_to_string(True) == "True"

    def test_list(self):
        assert convert_to_string([1, 2, 3]) == "[1, 2, 3]"


class TestStandardizeColDtype:
    """Test standardize_col_dtype function."""

    def test_single_type_int(self):
        vals = [1, 2, 3]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_single_type_str(self):
        vals = ["a", "b", "c"]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_int_and_float_preserved(self):
        vals = [1, 2.0, 3]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_mixed_int_float_str_preserved(self):
        # int+float+str: since int and float are both present, returns unchanged
        vals = [1, "a", 3.0]
        result = standardize_col_dtype(vals)
        assert result == vals

    def test_mixed_str_bool_converted(self):
        # str+bool without int/float -> converts to string
        vals = ["a", True]
        result = standardize_col_dtype(vals)
        assert all(isinstance(v, str) or v is None for v in result)


class TestCreatePlDfTypeSave:
    """Test create_pl_df_type_save function."""

    def test_row_orient(self):
        data = [[1, "a"], [2, "b"], [3, "c"]]
        df = create_pl_df_type_save(data, orient="row")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    def test_col_orient(self):
        data = [[1, 2, 3], ["a", "b", "c"]]
        df = create_pl_df_type_save(data, orient="col")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    def test_mixed_types_in_column(self):
        """Test that mixed types in a column are standardized."""
        data = [[1, "a", 3.0], [4, 5, 6]]
        df = create_pl_df_type_save(data, orient="row")
        assert isinstance(df, pl.DataFrame)

    def test_uniform_types(self):
        data = [[1, 2], [3, 4]]
        df = create_pl_df_type_save(data, orient="row")
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
