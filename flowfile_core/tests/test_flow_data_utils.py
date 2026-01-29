"""Tests for flowfile/flow_data_engine/utils module."""

import polars as pl

from flowfile_core.flowfile.flow_data_engine.utils import (
    define_pl_col_transformation,
    find_first_positions,
    get_data_type,
    match_order,
)


class TestGetDataType:
    """Test get_data_type function."""

    def test_all_ints(self):
        assert get_data_type([1, 2, 3]) == "int"

    def test_all_strings(self):
        assert get_data_type(["a", "b", "c"]) == "str"

    def test_all_floats(self):
        assert get_data_type([1.0, 2.0, 3.0]) == "float"

    def test_mixed_int_float(self):
        assert get_data_type([1, 2.0, 3]) == "float"

    def test_mixed_types(self):
        assert get_data_type([1, "a", 2.0]) == "str"


class TestDefinePlColTransformation:
    """Test define_pl_col_transformation function."""

    def test_datetime_type(self):
        expr = define_pl_col_transformation("col1", pl.Datetime)
        assert expr is not None

    def test_date_type(self):
        expr = define_pl_col_transformation("col1", pl.Date)
        assert expr is not None

    def test_int_type(self):
        expr = define_pl_col_transformation("col1", pl.Int64)
        assert expr is not None

    def test_string_type(self):
        expr = define_pl_col_transformation("col1", pl.String)
        assert expr is not None

    def test_float_type(self):
        expr = define_pl_col_transformation("col1", pl.Float64)
        assert expr is not None


class TestFindFirstPositions:
    """Test find_first_positions function."""

    def test_unique_elements(self):
        result = find_first_positions(["a", "b", "c"])
        assert result == {"a": 0, "b": 1, "c": 2}

    def test_duplicate_elements(self):
        result = find_first_positions(["a", "b", "a", "c"])
        assert result == {"a": 0, "b": 1, "c": 3}

    def test_empty_list(self):
        result = find_first_positions([])
        assert result == {}

    def test_single_element(self):
        result = find_first_positions(["x"])
        assert result == {"x": 0}

    def test_all_same(self):
        result = find_first_positions(["a", "a", "a"])
        assert result == {"a": 0}


class TestMatchOrder:
    """Test match_order function."""

    def test_basic_reorder(self):
        result = match_order(["c", "a", "b"], ["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_partial_match(self):
        result = match_order(["c", "d", "a"], ["a", "b", "c"])
        # a=0, c=2, d=inf
        assert result == ["a", "c", "d"]

    def test_same_order(self):
        result = match_order(["a", "b", "c"], ["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_empty_list(self):
        result = match_order([], ["a", "b"])
        assert result == []

    def test_no_ref_match(self):
        result = match_order(["x", "y"], ["a", "b"])
        # Both x and y have inf order, so they keep original relative order
        assert set(result) == {"x", "y"}
