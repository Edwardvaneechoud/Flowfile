"""Tests for flowfile/flow_data_engine/flow_file_column/utils module."""

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import (
    dtype_to_pl,
    dtype_to_pl_str,
    get_polars_type,
    safe_eval_pl_type,
    cast_str_to_polars_type,
)


class TestDtypeToPl:
    """Test dtype_to_pl mapping."""

    def test_int(self):
        assert dtype_to_pl["int"] == pl.Int64

    def test_integer(self):
        assert dtype_to_pl["integer"] == pl.Int64

    def test_string(self):
        assert dtype_to_pl["string"] == pl.String

    def test_float(self):
        assert dtype_to_pl["float"] == pl.Float64

    def test_bool(self):
        assert dtype_to_pl["bool"] == pl.Boolean

    def test_date(self):
        assert dtype_to_pl["date"] == pl.Date

    def test_datetime(self):
        assert dtype_to_pl["datetime"] == pl.Datetime


class TestDtypeToPlStr:
    """Test dtype_to_pl_str mapping."""

    def test_returns_names(self):
        assert dtype_to_pl_str["int"] == "Int64"
        assert dtype_to_pl_str["string"] == "String"
        assert dtype_to_pl_str["float"] == "Float64"


class TestSafeEvalPlType:
    """Test safe_eval_pl_type function."""

    def test_simple_type(self):
        result = safe_eval_pl_type("Int64")
        assert result == pl.Int64

    def test_pl_prefix(self):
        result = safe_eval_pl_type("pl.Int64")
        assert result == pl.Int64

    def test_list_type(self):
        result = safe_eval_pl_type("List(Int64)")
        assert result == pl.List(pl.Int64)

    def test_string_type(self):
        result = safe_eval_pl_type("String")
        assert result == pl.String

    def test_boolean_type(self):
        result = safe_eval_pl_type("Boolean")
        assert result == pl.Boolean

    def test_invalid_type_raises(self):
        import pytest
        with pytest.raises(ValueError, match="Failed to safely evaluate"):
            safe_eval_pl_type("os.system('bad')")

    def test_float32(self):
        result = safe_eval_pl_type("Float32")
        assert result == pl.Float32

    def test_date(self):
        result = safe_eval_pl_type("Date")
        assert result == pl.Date


class TestGetPolarsType:
    """Test get_polars_type function."""

    def test_known_type_lowercase(self):
        result = get_polars_type("int")
        assert result == pl.Int64

    def test_known_type_mixed_case(self):
        result = get_polars_type("STRING")
        assert result == pl.String

    def test_pl_prefix(self):
        result = get_polars_type("pl.Int64")
        assert result == pl.Int64

    def test_unknown_fallback_to_string(self):
        result = get_polars_type("unknown_type_xyz")
        assert result == pl.String

    def test_direct_polars_name(self):
        result = get_polars_type("Float64")
        assert result == pl.Float64


class TestCastStrToPolarsType:
    """Test cast_str_to_polars_type function."""

    def test_returns_instance(self):
        result = cast_str_to_polars_type("int")
        assert isinstance(result, pl.DataType)

    def test_string_type(self):
        result = cast_str_to_polars_type("string")
        assert isinstance(result, pl.String)

    def test_int_type(self):
        result = cast_str_to_polars_type("int")
        assert isinstance(result, pl.Int64)
