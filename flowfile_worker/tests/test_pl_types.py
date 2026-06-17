"""Tests for create/pl_types module."""

import polars as pl

from flowfile_worker.create.pl_types import (
    dtype_to_pl,
    dtype_to_pl_str,
    type_to_polars,
    type_to_polars_str,
)


class TestDtypeToPl:
    """Test dtype_to_pl mapping."""

    def test_int(self):
        assert dtype_to_pl["int"] == pl.Int64

    def test_integer(self):
        assert dtype_to_pl["integer"] == pl.Int64

    def test_char(self):
        assert dtype_to_pl["char"] == pl.String

    def test_fixed_decimal(self):
        assert dtype_to_pl["fixed decimal"] == pl.Float32

    def test_double(self):
        assert dtype_to_pl["double"] == pl.Float64

    def test_float(self):
        assert dtype_to_pl["float"] == pl.Float64

    def test_bool(self):
        assert dtype_to_pl["bool"] == pl.Boolean

    def test_byte(self):
        assert dtype_to_pl["byte"] == pl.UInt8

    def test_bit(self):
        assert dtype_to_pl["bit"] == pl.Binary

    def test_date(self):
        assert dtype_to_pl["date"] == pl.Date

    def test_datetime(self):
        assert dtype_to_pl["datetime"] == pl.Datetime

    def test_string(self):
        assert dtype_to_pl["string"] == pl.String

    def test_str(self):
        assert dtype_to_pl["str"] == pl.String

    def test_time(self):
        assert dtype_to_pl["time"] == pl.Time


class TestDtypeToPlStr:
    """Test dtype_to_pl_str mapping."""

    def test_int_str(self):
        assert dtype_to_pl_str["int"] == "Int64"

    def test_string_str(self):
        assert dtype_to_pl_str["string"] == "String"

    def test_float_str(self):
        assert dtype_to_pl_str["float"] == "Float64"

    def test_bool_str(self):
        assert dtype_to_pl_str["bool"] == "Boolean"


class TestTypeToPolars:
    """Test type_to_polars function."""

    def test_known_type(self):
        assert type_to_polars("int") == pl.Int64

    def test_known_type_uppercase(self):
        assert type_to_polars("INT") == pl.Int64

    def test_known_type_mixed_case(self):
        assert type_to_polars("String") == pl.String

    def test_polars_attr_fallback(self):
        """Test that it falls back to polars attributes."""
        result = type_to_polars("Int32")
        assert result == pl.Int32

    def test_unknown_type_returns_string(self):
        result = type_to_polars("unknown_type")
        assert result == pl.String

    def test_float_type(self):
        assert type_to_polars("float") == pl.Float64

    def test_date_type(self):
        assert type_to_polars("date") == pl.Date


class TestTypeToPolarsStr:
    """Test type_to_polars_str function."""

    def test_returns_instance(self):
        result = type_to_polars_str("int")
        assert isinstance(result, pl.DataType)

    def test_string_type(self):
        result = type_to_polars_str("string")
        assert isinstance(result, pl.String)

    def test_int_type(self):
        result = type_to_polars_str("int")
        assert isinstance(result, pl.Int64)

    def test_bool_type(self):
        result = type_to_polars_str("bool")
        assert isinstance(result, pl.Boolean)
