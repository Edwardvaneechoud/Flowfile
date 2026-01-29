"""Tests for pivot_table module."""

from flowfile_core.flowfile.flow_data_engine.pivot_table import AggFunc, agg_funcs


class TestAggFunc:
    """Test AggFunc dataclass."""

    def test_create_agg_func(self):
        import polars as pl
        func = AggFunc(func_name="sum", func_expr=pl.col("value").sum())
        assert func.func_name == "sum"
        assert func.func_expr is not None

    def test_agg_func_slots(self):
        assert AggFunc.__slots__ == ["func_name", "func_expr"]


class TestAggFuncs:
    """Test agg_funcs list."""

    def test_agg_funcs_contains_expected(self):
        expected = ["sum", "max", "min", "count", "first", "last", "std", "var", "n_unique", "list", "list_agg"]
        assert agg_funcs == expected

    def test_agg_funcs_length(self):
        assert len(agg_funcs) == 11
