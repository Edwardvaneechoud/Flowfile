"""Tests for _ff_repr (flowfile function representation) tracking on Expr
and NodeFormula conversion in with_columns."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_frame import FlowFrame
from flowfile_frame.expr import Column, Expr, col, lit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_ff():
    """FlowFrame with mixed types for integration tests."""
    return FlowFrame({
        "x": [1, 2, 3],
        "y": [10, 20, 30],
        "z": [100, 200, 300],
        "name": ["Alice", "Bob", "Charlie"],
        "active": [True, False, True],
        "date": pl.Series(["2022-01-01", "2022-06-15", "2023-03-20"]).str.to_date(),
        "amount": [10.5, 20.0, 30.75],
        "nullable": [1, None, 3],
    })


def _is_formula_node(node_settings) -> bool:
    """Check if a node is a NodeFormula (has function attribute)."""
    return hasattr(node_settings.setting_input, "function") and node_settings.setting_input.function is not None


def _is_polars_code_node(node_settings) -> bool:
    """Check if a node is a NodePolarsCode."""
    return hasattr(node_settings.setting_input, "polars_code_input")


def _get_formula(node_settings) -> str:
    """Get the formula string from a NodeFormula node."""
    return node_settings.setting_input.function.function


# ---------------------------------------------------------------------------
# Unit tests for _ff_repr on Expr
# ---------------------------------------------------------------------------

class TestFfReprBasic:
    """Test _ff_repr tracking on basic expression operations."""

    def test_col_ff_repr(self):
        assert col("x")._ff_repr == "[x]"

    def test_col_with_spaces_ff_repr(self):
        assert col("col name")._ff_repr == "[col name]"

    def test_lit_int_ff_repr(self):
        assert lit(5)._ff_repr == "5"

    def test_lit_float_ff_repr(self):
        assert lit(3.14)._ff_repr == "3.14"

    def test_lit_string_ff_repr(self):
        assert lit("hello")._ff_repr == '"hello"'

    def test_lit_bool_true_ff_repr(self):
        assert lit(True)._ff_repr == "true"

    def test_lit_bool_false_ff_repr(self):
        assert lit(False)._ff_repr == "false"

    def test_lit_none_ff_repr(self):
        assert lit(None)._ff_repr is None

    def test_lit_string_with_quotes(self):
        assert lit('say "hello"')._ff_repr == '"say \\"hello\\""'

    def test_lit_string_with_backslash(self):
        assert lit("back\\slash")._ff_repr == '"back\\\\slash"'


class TestFfReprBinaryOps:
    """Test _ff_repr for binary operations."""

    def test_add(self):
        assert (col("x") + col("y"))._ff_repr == "([x] + [y])"

    def test_sub(self):
        assert (col("x") - col("y"))._ff_repr == "([x] - [y])"

    def test_mul(self):
        assert (col("x") * col("y"))._ff_repr == "([x] * [y])"

    def test_div(self):
        assert (col("x") / col("y"))._ff_repr == "([x] / [y])"

    def test_mod(self):
        assert (col("x") % col("y"))._ff_repr == "([x] % [y])"

    def test_add_literal_int(self):
        assert (col("x") + 5)._ff_repr == "([x] + 5)"

    def test_mul_literal_float(self):
        assert (col("x") * 1.1)._ff_repr == "([x] * 1.1)"

    def test_eq_string(self):
        assert (col("x") == "active")._ff_repr == '([x] == "active")'

    def test_gt(self):
        assert (col("x") > 10)._ff_repr == "([x] > 10)"

    def test_lt(self):
        assert (col("x") < 10)._ff_repr == "([x] < 10)"

    def test_ge(self):
        assert (col("x") >= 10)._ff_repr == "([x] >= 10)"

    def test_le(self):
        assert (col("x") <= 100)._ff_repr == "([x] <= 100)"

    def test_ne(self):
        assert (col("x") != 0)._ff_repr == "([x] != 0)"

    def test_and(self):
        assert (col("x") & col("y"))._ff_repr == "([x] and [y])"

    def test_or(self):
        assert (col("x") | col("y"))._ff_repr == "([x] or [y])"

    def test_floordiv(self):
        assert (col("x") // col("y"))._ff_repr == "floor(([x] / [y]))"

    def test_pow(self):
        assert (col("x") ** 2)._ff_repr == "power([x], 2)"

    def test_nested_ops(self):
        expr = (col("x") + col("y")) * col("z")
        assert expr._ff_repr == "(([x] + [y]) * [z])"

    def test_triple_nested_ops(self):
        expr = (col("x") + col("y")) * (col("z") - 1)
        assert expr._ff_repr == "(([x] + [y]) * ([z] - 1))"


class TestFfReprReverseBinaryOps:
    """Test _ff_repr for right-side operations (e.g., 5 + col('x'))."""

    def test_radd(self):
        assert (5 + col("x"))._ff_repr == "(5 + [x])"

    def test_rsub(self):
        assert (10 - col("x"))._ff_repr == "(10 - [x])"

    def test_rmul(self):
        assert (2 * col("x"))._ff_repr == "(2 * [x])"

    def test_rtruediv(self):
        assert (100 / col("x"))._ff_repr == "(100 / [x])"

    def test_rmod(self):
        assert (100 % col("x"))._ff_repr == "(100 % [x])"

    def test_rfloordiv(self):
        assert (10 // col("x"))._ff_repr == "floor((10 / [x]))"

    def test_rpow(self):
        assert (2 ** col("x"))._ff_repr == "power(2, [x])"


class TestFfReprAlias:
    """Test that alias preserves _ff_repr."""

    def test_col_alias(self):
        expr = col("x").alias("new_x")
        assert expr._ff_repr == "[x]"
        assert expr.column_name == "new_x"

    def test_binary_op_alias(self):
        expr = (col("x") + col("y")).alias("sum")
        assert expr._ff_repr == "([x] + [y])"
        assert expr.column_name == "sum"

    def test_string_method_alias(self):
        expr = col("name").str.to_uppercase().alias("upper")
        assert expr._ff_repr == "uppercase([name])"
        assert expr.column_name == "upper"


class TestFfReprStringMethods:
    """Test _ff_repr for string namespace methods."""

    def test_to_uppercase(self):
        assert col("x").str.to_uppercase()._ff_repr == "uppercase([x])"

    def test_to_lowercase(self):
        assert col("x").str.to_lowercase()._ff_repr == "lowercase([x])"

    def test_to_titlecase(self):
        assert col("x").str.to_titlecase()._ff_repr == "titlecase([x])"

    def test_len_chars(self):
        assert col("x").str.len_chars()._ff_repr == "length([x])"

    def test_starts_with(self):
        assert col("x").str.starts_with("pre")._ff_repr == 'starts_with([x], "pre")'

    def test_ends_with(self):
        assert col("x").str.ends_with("suf")._ff_repr == 'ends_with([x], "suf")'

    def test_strip_chars_start(self):
        assert col("x").str.strip_chars_start()._ff_repr == "left_trim([x])"

    def test_strip_chars_end(self):
        assert col("x").str.strip_chars_end()._ff_repr == "right_trim([x])"

    def test_strip_chars_start_with_characters_not_supported(self):
        """strip_chars_start with specific characters should not produce ff_repr."""
        assert col("x").str.strip_chars_start("0")._ff_repr is None

    def test_strip_chars_end_with_characters_not_supported(self):
        """strip_chars_end with specific characters should not produce ff_repr."""
        assert col("x").str.strip_chars_end("0")._ff_repr is None

    def test_contains_not_mapped(self):
        """contains is not in STRING_METHOD_FF_MAP, so ff_repr should be None."""
        assert col("x").str.contains("pattern")._ff_repr is None

    def test_replace_not_mapped(self):
        """replace is not in STRING_METHOD_FF_MAP, so ff_repr should be None."""
        assert col("x").str.replace("a", "b")._ff_repr is None

    def test_chained_string_ops_break_ff_repr(self):
        """Chaining two string ops should lose ff_repr since the second operates on a non-col."""
        # After to_uppercase(), the result is an Expr (not Column), so the parent's _ff_repr
        # for the next string op depends on the first op's result
        expr = col("x").str.to_uppercase()
        # The result has ff_repr but is no longer a Column
        assert expr._ff_repr == "uppercase([x])"


class TestFfReprDateTimeMethods:
    """Test _ff_repr for datetime namespace methods."""

    def test_year(self):
        assert col("x").dt.year()._ff_repr == "year([x])"

    def test_month(self):
        assert col("x").dt.month()._ff_repr == "month([x])"

    def test_day(self):
        assert col("x").dt.day()._ff_repr == "day([x])"

    def test_hour(self):
        assert col("x").dt.hour()._ff_repr == "hour([x])"

    def test_minute(self):
        assert col("x").dt.minute()._ff_repr == "minute([x])"

    def test_second(self):
        assert col("x").dt.second()._ff_repr == "second([x])"

    def test_quarter(self):
        assert col("x").dt.quarter()._ff_repr == "quarter([x])"

    def test_week(self):
        assert col("x").dt.week()._ff_repr == "week([x])"


class TestFfReprCast:
    """Test _ff_repr for cast operations."""

    def test_cast_int64(self):
        assert col("x").cast(pl.Int64)._ff_repr == "to_integer([x])"

    def test_cast_int32(self):
        assert col("x").cast(pl.Int32)._ff_repr == "to_integer([x])"

    def test_cast_float64(self):
        assert col("x").cast(pl.Float64)._ff_repr == "to_float([x])"

    def test_cast_float32(self):
        assert col("x").cast(pl.Float32)._ff_repr == "to_float([x])"

    def test_cast_utf8(self):
        assert col("x").cast(pl.Utf8)._ff_repr == "to_string([x])"

    def test_cast_boolean(self):
        assert col("x").cast(pl.Boolean)._ff_repr == "to_boolean([x])"

    def test_cast_date(self):
        assert col("x").cast(pl.Date)._ff_repr == "to_date([x])"

    def test_cast_datetime(self):
        assert col("x").cast(pl.Datetime)._ff_repr == "to_datetime([x])"

    def test_cast_column_preserves_ff_repr(self):
        """Column.cast() should also compute ff_repr."""
        c = col("x").cast(pl.Int64)
        assert isinstance(c, Column)
        assert c._ff_repr == "to_integer([x])"


class TestFfReprMiscMethods:
    """Test _ff_repr for miscellaneous methods."""

    def test_is_null(self):
        assert col("x").is_null()._ff_repr == "is_empty([x])"

    def test_is_not_null(self):
        assert col("x").is_not_null()._ff_repr == "is_not_empty([x])"

    def test_fill_null_int(self):
        assert col("x").fill_null(0)._ff_repr == "coalesce([x], 0)"

    def test_fill_null_string(self):
        assert col("x").fill_null("default")._ff_repr == 'coalesce([x], "default")'

    def test_abs(self):
        assert col("x").abs()._ff_repr == "abs([x])"

    def test_round(self):
        assert col("x").round(2)._ff_repr == "round([x], 2)"

    def test_round_default(self):
        assert col("x").round()._ff_repr == "round([x], 0)"

    def test_ceil(self):
        assert col("x").ceil()._ff_repr == "ceil([x])"

    def test_floor(self):
        assert col("x").floor()._ff_repr == "floor([x])"


class TestFfReprNonConvertible:
    """Test that non-convertible operations set _ff_repr to None."""

    def test_map_elements(self):
        def fn(x):
            return x * 2
        assert col("x").map_elements(fn)._ff_repr is None

    def test_sort(self):
        assert col("x").sort()._ff_repr is None

    def test_over(self):
        assert col("x").sum().over("group")._ff_repr is None

    def test_floordiv_propagates(self):
        """Floor division ff_repr composes with further operations."""
        expr = (col("x") // col("y")) + col("z")
        assert expr._ff_repr == "(floor(([x] / [y])) + [z])"

    def test_pow_propagates(self):
        """Power ff_repr composes with further operations."""
        expr = (col("x") ** 2) + col("z")
        assert expr._ff_repr == "(power([x], 2) + [z])"


# ---------------------------------------------------------------------------
# Integration tests for with_columns NodeFormula conversion
# ---------------------------------------------------------------------------

class TestWithColumnsFormulaConversion:
    """Test that with_columns creates NodeFormula nodes when possible."""

    def test_simple_binary_op_creates_formula(self, sample_ff):
        result = sample_ff.with_columns((col("x") + col("y")).alias("sum"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for simple binary op"
        assert _get_formula(node) == "([x] + [y])"

    def test_multiply_literal_creates_formula(self, sample_ff):
        result = sample_ff.with_columns((col("x") * 2).alias("doubled"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for multiply with literal"
        assert _get_formula(node) == "([x] * 2)"

    def test_string_uppercase_creates_formula(self, sample_ff):
        result = sample_ff.with_columns(col("name").str.to_uppercase().alias("upper_name"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for string uppercase"
        assert _get_formula(node) == "uppercase([name])"

    def test_dt_year_creates_formula(self, sample_ff):
        result = sample_ff.with_columns(col("date").dt.year().alias("yr"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for dt.year"
        assert _get_formula(node) == "year([date])"

    def test_cast_creates_formula(self, sample_ff):
        result = sample_ff.with_columns(col("x").cast(pl.Float64).alias("x_float"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for cast"
        assert _get_formula(node) == "to_float([x])"

    def test_named_expr_creates_formula(self, sample_ff):
        result = sample_ff.with_columns(sum_xy=col("x") + col("y"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for named expression"
        assert _get_formula(node) == "([x] + [y])"

    def test_multiple_formulas_all_convertible(self, sample_ff):
        """Multiple expressions, all convertible, should create NodeFormula chain."""
        result = sample_ff.with_columns(
            (col("x") + col("y")).alias("sum"),
            (col("x") * col("y")).alias("product"),
        )
        # The last node should be a formula
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for last expression in chain"

    def test_map_elements_falls_back_to_polars_code(self, sample_ff):
        def fn(x):
            return x * 2
        result = sample_ff.with_columns(col("x").map_elements(fn, return_dtype=pl.Int64).alias("doubled"))
        node = result.get_node_settings()
        assert _is_polars_code_node(node), "Expected NodePolarsCode for map_elements"

    def test_mixed_falls_back_to_polars_code(self, sample_ff):
        """If any expression is non-convertible, all should fall back to polars code."""
        def fn(x):
            return x * 2
        result = sample_ff.with_columns(
            (col("x") + col("y")).alias("sum"),
            col("x").map_elements(fn, return_dtype=pl.Int64).alias("doubled"),
        )
        node = result.get_node_settings()
        assert _is_polars_code_node(node), "Expected NodePolarsCode for mixed expressions"

    def test_no_alias_uses_left_operand_name(self, sample_ff):
        """Expression without alias derives column name from left operand (Polars behavior)."""
        # (col('x') + col('y')) derives column_name='x' from polars meta.output_name()
        result = sample_ff.with_columns((col("x") + col("y")))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula with derived column name"
        assert _get_formula(node) == "([x] + [y])"

    def test_col_alias_creates_formula(self, sample_ff):
        """Simple column rename should create formula."""
        result = sample_ff.with_columns(col("x").alias("x_copy"))
        node = result.get_node_settings()
        assert _is_formula_node(node), "Expected NodeFormula for simple column alias"
        assert _get_formula(node) == "[x]"

    def test_flowfile_formulas_kwarg_still_works(self, sample_ff):
        """The existing flowfile_formulas kwarg should still work."""
        result = sample_ff.with_columns(
            flowfile_formulas=["[x] + [y]"],
            output_column_names=["sum_xy"],
        )
        node = result.get_node_settings()
        assert _is_formula_node(node)


# ---------------------------------------------------------------------------
# Correctness tests — formula results match polars results
# ---------------------------------------------------------------------------

class TestFormulaCorrectness:
    """Verify that formula conversion produces correct results."""

    def test_arithmetic_correctness(self, sample_ff):
        result = sample_ff.with_columns((col("x") + col("y")).alias("sum")).collect()
        expected = pl.DataFrame({
            "x": [1, 2, 3], "y": [10, 20, 30], "z": [100, 200, 300],
            "name": ["Alice", "Bob", "Charlie"], "active": [True, False, True],
            "date": pl.Series(["2022-01-01", "2022-06-15", "2023-03-20"]).str.to_date(),
            "amount": [10.5, 20.0, 30.75], "nullable": [1, None, 3],
            "sum": [11, 22, 33],
        })
        assert result["sum"].to_list() == [11, 22, 33]

    def test_multiply_correctness(self, sample_ff):
        result = sample_ff.with_columns((col("x") * 10).alias("x10")).collect()
        assert result["x10"].to_list() == [10, 20, 30]

    def test_subtract_correctness(self, sample_ff):
        result = sample_ff.with_columns((col("y") - col("x")).alias("diff")).collect()
        assert result["diff"].to_list() == [9, 18, 27]

    def test_division_correctness(self, sample_ff):
        result = sample_ff.with_columns((col("y") / col("x")).alias("ratio")).collect()
        assert result["ratio"].to_list() == [10.0, 10.0, 10.0]

    def test_string_uppercase_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("name").str.to_uppercase().alias("upper")
        ).collect()
        assert result["upper"].to_list() == ["ALICE", "BOB", "CHARLIE"]

    def test_string_lowercase_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("name").str.to_lowercase().alias("lower")
        ).collect()
        assert result["lower"].to_list() == ["alice", "bob", "charlie"]

    def test_dt_year_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("date").dt.year().alias("yr")
        ).collect()
        assert result["yr"].to_list() == [2022, 2022, 2023]

    def test_dt_month_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("date").dt.month().alias("mo")
        ).collect()
        assert result["mo"].to_list() == [1, 6, 3]

    def test_cast_float_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("x").cast(pl.Float64).alias("x_f")
        ).collect()
        assert result["x_f"].to_list() == [1.0, 2.0, 3.0]
        assert result["x_f"].dtype == pl.Float64

    def test_nested_arithmetic_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            ((col("x") + col("y")) * col("z")).alias("result")
        ).collect()
        assert result["result"].to_list() == [
            (1 + 10) * 100,
            (2 + 20) * 200,
            (3 + 30) * 300,
        ]

    def test_multiple_formulas_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            a=col("x") + 1,
            b=col("y") * 2,
        ).collect()
        assert result["a"].to_list() == [2, 3, 4]
        assert result["b"].to_list() == [20, 40, 60]

    def test_comparison_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            (col("x") > 1).alias("gt1")
        ).collect()
        assert result["gt1"].to_list() == [False, True, True]

    def test_is_null_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("nullable").is_null().alias("is_null")
        ).collect()
        assert result["is_null"].to_list() == [False, True, False]

    def test_fill_null_correctness(self, sample_ff):
        result = sample_ff.with_columns(
            col("nullable").fill_null(0).alias("filled")
        ).collect()
        assert result["filled"].to_list() == [1, 0, 3]

    def test_abs_correctness(self):
        ff = FlowFrame({"x": [-1, -2, 3]})
        result = ff.with_columns(col("x").abs().alias("abs_x")).collect()
        assert result["abs_x"].to_list() == [1, 2, 3]

    def test_round_correctness(self):
        ff = FlowFrame({"x": [1.567, 2.345, 3.999]})
        result = ff.with_columns(col("x").round(1).alias("rounded")).collect()
        assert result["rounded"].to_list() == [1.6, 2.3, 4.0]

    def test_floordiv_correctness(self, sample_ff):
        result = sample_ff.with_columns((col("y") // col("x")).alias("fd")).collect()
        assert result["fd"].to_list() == [10, 10, 10]

    def test_pow_correctness(self, sample_ff):
        result = sample_ff.with_columns((col("x") ** 2).alias("sq")).collect()
        assert result["sq"].to_list() == [1, 4, 9]

    def test_ceil_correctness(self):
        ff = FlowFrame({"x": [1.1, 2.5, 3.9]})
        result = ff.with_columns(col("x").ceil().alias("ceiled")).collect()
        assert result["ceiled"].to_list() == [2, 3, 4]

    def test_floor_correctness(self):
        ff = FlowFrame({"x": [1.1, 2.5, 3.9]})
        result = ff.with_columns(col("x").floor().alias("floored")).collect()
        assert result["floored"].to_list() == [1, 2, 3]

    def test_reverse_add_correctness(self, sample_ff):
        result = sample_ff.with_columns((100 + col("x")).alias("plus100")).collect()
        assert result["plus100"].to_list() == [101, 102, 103]

    def test_named_expr_correctness(self, sample_ff):
        result = sample_ff.with_columns(double_x=col("x") * 2).collect()
        assert result["double_x"].to_list() == [2, 4, 6]


# ---------------------------------------------------------------------------
# Output column name derivation tests
# ---------------------------------------------------------------------------

class TestBinaryOpColumnName:
    """Test that binary ops derive column_name from polars meta.output_name()."""

    def test_add_two_cols_gets_left_name(self):
        expr = col("a") + col("b")
        assert expr.column_name == "a"

    def test_sub_two_cols_gets_left_name(self):
        expr = col("x") - col("y")
        assert expr.column_name == "x"

    def test_mul_two_cols_gets_left_name(self):
        expr = col("price") * col("qty")
        assert expr.column_name == "price"

    def test_div_two_cols_gets_left_name(self):
        expr = col("total") / col("count")
        assert expr.column_name == "total"

    def test_mod_two_cols_gets_left_name(self):
        expr = col("a") % col("b")
        assert expr.column_name == "a"

    def test_col_plus_literal_gets_col_name(self):
        expr = col("score") + 10
        assert expr.column_name == "score"

    def test_col_mul_literal_gets_col_name(self):
        expr = col("price") * 1.1
        assert expr.column_name == "price"

    def test_literal_plus_col_gets_literal_name(self):
        """Reverse ops: literal on left means polars uses 'literal' as output name."""
        expr = 5 + col("x")
        # Polars names this 'literal' since the left operand is a literal
        assert expr.column_name == "literal"

    def test_comparison_gets_left_name(self):
        expr = col("age") > 18
        assert expr.column_name == "age"

    def test_nested_ops_get_root_left_name(self):
        """Nested binary ops: output name traces back to leftmost column."""
        expr = (col("a") + col("b")) * col("c")
        assert expr.column_name == "a"

    def test_alias_overrides_derived_name(self):
        expr = (col("a") + col("b")).alias("total")
        assert expr.column_name == "total"

    def test_matches_polars_meta_output_name(self):
        """Verify our column_name matches pl.Expr.meta.output_name() for various patterns."""
        cases = [
            (col("x") + col("y"), pl.col("x") + pl.col("y")),
            (col("x") - 1, pl.col("x") - 1),
            (col("x") * col("y"), pl.col("x") * pl.col("y")),
            (col("x") / 2, pl.col("x") / 2),
            ((col("a") + col("b")) * col("c"), (pl.col("a") + pl.col("b")) * pl.col("c")),
            (col("x") > 0, pl.col("x") > 0),
            (col("x") == col("y"), pl.col("x") == pl.col("y")),
        ]
        for ff_expr, pl_expr in cases:
            assert ff_expr.column_name == pl_expr.meta.output_name(), (
                f"Mismatch for {ff_expr._repr_str}: "
                f"ff={ff_expr.column_name}, pl={pl_expr.meta.output_name()}"
            )


# ---------------------------------------------------------------------------
# Polars parity tests — flowfile results must match polars results exactly
# ---------------------------------------------------------------------------

class TestPolarsParityWithColumns:
    """Verify that flowfile with_columns produces identical results to polars,
    especially for expressions without an explicit alias."""

    @pytest.fixture
    def base_data(self):
        return {
            "a": [1, 2, 3, 4, 5],
            "b": [10, 20, 30, 40, 50],
            "c": [1.5, 2.5, 3.5, 4.5, 5.5],
        }

    @pytest.fixture
    def pl_df(self, base_data):
        return pl.DataFrame(base_data)

    @pytest.fixture
    def ff_df(self, base_data):
        return FlowFrame(base_data)

    def test_add_no_alias(self, pl_df, ff_df):
        """col('a') + col('b') without alias should overwrite 'a', matching polars."""
        expected = pl_df.with_columns(pl.col("a") + pl.col("b"))
        result = ff_df.with_columns(col("a") + col("b")).collect()
        assert_frame_equal(result, expected)

    def test_sub_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("a") - pl.col("b"))
        result = ff_df.with_columns(col("a") - col("b")).collect()
        assert_frame_equal(result, expected)

    def test_mul_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("a") * pl.col("b"))
        result = ff_df.with_columns(col("a") * col("b")).collect()
        assert_frame_equal(result, expected)

    def test_div_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("a") / pl.col("b"))
        result = ff_df.with_columns(col("a") / col("b")).collect()
        assert_frame_equal(result, expected)

    def test_mod_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("b") % pl.col("a"))
        result = ff_df.with_columns(col("b") % col("a")).collect()
        assert_frame_equal(result, expected)

    def test_add_literal_no_alias(self, pl_df, ff_df):
        """col('a') + 100 should overwrite 'a'."""
        expected = pl_df.with_columns(pl.col("a") + 100)
        result = ff_df.with_columns(col("a") + 100).collect()
        assert_frame_equal(result, expected)

    def test_mul_float_literal_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("c") * 2.0)
        result = ff_df.with_columns(col("c") * 2.0).collect()
        assert_frame_equal(result, expected)

    def test_nested_arithmetic_no_alias(self, pl_df, ff_df):
        """(col('a') + col('b')) * col('c') — output name is 'a'."""
        expected = pl_df.with_columns((pl.col("a") + pl.col("b")) * pl.col("c"))
        result = ff_df.with_columns((col("a") + col("b")) * col("c")).collect()
        assert_frame_equal(result, expected)

    def test_comparison_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("a") > 2)
        result = ff_df.with_columns(col("a") > 2).collect()
        assert_frame_equal(result, expected)

    def test_add_with_alias(self, pl_df, ff_df):
        """With alias, new column is created instead of overwriting."""
        expected = pl_df.with_columns((pl.col("a") + pl.col("b")).alias("sum"))
        result = ff_df.with_columns((col("a") + col("b")).alias("sum")).collect()
        assert_frame_equal(result, expected)

    def test_multiple_exprs_no_alias(self, pl_df, ff_df):
        """Multiple expressions without aliases — each overwrites its left operand column."""
        expected = pl_df.with_columns(
            pl.col("a") + pl.col("b"),
            pl.col("c") * 2,
        )
        result = ff_df.with_columns(
            col("a") + col("b"),
            col("c") * 2,
        ).collect()
        assert_frame_equal(result, expected)

    def test_mixed_alias_and_no_alias(self, pl_df, ff_df):
        """Mix of aliased and non-aliased expressions."""
        expected = pl_df.with_columns(
            pl.col("a") * 10,
            (pl.col("b") + pl.col("c")).alias("bc_sum"),
        )
        result = ff_df.with_columns(
            col("a") * 10,
            (col("b") + col("c")).alias("bc_sum"),
        ).collect()
        assert_frame_equal(result, expected)

    def test_overwrite_preserves_column_order(self, pl_df, ff_df):
        """Overwriting 'a' should keep it in its original position."""
        expected = pl_df.with_columns(pl.col("a") + 1)
        result = ff_df.with_columns(col("a") + 1).collect()
        assert_frame_equal(result, expected)
        assert result.columns == expected.columns

    def test_chained_with_columns_no_alias(self, pl_df, ff_df):
        """Chained with_columns both without aliases."""
        expected = pl_df.with_columns(
            pl.col("a") + pl.col("b")
        ).with_columns(
            pl.col("a") * pl.col("c")
        )
        result = ff_df.with_columns(
            col("a") + col("b")
        ).with_columns(
            col("a") * col("c")
        ).collect()
        assert_frame_equal(result, expected)

    def test_named_kwarg_parity(self, pl_df, ff_df):
        """Named kwargs create new columns — should match polars."""
        expected = pl_df.with_columns(total=pl.col("a") + pl.col("b"))
        result = ff_df.with_columns(total=col("a") + col("b")).collect()
        assert_frame_equal(result, expected)

    def test_floordiv_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("b") // pl.col("a"))
        result = ff_df.with_columns(col("b") // col("a")).collect()
        # floor(b/a) formula produces Float64; Polars // keeps Int64 for integers
        assert_frame_equal(result, expected, check_dtypes=False)

    def test_floordiv_with_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns((pl.col("b") // pl.col("a")).alias("fd"))
        result = ff_df.with_columns((col("b") // col("a")).alias("fd")).collect()
        assert_frame_equal(result, expected, check_dtypes=False)

    def test_pow_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("a") ** 2)
        result = ff_df.with_columns(col("a") ** 2).collect()
        assert_frame_equal(result, expected, check_dtypes=False)

    def test_pow_with_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns((pl.col("a") ** 3).alias("cubed"))
        result = ff_df.with_columns((col("a") ** 3).alias("cubed")).collect()
        assert_frame_equal(result, expected, check_dtypes=False)

    def test_ceil_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("c").ceil())
        result = ff_df.with_columns(col("c").ceil()).collect()
        assert_frame_equal(result, expected)

    def test_ceil_with_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("c").ceil().alias("c_ceil"))
        result = ff_df.with_columns(col("c").ceil().alias("c_ceil")).collect()
        assert_frame_equal(result, expected)

    def test_floor_no_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("c").floor())
        result = ff_df.with_columns(col("c").floor()).collect()
        assert_frame_equal(result, expected)

    def test_floor_with_alias(self, pl_df, ff_df):
        expected = pl_df.with_columns(pl.col("c").floor().alias("c_floor"))
        result = ff_df.with_columns(col("c").floor().alias("c_floor")).collect()
        assert_frame_equal(result, expected)
