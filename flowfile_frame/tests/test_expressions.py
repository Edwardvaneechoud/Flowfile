import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_frame import FlowFrame
from flowfile_frame import selectors as sc
from flowfile_frame.expr import Column, Expr, col, count, cum_count, lit, max, mean, min, sum


class TestExpressions:
    """Tests focusing specifically on expression functionality."""

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing expressions."""
        return {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [85.5, 92.0, 78.5, 88.0, 95.5],
            "active": [True, False, True, True, False],
            "date": ["2022-01-01", "2022-02-15", "2022-03-20", "2022-04-10", "2022-05-05"]
        }

    def test_column_expr_creation(self, sample_data):
        """Test basic column expression creation and properties."""
        # Basic column expression
        c = col("age")
        assert isinstance(c, Column)
        assert c.column_name == "age"
        assert str(c) == "pl.col('age')"

        # Column with alias
        c_alias = col("age").alias("user_age")
        assert isinstance(c_alias, Column)
        assert c_alias.column_name == "user_age"
        assert str(c_alias) == "pl.col('age').alias('user_age')"

        # Column with cast
        c_cast = col("score").cast(pl.Int32)
        assert isinstance(c_cast, Column)
        assert str(c_cast) == "pl.col('score').cast(pl.Int32, strict=True)"

        # Cast with custom name
        c_cast_named = col("score").cast(pl.Int32).alias("score_int")
        assert c_cast_named.column_name == "score_int"

    def test_literal_expr_creation(self):
        """Test literal expression creation."""
        # Integer literal
        l_int = lit(42)
        assert isinstance(l_int, Expr)
        assert str(l_int) == "pl.lit(42)"

        # String literal
        l_str = lit("hello")
        assert isinstance(l_str, Expr)
        assert str(l_str) == "pl.lit('hello')"

        # Boolean literal
        l_bool = lit(True)
        assert isinstance(l_bool, Expr)
        assert str(l_bool) == "pl.lit(True)"

        # Float literal
        l_float = lit(3.14)
        assert isinstance(l_float, Expr)
        assert str(l_float) == "pl.lit(3.14)"

    def test_expression_arithmetic(self):
        """Test arithmetic operations with expressions."""
        # Addition
        expr = col("age") + 5
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') + 5)"

        # Subtraction
        expr = col("age") - lit(10)
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') - pl.lit(10))"

        # Multiplication
        expr = col("age") * 2
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') * 2)"

        # Division
        expr = col("age") / 5
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') / 5)"

        # Integer division
        expr = col("age") // 10
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') // 10)"

        # Modulo
        expr = col("age") % 7
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') % 7)"

        # Power
        expr = col("age") ** 2
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') ** 2)"

        # Chained operations
        expr = (col("age") + 5) * 2
        assert isinstance(expr, Expr)
        assert str(expr) == "((pl.col('age') + 5) * 2)"

        # Operations with two columns
        expr = col("age") + col("score")
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') + pl.col('score'))"

    def test_expression_comparison(self):
        """Test comparison operations with expressions."""
        # Equal
        expr = col("age") == 30
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') == 30)"

        # Not equal
        expr = col("age") != 30
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') != 30)"

        # Greater than
        expr = col("age") > 30
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') > 30)"

        # Less than
        expr = col("age") < 30
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') < 30)"

        # Greater than or equal
        expr = col("age") >= 30
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') >= 30)"

        # Less than or equal
        expr = col("age") <= 30
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') <= 30)"

        # Compare two columns
        expr = col("age") > col("score")
        assert isinstance(expr, Expr)
        assert str(expr) == "(pl.col('age') > pl.col('score'))"

    def test_expression_logical(self):
        """Test logical operations with expressions."""
        # And
        expr = (col("age") > 30) & (col("score") > 90)
        assert isinstance(expr, Expr)
        assert str(expr) == "((pl.col('age') > 30) & (pl.col('score') > 90))"

        # Or
        expr = (col("age") < 30) | (col("score") > 90)
        assert isinstance(expr, Expr)
        assert str(expr) == "((pl.col('age') < 30) | (pl.col('score') > 90))"

        # Not
        expr = ~(col("active"))
        assert isinstance(expr, Expr)
        assert str(expr) == "~(pl.col('active'))"

        # Complex logic
        expr = ((col("age") > 30) & (col("score") > 80)) | (col("active") == True)
        assert isinstance(expr, Expr)
        assert str(expr) == "(((pl.col('age') > 30) & (pl.col('score') > 80)) | (pl.col('active') == True))"

    def test_string_methods(self):
        """Test string methods on expressions."""
        # Contains
        expr = col("name").str.contains("a")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('name').str.contains('a', literal=False)"
        assert expr.convertable_to_code

        expr = col("order_date").str.to_date("%d/%m/%Y")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('order_date').str.to_date(format='%d/%m/%Y', strict=True, exact=True, cache=True)"
        assert expr.convertable_to_code

        expr = col("name").str.starts_with("A")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('name').str.starts_with('A')"
        assert expr.convertable_to_code

        # Ends with
        expr = col("name").str.ends_with("e")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('name').str.ends_with('e')"
        assert expr.convertable_to_code

        # Replace
        expr = col("name").str.replace("a", "X")
        assert isinstance(expr, Expr)
        assert expr.convertable_to_code
        assert str(expr) == "pl.col('name').str.replace('a', 'X', literal=False)"

        # To uppercase
        expr = col("name").str.to_uppercase()
        assert isinstance(expr, Expr)
        assert expr.convertable_to_code
        assert str(expr) == "pl.col('name').str.to_uppercase()"

        # To lowercase
        expr = col("name").str.to_lowercase()
        assert isinstance(expr, Expr)
        assert expr.convertable_to_code
        assert str(expr) == "pl.col('name').str.to_lowercase()"

        # Length functions
        expr = col("name").str.len_chars()
        assert isinstance(expr, Expr)
        assert expr.convertable_to_code
        assert str(expr) == "pl.col('name').str.len_chars()"

        # Length with alias
        expr = col("name").str.len_chars().alias("name_length")
        assert isinstance(expr, Expr)
        assert expr.convertable_to_code
        assert str(expr) == "pl.col('name').str.len_chars().alias('name_length')"

        expr = col("name").map_elements(lambda x: x.upper()).str
        assert expr.convertable_to_code  # lambdas are now convertible via AST extraction

    def test_datetime_methods(self):
        """Test datetime methods on expressions."""
        date_col = col("date").cast(pl.Date)
        # Year
        expr = date_col.dt.year()
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('date').cast(pl.Date, strict=True).dt.year()"
        assert expr.convertable_to_code
        # Month
        expr = date_col.dt.month()
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('date').cast(pl.Date, strict=True).dt.month()"
        assert expr.convertable_to_code

        # Day
        expr = date_col.dt.day()
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('date').cast(pl.Date, strict=True).dt.day()"
        assert expr.convertable_to_code

        # Date extraction with alias
        expr = date_col.dt.year().alias("year")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('date').cast(pl.Date, strict=True).dt.year().alias('year')"
        assert expr.convertable_to_code

        expr = date_col.dt.day().map_elements(lambda x: x.upper())
        assert expr.convertable_to_code  # lambdas are now convertible via AST extraction

    def test_null_related_methods(self):
        """Test null-related methods on expressions."""
        # Is null
        expr = col("age").is_null()
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').is_null()"

        # Is not null
        expr = col("age").is_not_null()
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').is_not_null()"

        # Fill null
        expr = col("age").fill_null(0)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').fill_null(0)"

        # Is in
        expr = col("age").is_in([25, 30, 35])
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').is_in([25, 30, 35])"

    def test_aggregation_methods(self):
        """Test aggregation methods on expressions."""
        # Sum
        expr = col("age").sum()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "sum"
        assert str(expr) == "pl.col('age').sum()"

        # Mean
        expr = col("age").mean()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "mean"
        assert str(expr) == "pl.col('age').mean()"

        # Min
        expr = col("age").min()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "min"
        assert str(expr) == "pl.col('age').min()"

        # Max
        expr = col("age").max()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "max"
        assert str(expr) == "pl.col('age').max()"

        # Count
        expr = col("age").count()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "count"
        assert str(expr) == "pl.col('age').count()"

        # First
        expr = col("age").first()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "first"
        assert str(expr) == "pl.col('age').first()"

        # Last
        expr = col("age").last()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "last"
        assert str(expr) == "pl.col('age').last()"

    def test_over_window_functions(self):
        """Test over() window function syntax."""
        # Sum over partition
        expr = col("score").sum().over("category")
        assert isinstance(expr, Expr)
        assert "over(" in str(expr)
        assert "category" in str(expr)

        # Sum over multiple partitions
        expr = col("score").sum().over(["category", "region"])
        assert isinstance(expr, Expr)
        assert "over([" in str(expr)
        assert "category" in str(expr)
        assert "region" in str(expr)

        # Sum over with order by
        expr = col("score").sum().over("category", order_by="date")
        assert isinstance(expr, Expr)
        assert "over(" in str(expr)
        assert "order_by" in str(expr)

        # Sum over with descending order
        expr = col("score").sum().over("category", order_by="date", descending=True)
        assert isinstance(expr, Expr)
        assert "over(" in str(expr)
        assert "order_by" in str(expr)
        assert "descending=True" in str(expr)

        # Sum over with nulls last
        expr = col("score").sum().over("category", order_by="date", nulls_last=True)
        assert isinstance(expr, Expr)
        assert "over(" in str(expr)
        assert "nulls_last=True" in str(expr)

    def test_sort(self):
        """Test sort expression syntax."""
        expr = col("age").sort()
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').sort(descending=False, nulls_last=False)"

        expr = col("age").sort(descending=True)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').sort(descending=True, nulls_last=False)"

        expr = col("age").sort(nulls_last=True)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').sort(descending=False, nulls_last=True)"

    def test_cast(self):
        """Test cast with different data types."""
        # Cast to Int32
        expr = col("score").cast(pl.Int32)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('score').cast(pl.Int32, strict=True)"

        # Cast to Float32
        expr = col("age").cast(pl.Float32)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').cast(pl.Float32, strict=True)"

        # Cast to String
        expr = col("age").cast(pl.String)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').cast(pl.String, strict=True)"

        # Cast to Boolean
        expr = col("age").cast(pl.Boolean)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('age').cast(pl.Boolean, strict=True)"

        # Cast without strict mode
        expr = col("score").cast(pl.Int32, strict=False)
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.col('score').cast(pl.Int32, strict=False)"

    def test_top_level_functions(self):
        """Test top-level aggregation functions."""
        # Sum function
        expr = sum("age")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.sum('age')"

        # Mean function
        expr = mean("age")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.mean('age')"

        # Min function
        expr = min("age")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.min('age')"

        # Max function
        expr = max("age")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.max('age')"

        # Count function
        expr = count("age")
        assert isinstance(expr, Expr)
        assert str(expr) == "pl.count('age')"

        # Cum count function
        expr = cum_count("age")
        assert isinstance(expr, Expr)
        assert "cum_count" in str(expr)

    def test_selectors_with_aggregations(self):
        """Test selectors combined with aggregations."""
        # Numeric selector with sum
        expr = sc.numeric().sum()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "sum"
        assert str(expr) == "pl.selectors.numeric().sum()"

        # String selector with count
        expr = sc.string().count()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "count"
        assert str(expr) == "pl.selectors.string().count()"

        # Float selector with mean
        expr = sc.float_().mean()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "mean"
        assert str(expr) == "pl.selectors.float().mean()"

        # Combined selector with aggregation
        expr = (sc.numeric() & ~sc.float_()).sum()
        assert isinstance(expr, Expr)
        assert expr.agg_func == "sum"
        assert "numeric()" in str(expr)
        assert "float()" in str(expr)

    def test_expression_in_dataframe(self, sample_data):
        """Test expressions actually work in a FlowFrame."""
        df = FlowFrame(sample_data)

        # # Test arithmetic operations
        result = df.select(
            (col("age") + 5).alias("age_plus_5")
        ).collect()
        assert result["age_plus_5"].to_list() == [30, 35, 40, 45, 50]

        # Test comparison operations
        result = df.filter(
            col("age") > 30
        ).collect()
        assert len(result) == 3  # Charlie, David, Eve

        # Test string operations
        result = df.filter(
            col("name").str.contains("a")
        ).collect()
        assert len(result) == 2

        # Test aggregate operations
        result = df.select(
            col("age").sum().alias("total_age")
        ).collect()
        assert result["total_age"][0] == 175  # 25 + 30 + 35 + 40 + 45

        # Test multiple expressions
        result = df.select(
            col("name"),
            (col("age") * 2).alias("double_age"),
            col("score").cast(pl.Int32).alias("score_int")
        ).collect()
        assert result["double_age"].to_list() == [50, 60, 70, 80, 90]
        assert result["score_int"].to_list() == [85, 92, 78, 88, 95]  # Round or truncate, depending on implementation


class TestAdvancedExpressions:
    """Tests for more complex and edge-case expression functionality."""

    @pytest.fixture
    def sample_df(self):
        """Create a sample dataset for testing advanced expressions."""
        data = {
            "id": [1, 2, 3, 4, 5],
            "value_1": [10, 20, 30, 40, 50],
            "value_2": [5, 15, 25, 35, 45],
            "category": ["A", "B", "A", "B", "C"],
            "date": ["2023-01-15", "2023-02-10", "2023-03-05", "2023-04-20", "2023-05-15"],
            "nested": [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
            "flags": [True, False, True, False, True]
        }
        return FlowFrame(data)

    def test_chained_operations(self, sample_df):
        """Test chaining multiple operations on expressions."""
        # Chain multiple transformations
        result = sample_df.select(
            col("value_1")
            .cast(pl.Float64)
            .fill_null(0)
            .alias("processed_value")
        ).collect()

        assert "processed_value" in result.columns
        assert result["processed_value"].dtype == pl.Float64

        # Chain comparison and then aggregation
        result = sample_df.filter(
            col("value_1") > 20
        ).select(
            col("value_1").sum().alias("filtered_sum")
        ).collect()

        assert result["filtered_sum"][0] == 120  # 30 + 40 + 50

        # Chain string operations
        result = sample_df.select(
            col("category")
            .str.to_lowercase()
            .str.replace("a", "x")
            .alias("modified_category")
        ).collect()

        assert "x" in result["modified_category"].to_list()

    def test_complex_window_functions(self, sample_df):
        """Test more complex window function scenarios."""
        # Add row numbers within each category
        result = sample_df.select(
            col("id"),
            col("category"),
            col("value_1"),
            col("id").cum_count().over("category").alias("row_in_category")
        ).collect()

        # Check that row numbers restart for each category
        a_rows = result.filter(pl.col("category") == "A")
        assert a_rows["row_in_category"].to_list() == [1, 2]

        b_rows = result.filter(pl.col("category") == "B")
        assert b_rows["row_in_category"].to_list() == [1, 2]

        # Running sum within category
        result = sample_df.select(
            col("id"),
            col("category"),
            col("value_1"),
            col("value_1").sum().over(
                "category",
                order_by=col("id")
            ).alias("running_sum")
        ).collect()

        # Check running sums
        a_rows = result.filter(pl.col("category") == "A")
        assert a_rows["running_sum"].to_list() == [40, 40]  # 10, then 10+30

        # Window function with ordered partition
        result = sample_df.select(
            col("id"),
            col("category"),
            col("value_1"),
            col("value_1").max().over(
                "category",
                order_by=col("id"),
                descending=True
            ).alias("reverse_running_max")
        ).collect()

        assert "reverse_running_max" in result.columns

    def test_nested_expressions(self, sample_df):
        """Test using expressions within other expressions."""
        # Create calculated field and then use it in filter
        result = sample_df.with_columns(
            (col("value_1") + col("value_2")).alias("total")
        ).filter(
            col("total") > 50
        ).collect()

        assert len(result) == 3  # id 3, 4, 5
        assert result["id"].to_list() == [3, 4, 5]

        # Create multiple chained filters
        result = sample_df.filter(
            col("value_1") > 20
        ).filter(
            col("category") == "A"
        ).collect()

        assert len(result) == 1
        assert result["id"][0] == 3

    def test_select_lambda_expression(self, sample_df):
        res = sample_df.select(col("nested").map_elements(lambda x: x[0])).collect()
        expected = sample_df.data.select(pl.col("nested").map_elements(lambda x: x[0])).collect()
        assert_frame_equal(res, expected)

    def test_with_columns_named_expressions_lambda(self, sample_df):
        res = sample_df.with_columns(output_col=col("nested").map_elements(lambda x: x[0])).collect()
        expected = sample_df.data.with_columns(output_col=pl.col("nested").map_elements(lambda x: x[0])).collect()
        assert_frame_equal(res, expected)

    def test_with_columns_lambda_expression(self, sample_df):
        res = sample_df.with_columns(col("nested").map_elements(lambda x: x[0])).collect()
        expected = sample_df.data.with_columns(pl.col("nested").map_elements(lambda x: x[0])).collect()
        assert_frame_equal(res, expected)

    def test_filter_lambda_expression(self, sample_df):
        res = sample_df.filter(col("nested").map_elements(lambda x: x[0] == 1)).collect()
        expected = sample_df.data.filter(pl.col("nested").map_elements(lambda x: x[0] == 1)).collect()
        assert_frame_equal(res, expected)

    def test_filter_constraint(self, sample_df):
        expected = sample_df.filter(category="A").collect()
        res = sample_df.data.filter(category="A").collect()
        assert_frame_equal(res, expected)

    def test_with_columns_with_custom_defined_function(self, sample_df):
        def first_element(x):
            return str(x)[-1]
        res = sample_df.with_columns(col("value_1").map_batches(first_element))
        res.get_node_settings().setting_input

    def test_sort_with_custom_defined_function(self, sample_df):
        def first_element(x):
            return -(x)
        res = sample_df.sort(col("value_1").map_batches(first_element).alias('test'))
        assert 'first_element' in res.get_node_settings().setting_input.polars_code_input.polars_code
        # ensure the actual function ref is used
        result_collected = res.collect()
        expected_collected = sample_df.data.sort(pl.col("value_1").map_batches(first_element).alias('test')).collect()
        assert_frame_equal(result_collected, expected_collected)

    def test_select_with_custom_defined_function(self, sample_df):
        def first_element(x):
            return x*2
        res = sample_df.select(col("value_1").map_batches(first_element))
        expected = sample_df.data.select(pl.col("value_1").map_elements(first_element))
        assert_frame_equal(res.collect(), expected.collect())
        node_settings = res.get_node_settings()
        assert node_settings.node_type == 'polars_code'
        assert node_settings.setting_input

    def test_sort_lambda_expression(self, sample_df):
        res = sample_df.sort(col("nested").map_elements(lambda x: x[0]).sort(descending=True)).collect()
        expected = sample_df.data.sort(pl.col("nested").map_elements(lambda x: x[0]).sort(descending=True)).collect()
        assert_frame_equal(res, expected)

    def test_conditional_expressions(self, sample_df):
        """Test expressions with conditional logic."""
        # Functional equivalent of "if/else" using filters and operations
        high_values = sample_df.filter(col("value_1") >= 30).select(
            col("id"),
            col("value_1"),
            lit("high").alias("tag")
        )

        low_values = sample_df.filter(col("value_1") < 30).select(
            col("id"),
            col("value_1"),
            lit("low").alias("tag")
        )

        # Concatenate the results
        result = high_values.concat(low_values).sort("id").collect()

        assert len(result) == 5
        assert result["tag"].to_list() == ["low", "low", "high", "high", "high"]

    def test_operator_precedence(self, sample_df):
        """Test operator precedence in expressions."""
        # Test arithmetic precedence
        result = sample_df.select(
            col("value_1") + col("value_2") * 2
        ).collect()

        # Multiplication should happen before addition
        expected = [
            10 + (5 * 2),
            20 + (15 * 2),
            30 + (25 * 2),
            40 + (35 * 2),
            50 + (45 * 2)
        ]
        assert result[result.columns[0]].to_list() == expected

        # Test with parentheses to change precedence
        result = sample_df.select(
            (col("value_1") + col("value_2")) * 2
        ).collect()

        # Addition should happen before multiplication now
        expected = [
            (10 + 5) * 2,
            (20 + 15) * 2,
            (30 + 25) * 2,
            (40 + 35) * 2,
            (50 + 45) * 2
        ]
        assert result[result.columns[0]].to_list() == expected

        # Test logical operator precedence
        result = sample_df.filter(
            (col("value_1") > 20) & (col("value_2") < 40) | (col("category") == "C")
        ).collect()

        # Should include id=3, id=4 (match first condition), and id=5 (match second)
        assert set(result["id"].to_list()) == {3, 4, 5}

        # Change precedence with parentheses
        result = sample_df.filter(
            (col("value_1") > 20) & ((col("value_2") < 40) | (col("category") == "C"))
        ).collect()

        # Only id=3 and id=4 should match now
        assert set(result["id"].to_list()) == {3, 4, 5}  # id=5 still matches

    def test_aggregation_after_transformation(self, sample_df):
        """Test aggregating after transforming columns."""
        # Transform then aggregate
        result = sample_df.select(
            col("value_1").cast(pl.Float64).sum().alias("float_sum")
        ).collect()

        assert result["float_sum"].dtype == pl.Float64
        assert result["float_sum"][0] == 150.0

        # More complex: transform, filter, then aggregate
        result = sample_df.select(
            col("value_1")
            .filter(col("category") == "A")
            .sum()
            .alias("a_sum")
        ).collect()

        assert result["a_sum"][0] == 40  # 10 + 30

    def test_explode_with_expressions(self, sample_df):
        """Test using expressions with explode operations."""
        # First transform the nested column, then explode
        result = sample_df.with_columns(
            col("nested").alias("modified_nested")
        ).explode("modified_nested").collect()

        # Should have 10 rows (5 original rows, each with 2 nested values)
        assert len(result) == 10

        # Values should be flattened
        assert set(result["modified_nested"].to_list()) == set(range(1, 11))

        # Check row expansion is correct
        row_1 = result.filter(pl.col("id") == 1)
        assert len(row_1) == 2
        assert set(row_1["modified_nested"].to_list()) == {1, 2}

    def test_cast_chains(self, sample_df):
        """Test chained cast operations."""
        # Cast to string, then modify, then cast back
        result = sample_df.select(
            col("value_1")
            .cast(pl.String)
            .str.replace("0", "9")
            .cast(pl.Int32)
            .alias("modified_value")
        ).collect()

        # Check that string manipulation worked before the cast back
        assert result["modified_value"].to_list() == [19, 29, 39, 49, 59]

        # Test with multiple transformations
        result = sample_df.select(
            col("value_1")
            .cast(pl.Float64)
            .alias("float_val")
        ).with_columns(
            col("float_val")
            .fill_null(0.0)
            .alias("float_val")
        ).collect()

        assert result["float_val"].dtype == pl.Float64
        assert result["float_val"].to_list() == [10.0, 20.0, 30.0, 40.0, 50.0]


class TestLambdaSerialization:
    """Tests for lambda-to-named-function conversion in code generation."""

    @pytest.fixture
    def sample_df(self):
        return FlowFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value_1": [10, 20, 30, 40, 50],
                "nested": [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
                "category": ["A", "B", "A", "B", "C"],
            }
        )

    def test_extract_lambda_source_simple(self):
        """Test that _extract_lambda_source converts a simple lambda to a named function."""
        from flowfile_frame.utils import _extract_lambda_source

        fn = lambda x: x * 2  # noqa: E731
        func_def, func_name = _extract_lambda_source(fn)
        assert func_def is not None
        assert func_name is not None
        assert func_name.startswith("_lambda_fn_")
        assert "def " in func_def
        assert "return" in func_def
        assert "x * 2" in func_def

    def test_extract_lambda_source_with_closure(self):
        """Test lambda extraction captures closure variables."""
        from flowfile_frame.utils import _extract_lambda_source

        threshold = 10
        fn = lambda x: x > threshold  # noqa: E731
        func_def, func_name = _extract_lambda_source(fn)
        assert func_def is not None
        assert "threshold = 10" in func_def
        assert "return" in func_def

    def test_extract_lambda_source_non_representable_closure(self):
        """Test that lambdas with non-representable closures return None."""
        from flowfile_frame.utils import _extract_lambda_source

        class Custom:
            pass

        obj = Custom()
        fn = lambda x: obj  # noqa: E731
        func_def, func_name = _extract_lambda_source(fn)
        assert func_def is None
        assert func_name is None

    def test_with_columns_lambda_generates_code(self, sample_df):
        """Test that a lambda in with_columns produces code instead of serialized LazyFrame."""
        result = sample_df.with_columns(
            col("nested").map_elements(lambda x: x[0]).alias("first_elem")
        )
        node_settings = result.get_node_settings()
        polars_code = node_settings.setting_input.polars_code_input.polars_code
        # Should contain a function definition, not a serialized LazyFrame
        assert "serialized_value" not in polars_code
        assert "_lambda_fn_" in polars_code
        assert "def " in polars_code

    def test_with_columns_lambda_correctness(self, sample_df):
        """Test that lambda code generation produces correct results."""
        res = sample_df.with_columns(
            col("nested").map_elements(lambda x: x[0]).alias("first_elem")
        ).collect()
        expected = sample_df.data.with_columns(
            pl.col("nested").map_elements(lambda x: x[0]).alias("first_elem")
        ).collect()
        assert_frame_equal(res, expected)

    def test_select_lambda_generates_code(self, sample_df):
        """Test that a lambda in select produces code instead of serialized LazyFrame."""
        result = sample_df.select(
            col("nested").map_elements(lambda x: x[0])
        )
        node_settings = result.get_node_settings()
        polars_code = node_settings.setting_input.polars_code_input.polars_code
        assert "serialized_value" not in polars_code
        assert "_lambda_fn_" in polars_code

    def test_filter_lambda_correctness(self, sample_df):
        """Test that filter with lambda still produces correct results."""
        res = sample_df.filter(
            col("nested").map_elements(lambda x: x[0] == 1)
        ).collect()
        expected = sample_df.data.filter(
            pl.col("nested").map_elements(lambda x: x[0] == 1)
        ).collect()
        assert_frame_equal(res, expected)

    def test_lambda_with_closure_variable(self, sample_df):
        """Test that lambdas capturing closure variables work correctly."""
        threshold = 25
        res = sample_df.filter(
            col("value_1").map_elements(lambda x: x > threshold)
        ).collect()
        expected = sample_df.data.filter(
            pl.col("value_1").map_elements(lambda x: x > threshold)
        ).collect()
        assert_frame_equal(res, expected)

    def test_named_function_still_works(self, sample_df):
        """Verify that named functions continue to work as before."""
        def double(x):
            return x * 2

        res = sample_df.with_columns(col("value_1").map_batches(double))
        node_settings = res.get_node_settings()
        polars_code = node_settings.setting_input.polars_code_input.polars_code
        assert "double" in polars_code
        assert "serialized_value" not in polars_code


if __name__ == "__main__":
    pytest.main([__file__])
