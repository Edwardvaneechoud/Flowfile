import polars as pl
import polars.selectors as cs
import pytest
from polars.testing import assert_frame_equal

from flowfile_frame.expr import col
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.group_frame import GroupByFrame


class TestGroupByFrame:
    """Tests specifically for GroupByFrame functionality."""

    @pytest.fixture
    def sales_data(self):
        """Create a sample dataset for testing GroupByFrame."""
        data = {
            "region": ["North", "South", "North", "South", "East", "West", "East",
                       "West", "North", "South", "East", "West"],
            "category": ["A", "B", "A", "B", "C", "A", "B", "C", "B", "C", "A", "B"],
            "product": ["P1", "P2", "P3", "P1", "P2", "P3", "P1", "P2", "P3", "P1", "P2", "P3"],
            "sales": [100, 200, 150, 300, 250, 400, 220, 180, 120, 270, 350, 190],
            "units": [10, 20, 15, 30, 25, 40, 22, 18, 12, 27, 35, 19],
            "returns": [1, 2, 0, 3, 1, 0, 2, 1, 0, 2, 3, 1],
            "date": ["2023-01-15", "2023-01-20", "2023-02-10", "2023-02-15",
                     "2023-03-05", "2023-03-10", "2023-04-05", "2023-04-10",
                     "2023-05-15", "2023-05-20", "2023-06-05", "2023-06-10"],
            "is_promoted": [True, False, True, False, True, False, True, False,
                            True, False, True, False]
        }
        return FlowFrame(data)

    def test_group_by_construction(self, sales_data):
        """Test creation of GroupByFrame objects."""
        gb = sales_data.group_by("region")
        assert isinstance(gb, GroupByFrame)

        assert hasattr(gb, "node_id")
        assert gb.node_id > 0

        assert hasattr(gb, "parent")
        assert gb.parent is sales_data

        assert hasattr(gb, "by_cols")
        assert len(gb.by_cols) == 1
        assert gb.by_cols[0] == "region"

        gb2 = sales_data.group_by(["region", "category"])
        assert isinstance(gb2, GroupByFrame)
        assert len(gb2.by_cols) == 2
        assert gb2.by_cols[0] == "region"
        assert gb2.by_cols[1] == "category"

        gb3 = sales_data.group_by(col("region"), col("category"))
        assert isinstance(gb3, GroupByFrame)
        assert len(gb3.by_cols) == 2

        gb4 = sales_data.group_by(col("region"), area=col("category"))
        assert isinstance(gb4, GroupByFrame)

    def test_group_by_len(self, sales_data):
        """Test len() method on GroupByFrame."""
        result = sales_data.group_by("region").len().collect()

        assert "region" in result.columns
        assert "len" in result.columns

        assert len(result) == 4  # 4 unique regions

        north_row = result.filter(pl.col("region") == "North")
        assert north_row["len"][0] == 3  # 3 North regions

        south_row = result.filter(pl.col("region") == "South")
        assert south_row["len"][0] == 3  # 3 South regions

        east_row = result.filter(pl.col("region") == "East")
        assert east_row["len"][0] == 3  # 3 East regions

        west_row = result.filter(pl.col("region") == "West")
        assert west_row["len"][0] == 3  # 3 West regions

    def test_group_by_count(self, sales_data):
        """Test count() method on GroupByFrame."""
        result = sales_data.group_by("region").count().collect()

        assert "region" in result.columns
        assert "count" in result.columns

        # Count should be the same as len for this dataset
        assert len(result) == 4  # 4 unique regions

        north_row = result.filter(pl.col("region") == "North")
        assert north_row["count"][0] == 3  # 3 North regions

    def test_group_by_sum(self, sales_data):
        """Test sum() method on GroupByFrame."""
        result = sales_data.group_by("region").sum().collect()

        assert "region" in result.columns
        assert "sales" in result.columns
        assert "units" in result.columns
        assert "returns" in result.columns
        assert "category" not in result.columns
        assert "product" not in result.columns

        north_row = result.filter(pl.col("region") == "North")
        assert north_row["sales"][0] == 370  # 100 + 150 + 120
        assert north_row["units"][0] == 37   # 10 + 15 + 12
        assert north_row["returns"][0] == 1  # 1 + 0 + 0

    def test_group_by_mean(self, sales_data):
        """Test mean() method on GroupByFrame."""
        result = sales_data.group_by("region").mean().collect()

        assert "region" in result.columns
        assert "sales" in result.columns
        assert "units" in result.columns
        assert "returns" in result.columns

        east_row = result.filter(pl.col("region") == "East")
        assert east_row["sales"][0] == (250 + 220 + 350) / 3
        assert east_row["units"][0] == (25 + 22 + 35) / 3
        assert east_row["returns"][0] == (1 + 2 + 3) / 3

    def test_group_by_min(self, sales_data):
        """Test min() method on GroupByFrame."""
        result = sales_data.group_by("region").min().collect()

        assert "region" in result.columns
        assert "sales" in result.columns
        assert "units" in result.columns
        assert "returns" in result.columns

        south_row = result.filter(pl.col("region") == "South")
        assert south_row["sales"][0] == 200  # min of 200, 300, 270
        assert south_row["units"][0] == 20   # min of 20, 30, 27
        assert south_row["returns"][0] == 2  # min of 2, 3, 2

    def test_group_by_max(self, sales_data):
        """Test max() method on GroupByFrame."""
        result = sales_data.group_by("region").max().collect()

        assert "region" in result.columns
        assert "sales" in result.columns
        assert "units" in result.columns
        assert "returns" in result.columns

        west_row = result.filter(pl.col("region") == "West")
        assert west_row["sales"][0] == 400  # max of 400, 180, 190
        assert west_row["units"][0] == 40   # max of 40, 18, 19
        assert west_row["returns"][0] == 1  # max of 0, 1, 1

    def test_group_by_median(self, sales_data):
        """Test median() method on GroupByFrame."""
        result = sales_data.group_by("region").median().collect()

        assert "region" in result.columns
        assert "sales" in result.columns
        assert "units" in result.columns
        assert "returns" in result.columns

        # Verify medians for North region (values: 100, 150, 120)
        north_row = result.filter(pl.col("region") == "North")
        assert north_row["sales"][0] == 120  # median of 100, 150, 120

    def test_group_by_first(self, sales_data):
        """Test first() method on GroupByFrame."""
        result = sales_data.group_by("region").first().collect()

        assert "region" in result.columns
        assert "category" in result.columns
        assert "product" in result.columns
        assert "sales" in result.columns

        north_row = result.filter(pl.col("region") == "North")
        assert north_row["category"][0] == "A"  # First category for North
        assert north_row["product"][0] == "P1"  # First product for North
        assert north_row["sales"][0] == 100    # First sales for North

    def test_group_by_last(self, sales_data):
        """Test last() method on GroupByFrame."""
        result = sales_data.group_by("region").last().collect()

        assert "region" in result.columns
        assert "category" in result.columns
        assert "product" in result.columns
        assert "sales" in result.columns

        south_row = result.filter(pl.col("region") == "South")
        assert south_row["category"][0] == "C"  # Last category for South
        assert south_row["product"][0] == "P1"  # Last product for South
        assert south_row["sales"][0] == 270    # Last sales for South

    def test_group_by_agg_single(self, sales_data):
        """Test agg() method with a single aggregation."""
        result = sales_data.group_by("region").agg(
            col("sales").sum().alias("total_sales")
        ).collect()

        assert "region" in result.columns
        assert "total_sales" in result.columns
        assert len(result.columns) == 2

        north_row = result.filter(pl.col("region") == "North")
        assert north_row["total_sales"][0] == 370  # 100 + 150 + 120

    def test_group_by_agg_multiple(self, sales_data):
        """Test agg() method with multiple aggregations."""
        flow_frame = sales_data.group_by("region").agg([
            col("sales").sum().alias("total_sales"),
            col("sales").mean().alias("avg_sales"),
            col("units").sum().alias("total_units"),
            col("returns").mean().alias("avg_returns")
        ])
        assert flow_frame.get_node_settings().node_type == 'group_by', 'Should be executed with group by node'
        result = flow_frame.collect()
        expected_result = sales_data.data.group_by("region").agg([
            pl.col("sales").sum().alias("total_sales"),
            pl.col("sales").mean().alias("avg_sales"),
            pl.col("units").sum().alias("total_units"),
            pl.col("returns").mean().alias("avg_returns")
        ]).collect()
        assert_frame_equal(result, expected_result, check_row_order=False)

    def test_group_by_agg_named(self, sales_data):
        """Test agg() method with named aggregations."""
        result = sales_data.group_by("region").agg(
            total=col("sales").sum(),
            average=col("sales").mean(),
            max_sale=col("sales").max(),
            min_sale=col("sales").min()
        ).collect()

        assert "region" in result.columns
        assert "total" in result.columns
        assert "average" in result.columns
        assert "max_sale" in result.columns
        assert "min_sale" in result.columns

        west_row = result.filter(pl.col("region") == "West")
        assert west_row["total"][0] == 770       # 400 + 180 + 190
        assert west_row["average"][0] == 770/3   # (400 + 180 + 190) / 3
        assert west_row["max_sale"][0] == 400
        assert west_row["min_sale"][0] == 180

    def test_group_by_agg_mixed(self, sales_data):
        """Test agg() method with mixed positional and named arguments."""
        result = sales_data.group_by("region").agg(
            col("sales").sum().alias("total_sales"),
            avg_units=col("units").mean(),
            max_returns=col("returns").max()
        ).collect()

        assert "region" in result.columns
        assert "total_sales" in result.columns
        assert "avg_units" in result.columns
        assert "max_returns" in result.columns

        south_row = result.filter(pl.col("region") == "South")
        assert south_row["total_sales"][0] == 770  # 200 + 300 + 270
        assert south_row["avg_units"][0] == 77/3   # (20 + 30 + 27) / 3
        assert south_row["max_returns"][0] == 3

    def test_group_by_agg_expressions(self, sales_data):
        """Test agg() with more complex expressions."""
        flow_frame: FlowFrame = sales_data.group_by("region").agg(
            revenue=(col("sales") * col("units")).sum(),
            return_rate=(col("returns") / col("units")).mean()
        )
        assert flow_frame.get_node_settings().node_type == 'polars_code', 'Group by should be performed in polars code'
        result = flow_frame.collect()
        expected_result = sales_data.data.group_by("region").agg(
            revenue=(pl.col("sales") * pl.col("units")).sum(),
            return_rate=(pl.col("returns") / pl.col("units")).mean()).collect()
        assert_frame_equal(expected_result, result, check_row_order=False)

    def test_group_by_agg_unique_counts(self):
        """Regression: bare unique_counts() must use the polars_code fallback, not the native node.

        pl.unique_counts is not a top-level Polars function (expression-method only), so the
        native group_by node raised ValueError("Could not execute the function").
        """
        data = {"group": ["a", "a", "a", "b", "b", "b", "b"], "value": [1, 1, 2, 3, 4, 3, 5]}
        flow_frame = FlowFrame(data).group_by("group").agg(col("value").unique_counts())
        assert flow_frame.get_node_settings().node_type == "polars_code"
        result = flow_frame.collect().sort("group").with_columns(pl.col("value").list.sort())
        expected = (
            pl.DataFrame(data).group_by("group").agg(pl.col("value").unique_counts())
            .sort("group").with_columns(pl.col("value").list.sort())
        )
        assert_frame_equal(result, expected)

    def test_group_by_agg_unique_counts_named(self):
        """Regression: named unique_counts() aggregation must also use the polars_code fallback."""
        data = {"group": ["a", "a", "a", "b", "b", "b", "b"], "value": [1, 1, 2, 3, 4, 3, 5]}
        flow_frame = FlowFrame(data).group_by("group").agg(counts=col("value").unique_counts())
        assert flow_frame.get_node_settings().node_type == "polars_code"
        result = flow_frame.collect().sort("group").with_columns(pl.col("counts").list.sort())
        expected = (
            pl.DataFrame(data).group_by("group").agg(counts=pl.col("value").unique_counts())
            .sort("group").with_columns(pl.col("counts").list.sort())
        )
        assert_frame_equal(result, expected)

    def test_group_by_agg_implode(self):
        """Regression: bare implode() is expression-only; must use the polars_code fallback."""
        data = {"group": ["a", "a", "b", "b", "b"], "value": [1, 2, 3, 4, 5]}
        flow_frame = FlowFrame(data).group_by("group").agg(col("value").implode())
        assert flow_frame.get_node_settings().node_type == "polars_code"
        result = flow_frame.collect().sort("group").with_columns(pl.col("value").list.sort())
        expected = (
            pl.DataFrame(data).group_by("group").agg(pl.col("value").implode())
            .sort("group").with_columns(pl.col("value").list.sort())
        )
        assert_frame_equal(result, expected)

    def test_group_by_multiple_columns(self, sales_data):
        """Test grouping by multiple columns."""
        result = sales_data.group_by(["region", "category"]).sum().collect()

        assert "region" in result.columns
        assert "category" in result.columns
        assert "sales" in result.columns

        assert len(result) <= 12  # 4 regions × 3 categories (not all combinations exist)

        north_a = result.filter((pl.col("region") == "North") & (pl.col("category") == "A"))
        if len(north_a) > 0:
            assert north_a["sales"][0] == 250  # 100 + 150

    def test_group_by_rename_group_keys_count(self, sales_data):
        """Test the alias use in the group by expression"""
        df = sales_data.group_by(col("product").alias("product_name")).len()
        pl_df = sales_data.data.group_by(pl.col("product").alias("product_name")).len()
        assert_frame_equal(df.data, pl_df, check_row_order=False)

    def test_group_by_rename_group_keys_agg(self, sales_data):
        """Test the alias use in the group by expression"""
        df = sales_data.group_by(col("product").alias("product_name")).agg(col("sales").sum())
        pl_df = sales_data.data.group_by(pl.col("product").alias("product_name")).agg(pl.col("sales").sum())
        assert_frame_equal(df.data, pl_df, check_row_order=False)

    def test_group_by_mixed_column_types(self, sales_data):
        """Test grouping with a mix of string columns and expressions."""
        sales_with_date = sales_data.with_columns(
            col("date").cast(pl.Date).alias("order_date")
        )

        result = sales_with_date.with_columns(
            col("order_date").dt.month().alias("month")
        ).group_by(["region", "month"]).agg(
            col("sales").sum().alias("monthly_sales")
        ).collect()

        assert "region" in result.columns
        assert "month" in result.columns
        assert "monthly_sales" in result.columns

        assert len(result["month"].unique()) > 1

    def test_group_by_boolean_column(self, sales_data):
        """Test grouping by a boolean column."""
        flow_frame = sales_data.group_by("is_promoted").sum()
        result = flow_frame.collect()
        expected_data = sales_data.data.group_by("is_promoted").agg(cs.numeric().sum()).collect()
        assert_frame_equal(expected_data, result, check_row_order=False)

    def test_group_by_maintain_order(self, sales_data):
        """Test grouping with maintain_order parameter."""
        result = sales_data.group_by("region", maintain_order=True).count().collect()
        expected_result = sales_data.data.group_by("region", maintain_order=True).count().collect()
        assert_frame_equal(result, expected_result)

    def test_group_by_with_description(self, sales_data):
        """Test grouping with description parameter."""
        gb = sales_data.group_by("region", description="Group sales by region")

        assert hasattr(gb, "description")
        assert gb.description == "Group sales by region"

        result = gb.sum()

        node_settings = result.get_node_settings()
        assert "Group sales by region" in node_settings.setting_input.description

    def test_group_by_readable_group(self, sales_data):
        """Test the readable_group method for string representation."""
        gb = sales_data.group_by(
            "region",
            col("category"),
            col("product").alias("prod")
        )

        readable = gb.readable_group()

        assert "region" in readable
        assert "category" in readable
        assert "prod" in readable or "product" in readable

    def test_group_by_agg_top_level_functions(self, sales_data):
        """Test using top-level aggregation functions in agg()."""
        from flowfile_frame.expr import count, max, mean, min, sum

        flow_frame = sales_data.group_by("region").agg([
            sum("sales").alias("total_sales"),
            mean("units").alias("avg_units"),
            min("returns").alias("min_returns"),
            max("returns").alias("max_returns"),
            count("product").alias("product_count")
        ])
        assert flow_frame.get_node_settings().node_type == 'group_by', 'Should be able to utilize the group by node'
        result = flow_frame.collect()
        expected_data = sales_data.data.group_by("region").agg([
            pl.sum("sales").alias("total_sales"),
            pl.mean("units").alias("avg_units"),
            pl.min("returns").alias("min_returns"),
            pl.max("returns").alias("max_returns"),
            pl.count("product").alias("product_count")
        ]).collect()
        assert_frame_equal(expected_data, result, check_row_order=False)

    def test_group_by_chained_aggregations(self, sales_data):
        """Test chaining operations after group by aggregation."""
        result = (sales_data.group_by("region")
                  .agg(col("sales").sum().alias("total_sales"))
                  .filter(col("total_sales") > 500)
                  .collect())

        assert all(row["total_sales"] > 500 for row in result.iter_rows(named=True))

        result = (sales_data.group_by("region")
                  .agg(col("sales").sum().alias("total_sales"))
                  .sort("total_sales", descending=True)
                  .collect())

        sales_values = result["total_sales"].to_list()
        assert sales_values == sorted(sales_values, reverse=True)


if __name__ == "__main__":
    pytest.main([__file__])
