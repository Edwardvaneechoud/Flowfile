import os
import pytest
import polars as pl
from polars.testing import assert_frame_equal
from flowfile_frame.flow_frame import FlowFrame, read_parquet, from_dict, concat
from flowfile_frame.expr import col, lit, cum_count
from flowfile_frame import selectors as sc
from flowfile_frame.utils import open_graph_in_editor


def test_create_empty_flow_frame():
    """Test creating an empty FlowFrame."""
    df = FlowFrame()
    assert isinstance(df, FlowFrame)
    assert isinstance(df.data, pl.LazyFrame)
    assert len(df.data.collect()) == 0


def test_create_flow_frame_from_dict():
    """Test creating a FlowFrame from a dictionary."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    }
    df = FlowFrame(data)

    # Check the instance
    assert isinstance(df, FlowFrame)
    assert isinstance(df.data, pl.LazyFrame)

    # Check the data
    result = df.collect()
    assert len(result) == 3
    assert result.columns == ["id", "name", "age"]
    assert result["name"][1] == "Bob"


def test_from_dict_factory():
    """Test the from_dict factory method."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    }
    df = from_dict(data, description="Test data")

    # Check the instance
    assert isinstance(df, FlowFrame)
    assert isinstance(df.data, pl.LazyFrame)

    # Check the data
    result = df.collect()
    assert len(result) == 3
    assert result.columns == ["id", "name"]

    # Check that the description was set
    assert "Test data" in df.get_node_settings().setting_input.description


def test_select_columns():
    """Test selecting columns from a FlowFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "city": ["New York", "Los Angeles", "Chicago"]
    }
    df = FlowFrame(data)

    # Select specific columns
    result = df.select("id", "name")
    assert result.columns == ["id", "name"]

    # Select with expressions
    result = df.select(col("id"), col("name").alias("username")).collect()
    assert result.columns == ["id", "username"]

    # Select with selector
    result = df.select(sc.numeric()).collect()
    assert result.columns == ["id", "age"]


def test_filter():
    """Test filtering rows in a FlowFrame."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45]
    }
    df = FlowFrame(data)

    # Filter with expression
    result = df.filter(col("age") > 30).collect()
    assert len(result) == 3
    assert result["name"].to_list() == ["Charlie", "David", "Eve"]

    # Filter with formula
    result = df.filter(flowfile_formula="[age] > 40").collect()
    assert len(result) == 1
    assert result["name"][0] == "Eve"


def test_sort():
    """Test sorting a FlowFrame."""
    data = {
        "id": [3, 1, 4, 5, 2],
        "name": ["Charlie", "Alice", "David", "Eve", "Bob"],
        "age": [35, 25, 40, 45, 30]
    }
    df = FlowFrame(data)

    # Sort by a single column
    result = df.sort("id").collect()
    assert result["id"].to_list() == [1, 2, 3, 4, 5]

    # Sort by multiple columns
    result = df.sort(by=["age", "name"], descending=[True, False]).collect()
    assert result["name"].to_list() == ["Eve", "David", "Charlie", "Bob", "Alice"]

    # Sort with expressions
    result = df.sort(by=col("name"), descending=True).collect()
    assert result["name"].to_list() == ["Eve", "David", "Charlie", "Bob", "Alice"]


def test_with_columns():
    """Test adding new columns to a FlowFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    }
    df = FlowFrame(data)

    # Add a constant column
    result = df.with_columns(lit(True).alias("is_active")).collect()
    assert "is_active" in result.columns
    assert result["is_active"].to_list() == [True, True, True]

    # Add a derived column
    result = df.with_columns(col("age").cast(pl.Float32).alias("age_float")).collect()
    assert "age_float" in result.columns

    # Add multiple columns
    result = df.with_columns([
        col("id").alias("user_id"),
        (col("age") * 2).alias("double_age")
    ]).collect()
    assert "user_id" in result.columns
    assert "double_age" in result.columns
    assert result["double_age"].to_list() == [50, 60, 70]

    # Add with flowfile formula
    result = df.with_columns(
        flowfile_formulas=["[age] * 2"],
        output_column_names=["double_age"]
    ).collect()
    assert "double_age" in result.columns
    assert result["double_age"].to_list() == [50, 60, 70]


def test_with_row_index():
    """Test adding row indices to a FlowFrame."""
    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    }
    df = FlowFrame(data)

    # Add default index
    result = df.with_row_index().collect()
    assert "index" in result.columns
    assert result["index"].to_list() == [0, 1, 2]

    # Add custom index
    result = df.with_row_index(name="row_num", offset=1).collect()
    assert "row_num" in result.columns
    assert result["row_num"].to_list() == [1, 2, 3]


def group_by():
    """Test grouping operations in a FlowFrame."""
    data = {
        "category": ["A", "B", "A", "B", "A"],
        "value": [10, 20, 30, 40, 50]
    }
    df = FlowFrame(data)

    # Group by category and get count
    result = df.group_by("category").count().collect()
    assert len(result.collect()) == 2
    assert set(result["category"].to_list()) == {"A", "B"}
    assert result["count"].to_list() == [3, 2] if result["category"][0] == "A" else [2, 3]

    # Group by category and aggregate
    result = df.group_by("category").agg(col("value").sum().alias("total_value")).collect()
    assert len(result) == 2
    expected_result = pl.DataFrame({'category': ['A', 'B'], 'total_value': [90, 60]})
    assert expected_result.equals(result)

    # Test multiple aggregations
    result = df.group_by("category").agg(
        col("value").sum().alias("total_value"),
        col("value").mean().alias("avg_value")
    ).collect()
    assert "total_value" in result.columns
    assert "avg_value" in result.columns
    expected_result = pl.DataFrame({'category': ['A', 'B'], 'total_value': [90, 60], 'avg_value': [30.0, 30.0]})
    assert expected_result.equals(result)


def test_join():
    """Test joining FlowFrames."""
    # Create main dataframe
    df1 = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Create second dataframe
    df2 = FlowFrame({
        "id": [1, 2, 4],
        "age": [25, 30, 40]
    }, flow_graph=df1.flow_graph)

    # Test inner join
    result = df1.join(df2, on="id").collect()
    assert len(result) == 2  # Only matches for id 1 and 2
    assert result.columns == ["id", "name", "age"]

    # Test left join
    result = df1.join(df2, on="id", how="left").collect()
    assert len(result) == 3  # All rows from df1
    assert result["name"].to_list() == ["Alice", "Bob", "Charlie"]

    # Test join with different column names
    df3 = FlowFrame({
        "user_id": [1, 2, 4],
        "city": ["New York", "Los Angeles", "Chicago"]
    }, flow_graph=df1.flow_graph)

    result = df1.join(df3, left_on="id", right_on="user_id").collect()
    assert len(result) == 2
    assert "city" in result.columns


def test_explode():
    """Test exploding list columns."""
    data = {
        "id": [1, 2],
        "values": [[10, 20], [30, 40, 50]]
    }
    df = FlowFrame(data)

    result = df.explode("values").collect()
    assert len(result) == 5  # 2 values for first row, 3 for second
    assert result["values"].to_list() == [10, 20, 30, 40, 50]
    assert result["id"].to_list() == [1, 1, 2, 2, 2]


def test_unique():
    """Test getting unique rows from a FlowFrame."""
    data = {
        "category": ["A", "B", "A", "B", "A"],
        "value": [10, 20, 10, 20, 30]
    }
    df = FlowFrame(data)

    # Get unique combinations
    result = df.unique().collect()
    assert len(result) == 3  # (A,10), (B,20), (A,30)

    # Get unique by subset of columns
    result = df.unique(subset="category").collect()
    assert len(result) == 2  # A, B

    # Test with different keep strategies
    result = df.unique(subset="category", keep="first").collect()
    assert len(result) == 2
    assert result["value"].to_list() == [10, 20] if result["category"][0] == "A" else [20, 10]


def test_pivot():
    """Test pivot operations."""
    # Create test data in long format
    data = {
        "id": [1, 1, 2, 2],
        "metric": ["sales", "profit", "sales", "profit"],
        "value": [100, 20, 150, 30]
    }
    df = FlowFrame(data)

    # Pivot the data to wide format
    result = df.pivot(
        on="metric",
        index="id",
        values="value"
    ).collect()

    expected_data = pl.DataFrame({'id': [2, 1], 'profit': [30, 20], 'sales': [150, 100]})
    assert_frame_equal(expected_data, result, check_row_order=False, check_exact=False)


def test_unpivot():
    """Test unpivot operations."""
    # Create test data in wide format
    data = {
        "id": [1, 2],
        "sales_2021": [100, 150],
        "sales_2022": [120, 170]
    }
    df = FlowFrame(data)

    # Unpivot the data to long format
    result = df.unpivot(
        on=["sales_2021", "sales_2022"],
        index="id"
    ).collect()
    expected_data = pl.DataFrame({'id': [1, 2, 1, 2],
                                  'variable': ['sales_2021', 'sales_2021', 'sales_2022', 'sales_2022'],
                                  'value': [100, 150, 120, 170]})
    assert_frame_equal(expected_data, result, check_row_order=False, check_exact=False)


def test_concat():
    """Test concatenating FlowFrames."""
    df1 = FlowFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"]
    })

    df2 = FlowFrame({
        "id": [3, 4],
        "name": ["Charlie", "David"]
    }, flow_graph=df1.flow_graph)

    # Vertical concat (union)
    result = df1.concat(df2).collect()
    assert len(result) == 4
    assert result["id"].to_list() == [1, 2, 3, 4]
    assert result["name"].to_list() == ["Alice", "Bob", "Charlie", "David"]

    # Test concat function
    df3 = FlowFrame({
        "id": [5],
        "name": ["Eve"]
    }, flow_graph=df1.flow_graph)

    result = concat([df1, df2, df3]).collect()
    assert len(result) == 5
    assert result["id"].to_list() == [1, 2, 3, 4, 5]


@pytest.mark.skip(reason="Parquet file not available in test environment")
def test_write_read_parquet(tmpdir):
    """Test writing and reading Parquet files."""
    df = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Define a temporary path
    temp_path = os.path.join(tmpdir, "test_data.parquet")

    # Write to Parquet
    df.write_parquet(temp_path)

    # Read from Parquet
    df2 = read_parquet(temp_path, flow_graph=df.flow_graph)

    # Check the data
    result = df2.collect()
    assert len(result) == 3
    assert result.columns == ["id", "name"]
    assert result["name"].to_list() == ["Alice", "Bob", "Charlie"]


def test_save_flow_graph(tmpdir):
    """Test saving the flow graph to a file."""
    df = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Add some operations to create a more complex graph
    df2 = df.select("id", "name").filter(col("id") > 1)

    # Define a temporary path
    temp_path = os.path.join(tmpdir, "test_flow.flowfile")

    # Save the graph
    df2.save_graph(temp_path)

    # Just check that the file exists
    assert os.path.exists(temp_path)


def test_head_limit():
    """Test head/limit operations."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"]
    }
    df = FlowFrame(data)

    # Test head
    result = df.head(3).collect()
    assert len(result) == 3
    assert result["id"].to_list() == [1, 2, 3]

    # Test limit (alias for head)
    result = df.limit(2).collect()
    assert len(result) == 2
    assert result["id"].to_list() == [1, 2]


def test_complex_workflow():
    """Test a more complex workflow combining multiple operations."""
    # Start with raw data
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "department": ["Sales", "IT", "Sales", "HR", "IT"],
        "salary": [50000, 60000, 55000, 65000, 70000]
    }
    df = FlowFrame(data)

    # Build a workflow: filter → transform → group → sort
    result = (df
              .filter(col("age") > 30)  # Keep employees older than 30
              .with_columns((col("salary") * 1.1).alias("new_salary"))  # Add 10% salary
              .group_by("department")  # Group by department
              .agg([
                    col("new_salary").mean().alias("avg_salary"),
                    col("age").mean().alias("avg_age")
                    ])  # Calculate averages
              .sort("avg_salary", descending=True)  # Sort by average salary
              )

    #  Just use the actual implementation of polars
    expected_result = (df.data.filter(pl.col("age") > 30)  # Keep employees older than 30
                       .with_columns((pl.col("salary") * 1.1).alias("new_salary"))  # Add 10% salary
                       .group_by("department")  # Group by department
                       .agg([
                            pl.col("new_salary").mean().alias("avg_salary"),
                            pl.col("age").mean().alias("avg_age")
                            ])  # Calculate averages
                       .sort("avg_salary", descending=True)  # Sort by average salary
                       .collect())
    assert_frame_equal(result.collect(), expected_result, check_row_order=False)


def test_cache():
    """Test caching a FlowFrame."""
    df = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Cache the dataframe
    df_cached = df.cache()

    # Check that it's the same instance
    assert df_cached is df

    # Check that caching is enabled in the node settings
    node_settings = df.get_node_settings()
    assert node_settings.setting_input.cache_results is True


def test_cum_count():
    """Test cumulative count operations."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "category": ["A", "B", "A", "B", "A"]
    }
    df = FlowFrame(data)

    # Test simple cumulative count
    result = df.with_columns(
        cum_count("category").alias("count")
    ).collect()
    assert "count" in result.columns
    assert result["count"].to_list() == [1, 2, 3, 4, 5]

    # Test grouped cumulative count
    result = df.with_columns(
        cum_count("category").over("category").alias("group_count")
    ).collect()
    assert "group_count" in result.columns
    expected_result = pl.DataFrame({'group_count': [1, 1, 2, 2, 3], 'id': [1, 2, 3, 4, 5], 'category': ['A', 'B', 'A', 'B', 'A']})
    assert_frame_equal(expected_result, result, check_row_order=True, check_dtypes=False)


def test_schema():
    """Test schema-related operations."""
    df = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "active": [True, False, True]
    })

    # Get schema
    schema = df.schema
    assert isinstance(schema, dict)
    assert set(schema.keys()) == {"id", "name", "active"}
    assert str(schema["id"]) == "Int64"
    assert str(schema["name"]) == "String"
    assert str(schema["active"]) == "Boolean"
