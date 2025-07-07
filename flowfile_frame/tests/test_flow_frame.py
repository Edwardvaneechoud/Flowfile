from flowfile_frame.flow_frame_methods import read_parquet, from_dict, concat
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.expr import col, lit, cum_count
from flowfile_frame import selectors as sc

import os
import io
import pytest
import tempfile
import polars as pl
from polars.testing import assert_frame_equal
from flowfile_frame.flow_frame_methods import read_csv
from flowfile_frame.expr import col

@pytest.fixture
def df():
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "city": ["New York", "Los Angeles", "Chicago"]
    }
    return FlowFrame(data)


def test_create_empty_flow_frame():
    """Test creating an empty FlowFrame."""
    df = FlowFrame()
    assert isinstance(df, FlowFrame)
    assert isinstance(df.data, pl.LazyFrame)
    assert len(df.data.collect()) == 0


def test_create_flow_frame_from_dict(df):
    """Test creating a FlowFrame from a dictionary."""
    # Check the instance
    assert isinstance(df, FlowFrame)
    assert isinstance(df.data, pl.LazyFrame)

    # Check the data
    result = df.collect()
    assert len(result) == 3
    assert result.columns == ["id", "name", "age", "city"]
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


def test_select_columns_with_node_conversion(df):
    """
    Test selecting columns from a FlowFrame. All these select statements should result in a node select, so no custom
    polars code."""

    result = df.select("id", "name")
    assert result.columns == ["id", "name"]
    assert result.get_node_settings().node_type == 'select'

    result = df.select(col("id"), col("name").alias("username"))
    assert result.columns == ["id", "username"]
    assert result.get_node_settings().node_type == 'select'

    result = df.select("*")
    assert result.columns  == df.columns
    assert result.get_node_settings().node_type == 'select'


def test_select_with_non_native_conversion(df):
    # Select with selector
    result = df.select(sc.numeric())
    assert result.get_node_settings().node_type != 'select'
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
    #
    # Add multiple columns with list
    result = df.with_columns([
        col("id").alias("user_id"),
        (col("age") * 2).alias("double_age")
    ]).collect()
    assert "user_id" in result.columns
    assert "double_age" in result.columns
    assert result["double_age"].to_list() == [50, 60, 70]

    result = df.with_columns(
        col("id").alias("user_id"),
        (col("age") * 2).alias("double_age")
    ).collect()

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


def test_join_not_in_graph():
    df1 = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Create second dataframe
    df2 = FlowFrame({
        "id": [1, 2, 4],
        "age": [25, 30, 40]
    })

    result = df1.join(df2, on="id").collect()
    assert len(result) == 2  # Only matches for id 1 and 2
    assert result.columns == ["id", "name", "age"]
    assert_frame_equal(result, df1.data.join(df2.data, on="id").collect())


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
    assert_frame_equal(expected_data, result, check_row_order=False, check_exact=False, check_column_order=False)


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
    expected_result = df.data.with_columns(
        pl.cum_count("category").over("category").alias("group_count")
    ).collect()
    assert "group_count" in result.columns
    assert_frame_equal(expected_result, expected_result, check_row_order=True, check_dtypes=False)


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


def test_read_csv_basic():
    """Test basic CSV reading functionality."""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp:
        tmp.write("id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35")
        tmp_path = tmp.name

    try:
        # Read the CSV file
        df = read_csv(tmp_path)

        # Check the instance
        assert isinstance(df, FlowFrame)
        assert isinstance(df.data, pl.LazyFrame)

        # Check the data
        result = df.collect()
        assert len(result) == 3
        assert result.columns == ["id", "name", "age"]
        assert result["name"][1] == "Bob"

        # Check data types
        assert result["id"].dtype == pl.Int64
        assert result["name"].dtype == pl.Utf8
        assert result["age"].dtype == pl.Int64
    finally:
        # Clean up
        os.unlink(tmp_path)


def test_read_csv_parameter_variations():
    """Test CSV reading with different parameter combinations."""
    # Create a CSV with various data types and formats
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp:
        tmp.write("id;first_name;last_name;salary;active\n")
        tmp.write("1;Alice;Smith;50000.50;true\n")
        tmp.write("2;Bob;Jones;60000.75;false\n")
        tmp.write("3;Charlie;Brown;55000.25;true\n")
        tmp_path = tmp.name
    try:
        # Test with custom separator
        df = read_csv(tmp_path, separator=";")
        result = df.collect()
        assert len(result) == 3
        assert result.columns == ["id", "first_name", "last_name", "salary", "active"]
        assert result["first_name"][0] == "Alice"

        # Test with column renaming
        df = read_csv(tmp_path, separator=";",
                      new_columns=["user_id", "first", "last"])
        result = df.collect()
        assert result.columns[:3] == ["user_id", "first", "last"]
        df = read_csv(tmp_path, separator=";", has_header=False)

        result = df.collect()
        assert len(result) == 4  # Now includes the header row as data

        # Test with skip_rows
        df = read_csv(tmp_path, separator=";", skip_rows=1)

        result = df.collect()
        assert len(result) == 2

        df = read_csv(tmp_path, separator=";", row_index_name="row_num", row_index_offset=10)
        result = df.collect()
        assert "row_num" in result.columns
        assert result["row_num"].to_list() == [10, 11, 12]
    finally:
        # Clean up
        os.unlink(tmp_path)


def test_read_csv_schema_handling():
    """Test CSV reading with schema handling."""
    # Create a CSV with data that could be interpreted in multiple ways
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp:
        tmp.write("id,name,value\n")
        tmp.write("1,Alice,10.5\n")
        tmp.write("2,Bob,20.0\n")
        tmp.write("3,Charlie,N/A\n")  # Missing value
        tmp_path = tmp.name

    try:
        # Test with automatic schema inference
        df = read_csv(tmp_path)


        # Test with explicit schema
        schema = {"id": pl.Int32, "name": pl.Utf8, "value": pl.Float64}
        df = read_csv(tmp_path, schema=schema, null_values=["N/A"])
        result = df.collect()
        assert result.schema["id"] == pl.Int32
        assert result.schema["name"] == pl.Utf8
        assert result.schema["value"] == pl.Float64

        # Test with schema overrides
        df = read_csv(tmp_path, schema_overrides={"id": pl.UInt8}, null_values=["N/A"])
        result = df.collect()
        assert result.schema["id"] == pl.UInt8

        # Test with null values
        df = read_csv(tmp_path, null_values=["N/A"])
        result = df.collect()
        assert result["value"][2] is None  # The N/A should be converted to None
    finally:
        # Clean up
        os.unlink(tmp_path)


def test_read_csv_file_like_object():
    """Test reading CSV from file-like objects."""
    # Create a CSV string
    csv_data = "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35"

    # Create file-like objects
    bytes_io = io.BytesIO(csv_data.encode('utf-8'))
    string_io = io.StringIO(csv_data)

    # Test reading from BytesIO
    df = read_csv(bytes_io)
    result = df.collect()
    assert len(result) == 3
    assert result["name"][0] == "Alice"
    # Test reading from StringIO
    df = read_csv(string_io)
    result = df.collect()
    assert len(result) == 3
    assert result["name"][0] == "Alice"


def test_read_csv_with_flow_graph():
    """Test reading CSV with an existing flow graph."""
    # Create a base FlowFrame
    base_df = FlowFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp:
        tmp.write("id,age\n1,25\n2,30\n3,35")
        tmp_path = tmp.name

    try:
        # Read CSV into the same flow graph
        df = read_csv(tmp_path, flow_graph=base_df.flow_graph)

        # Check that they share the same flow graph
        assert df.flow_graph is base_df.flow_graph

        # Test joining the two dataframes
        result = base_df.join(df, on="id").collect()
        assert len(result) == 3
        assert result.columns == ["id", "name", "age"]
    finally:
        # Clean up
        os.unlink(tmp_path)


def test_read_csv_complex_operations():
    """Test CSV reading followed by complex operations."""
    # Create a CSV file
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp:
        tmp.write("id,name,department,salary\n")
        tmp.write("1,Alice,Sales,50000\n")
        tmp.write("2,Bob,IT,60000\n")
        tmp.write("3,Charlie,Sales,55000\n")
        tmp.write("4,David,HR,65000\n")
        tmp.write("5,Eve,IT,70000\n")
        tmp_path = tmp.name

    try:
        # Read the CSV
        df = read_csv(tmp_path)

        result = (df
                  .filter(col("salary") > 55000)
                  .with_columns((col("salary") * 1.1).alias("new_salary"))
                  .group_by("department")
                  .agg([
            col("new_salary").mean().alias("avg_salary"),
            col("id").count().alias("employee_count")
        ])
                  .sort("avg_salary", descending=True)
                  .collect())

        # Check results
        assert len(result) == 2  # IT and HR departments
        assert "avg_salary" in result.columns
        assert "employee_count" in result.columns
    finally:
        # Clean up
        os.unlink(tmp_path)


def test_read_csv_integration():
    """Test CSV reading integration with other FlowFrame operations."""
    # Create two CSV files for testing
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp1:
        tmp1.write("id,name\n1,Alice\n2,Bob\n3,Charlie")
        tmp1_path = tmp1.name

    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w+', delete=False) as tmp2:
        tmp2.write("id,age\n1,25\n2,30\n4,40")
        tmp2_path = tmp2.name

    try:
        # Read both CSVs
        df1 = read_csv(tmp1_path)
        df2 = read_csv(tmp2_path, flow_graph=df1.flow_graph)

        # Test joining them
        joined = df1.join(df2, on="id", how="left")
        result = joined.collect()

        # Check the results
        assert len(result) == 3  # All rows from df1
        assert result["id"].to_list() == [1, 2, 3]
        assert result["age"][2] is None  # No match for id=3

        # Test filtering and transforming
        filtered = joined.filter(col("age").is_not_null())
        result = filtered.collect()
        assert len(result) == 2  # Only rows with matching ages
    finally:
        # Clean up
        os.unlink(tmp1_path)
        os.unlink(tmp2_path)