# Building code with flows

Flowfile's Code Generator allows you to export your visually designed data pipelines as clean, executable Python code. This feature is designed to empower users who wish to inspect the underlying data transformation logic, integrate Flowfile pipelines into existing Python projects, or extend their workflows with custom scripts.

For pure transformation flows (filter, join, group by, etc.), the generated code is standalone Polars — no Flowfile dependency required. Flows that include I/O nodes (database, catalog, cloud storage, Kafka) generate code that uses `import flowfile as ff` for connection-aware operations. The transformation logic remains Polars in both cases.
![code_generator](../../../assets/images/guides/code_generator/code_generator.gif)

## Key Characteristics of the Generated Code

When you generate code from your Flowfile graph, you can expect the output to be:

* **Mostly Standalone**: Transformation logic uses only Polars. I/O operations (database, catalog, cloud storage, Kafka) use `flowfile` for managed connections.
* **Readable**: The structure mirrors your visual flow, making it easy to understand the sequence of operations.
* **Direct Translation**: Transformation nodes translate to Polars operations. I/O nodes translate to FlowFrame API calls (`ff.read_database()`, `ff.read_catalog_table()`, etc.).
* **Ready for Integration**: Pure transformation flows can be embedded anywhere. Flows with I/O nodes require `pip install Flowfile`.

## Examples of Generated Code

Here are some simplified examples illustrating what the generated Polars code looks like for common Flowfile operations. These examples highlight how your visual workflow seamlessly translates into Python.

### Example 1: Reading a CSV and Selecting Columns

This example shows how a pipeline that reads a CSV file and then selects/renames specific columns translates into Polars code.

**Flowfile Pipeline:**

1.  **Read CSV** (e.g., `customers.csv`)
2.  **Select** (e.g., keep `name` as `customer_name`, `age`)

<details markdown="1">
<summary>Generated Polars Code</summary>

```python
# Example 1: Reading a CSV and Selecting Columns
import polars as pl

def run_etl_pipeline():
    """
    ETL Pipeline: Example CSV Read and Select
    Generated from Flowfile
    """
    df_1 = pl.scan_csv("/path/to/your/customers.csv")
    df_2 = df_1.select(
        pl.col("name").alias("customer_name"),
        pl.col("age")
    )
    return df_2

if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
```

</details>

### Example 2: Grouping and Aggregating Data

This example demonstrates the code generated for a pipeline that processes a dataset and performs a group by operation with aggregations.

**Flowfile Pipeline:**

1.  **Manual Input** (sample sales data with `product` and `revenue`)
2.  **Group By** (e.g., group by `product`, sum `revenue` as `total_revenue`)

<details markdown="1">
<summary>Generated Polars Code</summary>

```python
# Example 2: Grouping and Aggregating Data
import polars as pl

def run_etl_pipeline():
    """
    ETL Pipeline: Example Grouping and Aggregating
    Generated from Flowfile
    """
    # Simplified manual input example
    df_1 = pl.LazyFrame(
        {
            "product": ["A", "B", "A", "B", "C"],
            "revenue": [100.0, 200.0, 100.0, 200.0, 150.0],
        }
    )
    df_2 = df_1.group_by(["product"]).agg([
        pl.col("revenue").sum().alias("total_revenue"),
    ])
    return df_2

if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
```

</details>

### Example 3: Custom Polars Code Execution

For advanced users, Flowfile offers a "Polars Code" node where you can write custom Polars expressions. Here's how that custom code is integrated into the generated script.

**Flowfile Pipeline:**

1.  **Manual Input** (a basic DataFrame)
2.  **Polars Code** (a node containing custom Polars logic, e.g., adding a new column)

<details markdown="1">
<summary>Generated Polars Code</summary>

```python
# Example 3: Custom Polars Code Execution
import polars as pl

def run_etl_pipeline():
    """
    ETL Pipeline: Custom Polars Code Example
    Generated from Flowfile
    """
    df_1 = pl.LazyFrame({"value": [1, 2, 3]})

    # Custom Polars code as defined in the Flowfile node
    def _custom_code_node_name(input_df: pl.LazyFrame):
        return input_df.with_columns((pl.col('value') * 10).alias('scaled_value'))

    df_2 = _custom_code_node_name(df_1)
    return df_2

if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
```

</details>

### Example 4: Reading from the Catalog

When a flow includes I/O nodes like a catalog reader, the generated code imports `flowfile` and uses the FlowFrame API for connection-aware operations.

**Flowfile Pipeline:**

1.  **Catalog Reader** (e.g., `sales_data` table from namespace 3)
2.  **Filter** (e.g., keep rows where `amount > 100`)

<details markdown="1">
<summary>Generated Code</summary>

```python
# Example 4: Catalog Read with Filter
import flowfile as ff
import polars as pl

def run_etl_pipeline():
    """
    ETL Pipeline: Catalog Read and Filter
    Generated from Flowfile
    """
    df_1 = ff.read_catalog_table("sales_data", namespace_id=3).data
    df_2 = df_1.filter(pl.col("amount") > 100)
    return df_2

if __name__ == "__main__":
    pipeline_output = run_etl_pipeline()
```

</details>

!!! note "`.data` accessor"
    The generated code calls `.data` on FlowFrame results to extract the underlying Polars `LazyFrame`. This keeps the rest of the pipeline as standard Polars operations.

These examples provide a clear overview of the type of high-quality, executable Python code produced by Flowfile's Code Generator.

!!! info "Future Direction"
    In a future release, generated code will use the FlowFrame API directly for all operations, enabling round-trip editing between code and canvas.