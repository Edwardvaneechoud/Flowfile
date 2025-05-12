# FlowFile Frame: Polars-Like API for Building ETL Graphs

FlowFile Frame is a Python library that provides a familiar Polars-like API for data manipulation, while simultaneously building an ETL (Extract, Transform, Load) graph under the hood. This allows you to:

1. Write data transformation code using a simple, Pandas/Polars-like API
2. Automatically generate executable ETL workflows
3. Visualize, save, and share your data pipelines
4. Get the performance benefits of Polars with the traceability of ETL graphs

## Installation

```bash
pip install flowfile-frame
```

## Quick Start

```python
import flowfile_frame as ff

# Create a dataframe from a dictionary
df = ff.from_dict({
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 35, 28, 42, 31],
    "salary": [50000, 60000, 55000, 75000, 65000]
})

# Basic transformations
filtered_df = df.filter(ff.col("age") > 30)
result = filtered_df.with_columns(
    (ff.col("salary") * 1.1).alias("new_salary")
)

# Save the resulting ETL graph
result.save_graph("my_pipeline.flowfile")

# Execute and get data
result_data = result.collect()
```

## Key Features

- **Familiar API**: Based on Polars, making it easy to learn if you know Pandas or Polars
- **ETL Graph Generation**: Automatically builds a directed acyclic graph of your data operations
- **Lazy Evaluation**: Operations are not executed until needed
- **Visual Workflow**: Visualize your data transformation pipeline
- **High Performance**: Leverages Polars for fast data processing
- **Reproducible**: Save and share your data transformation workflows

## Core Components

### FlowFrame

The main data structure, similar to a Polars DataFrame, but with ETL graph tracking:

```python
import flowfile_frame as ff

# Create from a dictionary
df = ff.from_dict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 35, 28]
})

# Chain operations
result = df.filter(ff.col("age") > 30) \
           .with_columns((ff.col("age") * 2).alias("double_age")) \
           .sort("name", descending=True)
```

### Expressions

Use expressions to define transformations:

```python
import flowfile_frame as ff
from flowfile_frame import col, lit, when

# Sample data
df = ff.from_dict({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [15, 25, 35],
    "city": ["New York", "London", "Paris"]
})

# Basic column reference
col_expr = col("age")

# Arithmetic
age_plus_10 = col("age") + 10

# Comparison
is_adult = col("age") >= 18

# Conditional logic
status = when(col("age") < 18).then("Minor").otherwise("Adult")

# String operations
uppercase_name = col("name").str.to_uppercase()

# Apply these expressions
result = df.with_columns([
    (col("age") + 10).alias("age_plus_10"),
    (col("age") >= 18).alias("is_adult"),
    uppercase_name.alias("name_upper"),
    status.alias("status")
])

# Custom formulas
df_with_formula = df.with_columns(
    flowfile_formulas=["[age] * 2"], 
    output_column_names=["double_age"]
)
```

### Aggregations

Perform grouping and aggregation operations:

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data
df = ff.from_dict({
    "category": ["A", "B", "A", "B", "A", "C"],
    "sales": [100, 200, 150, 250, 300, 400],
    "quantity": [10, 20, 15, 25, 30, 40]
})

# Group by and calculate aggregates
result = df.group_by("category").agg([
    col("sales").sum().alias("total_sales"),
    col("sales").mean().alias("avg_sales"),
    col("quantity").sum().alias("total_quantity")
])

# Or use convenience methods
sum_by_category = df.group_by("category").sum()
```

## Common Operations

### Reading Data

```python
import flowfile_frame as ff

# From dictionary
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 35, 28, 42, 31]
})

# You can also read from files
# df_csv = ff.read_csv("data.csv", separator=";")
# df_parquet = ff.read_parquet("data.parquet")
```

### Filtering

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data
df = ff.from_dict({
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 35, 28, 42, 31],
    "subscription": ["basic", "premium", "premium", "basic", "premium"],
    "active": [True, True, False, True, False]
})

# Filter with expressions
adults = df.filter(col("age") >= 30)
premium = df.filter((col("subscription") == "premium") & (col("active") == True))

# Filter with formula strings
active_users = df.filter(flowfile_formula="[active] == True")
```

### Selecting and Transforming Columns

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data
df = ff.from_dict({
    "first_name": ["Alice", "Bob", "Charlie"],
    "last_name": ["Smith", "Jones", "Brown"],
    "age": [25, 35, 45],
    "email": ["ALICE@example.com", "bob@example.com", "charlie@example.com"],
    "price": [10.50, 20.75, 15.25],
    "quantity": [2, 3, 1],
    "date": ["2023-01-15", "2023-02-20", "2023-03-25"]
})

# Select specific columns
subset = df.select("first_name", "age", "email")

# Select with expressions
subset = df.select(
    col("first_name").alias("name"),
    (col("age") + 1).alias("next_year_age"),
    col("email").str.to_lowercase()
)

# Add new columns
df_with_cols = df.with_columns([
    (col("price") * col("quantity")).alias("total"),
    col("date").cast(ff.Date).dt.year().alias("year")
])
```

### Sorting

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data
df = ff.from_dict({
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 35, 28, 42, 31],
    "category": ["A", "B", "A", "C", "B"]
})

# Sort by one column
sorted_by_age = df.sort("age")

# Sort by multiple columns
sorted_by_category_age = df.sort(by=["category", "age"], descending=[False, True])

# Sort with expressions
sorted_by_name_desc = df.sort(by=col("name"), descending=True)
```

### Joins

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data - customers
customers = ff.from_dict({
    "customer_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "country": ["USA", "Canada", "UK", "Australia", "France"]
})

# Sample data - orders
orders = ff.from_dict({
    "order_id": [101, 102, 103, 104, 105, 106],
    "customer_id": [1, 2, 3, 1, 2, 6],
    "amount": [150.50, 200.75, 300.25, 120.80, 85.60, 500.00]
})

# Simple join
result = customers.join(orders, on="customer_id")

# Join with different columns
result = customers.join(
    orders, 
    left_on="customer_id", 
    right_on="customer_id"
)

# Different join types
left_join = customers.join(orders, on="customer_id", how="left")
outer_join = customers.join(orders, on="customer_id", how="outer")
```

### Grouping and Aggregation

```python
import flowfile_frame as ff
from flowfile_frame import col, sum, mean, count

# Sample data
df = ff.from_dict({
    "region": ["North", "South", "North", "South", "East", "West"],
    "category": ["A", "B", "A", "B", "C", "A"],
    "sales": [100, 200, 150, 300, 250, 400],
    "quantity": [10, 20, 15, 30, 25, 40],
    "date": ["2023-01-15", "2023-02-10", "2023-03-05", "2023-04-20", "2023-05-15", "2023-06-10"]
})

# Group by and aggregate
sales_by_region = df.group_by("region").agg([
    col("sales").sum().alias("total_sales"),
    col("sales").mean().alias("avg_sales"),
    col("quantity").count().alias("num_orders")
])

# Alternative using top-level functions
sales_by_region = df.group_by("region").agg([
    sum("sales").alias("total_sales"),
    mean("sales").alias("avg_sales"),
    count("quantity").alias("num_orders")
])

# Simplified aggregation
count_by_region = df.group_by("region").count()
sum_by_region = df.group_by("region").sum()
```

### Pivoting and Reshaping

```python
import flowfile_frame as ff

# Sample data in long format
df_long = ff.from_dict({
    "id": [1, 1, 2, 2],
    "metric": ["sales", "profit", "sales", "profit"],
    "value": [100, 20, 150, 30]
})

# Pivot from long to wide format
pivot_result = df_long.pivot(
    on="metric",
    index="id",
    values="value"
)

# Sample data in wide format
df_wide = ff.from_dict({
    "product": ["A", "B", "C"],
    "sales_2021": [100, 150, 200],
    "sales_2022": [120, 170, 220]
})

# Unpivot from wide to long format
unpivot_result = df_wide.unpivot(
    on=["sales_2021", "sales_2022"],
    index="product"
)
```

### Writing Data

```python
import flowfile_frame as ff

# Sample data
df = ff.from_dict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 35, 28]
})

# Write to CSV
df.write_csv("output.csv", separator=";")

# Write to Parquet
df.write_parquet("output.parquet")
```

## Working with ETL Graphs

```python
import flowfile_frame as ff
from flowfile_frame.utils import open_graph_in_editor

# Sample data
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 35, 28, 42, 31],
    "salary": [50000, 60000, 55000, 75000, 65000]
})

# Create pipeline
pipeline = df.filter(ff.col("age") > 30) \
             .with_columns((ff.col("salary") * 1.1).alias("new_salary")) \
             .sort("new_salary", descending=True)

# Save the ETL graph
pipeline.save_graph("my_pipeline.flowfile")

# View the graph in the Flowfile editor (if installed)
open_graph_in_editor(pipeline.flow_graph, "my_pipeline.flowfile")
```

## Advanced Features

### Window Functions

```python
import flowfile_frame as ff
from flowfile_frame import col, cum_count

# Sample data
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "B", "A"],
    "value": [100, 200, 150, 250, 300]
})

# Simple cumulative count
result = df.with_columns(
    cum_count("id").alias("row_number")
)

# Partitioned cumulative count
result = df.with_columns(
    cum_count("id").over("category").alias("category_row_number")
)

# Running sum and more complex window operations
result = df.with_columns(
    col("value").sum().over("category").alias("category_total"),
    col("value").mean().over("category").alias("category_avg")
)
```

### Exploding Lists

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data with nested lists
df = ff.from_dict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "tags": [["python", "data"], ["sql", "analytics", "python"], ["data", "visualization"]]
})

# Explode the tags list to multiple rows
exploded = df.explode("tags")

# Alternatively use text_to_rows for string splitting
text_df = ff.from_dict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "skills": ["python,sql", "js,python,react", "sql,tableau"]
})

# Split the comma-separated skills into rows
skills_rows = text_df.text_to_rows(
    column="skills",
    delimiter=",",
    output_column="skill"
)
```

### Unique Values

```python
import flowfile_frame as ff

# Sample data with duplicates
df = ff.from_dict({
    "id": [1, 2, 1, 3, 2],
    "category": ["A", "B", "A", "C", "B"]
})

# Get unique combinations
unique_rows = df.unique()

# Get unique by specific columns
unique_categories = df.unique(subset="category")

# Control which duplicate rows to keep
first_occurrence = df.unique(keep="first")
last_occurrence = df.unique(keep="last")
```

### Row Operations

```python
import flowfile_frame as ff
from flowfile_frame import col

# Sample data
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"]
})

# Add row indices
df_with_index = df.with_row_index(name="row_num", offset=1)

# Head/limit to get first n rows
first_3 = df.head(3)  # or df.limit(3)

# Cache results for performance
cached_df = df.cache()
```

### Complex Workflows

```python
import flowfile_frame as ff
from flowfile_frame import col, when

# Sample data
sales_data = ff.from_dict({
    "date": ["2023-01-15", "2023-01-20", "2023-02-10", "2023-02-15", "2023-03-05"],
    "region": ["East", "West", "East", "West", "South"],
    "channel": ["Online", "Store", "Online", "Store", "Online"],
    "sales": [1200, 950, 1350, 1100, 1500],
    "units": [24, 19, 27, 22, 30],
    "cost": [600, 475, 675, 550, 750]
})

# Create a comprehensive analysis pipeline
analysis = (
    sales_data
    # Pre-processing
    .with_columns([
        # Convert date to proper type
        col("date").cast(ff.Date).alias("order_date"),
        # Calculate profit
        (col("sales") - col("cost")).alias("profit"),
        # Calculate unit price
        (col("sales") / col("units")).alias("unit_price"),
        # Categorize sales
        when(col("sales") > 1300)
            .then("High")
            .otherwise(when(col("sales") < 1000).then("Low").otherwise("Medium"))
            .alias("sales_category")
    ])
    # Filter to more recent sales
    .filter(col("date").cast(ff.Date) >= "2023-02-01")
    # Group by region and channel
    .group_by(["region", "channel"])
    .agg([
        col("sales").sum().alias("total_sales"),
        col("profit").mean().alias("avg_profit"),
        col("units").sum().alias("total_units")
    ])
    # Sort by total sales descending
    .sort("total_sales", descending=True)
)

# Execute and view the results
result = analysis.collect()
```

## Limitations

- FlowFile Frame is primarily designed for analytics and ETL workflows, not for low-latency applications
- Some very complex operations may fall back to Polars code execution rather than native graph nodes
- The underlying ETL graph requires more memory than pure Polars operations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License

## Credits

FlowFile Frame is built on top of [Polars](https://github.com/pola-rs/polars) and integrates with the FlowFile ETL system.