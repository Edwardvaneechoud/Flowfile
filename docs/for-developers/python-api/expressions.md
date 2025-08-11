# Expressions

Column expressions for data transformations. Flowfile follows the Polars expressions API, with additional features for Flowfile-formula syntax.

## Flowfile formula syntax
Flowfile supports a simplified formula syntax for expressions, allowing you to use bracket notation for column references. This will render nicely in the Flowfile UI and was implemented to decrease the learning curve for users coming from Excel, PowerBI, Alteryx and Tableau.
You can try it out here: [Flowfile Formula Playground](https://polars-expr-transformer-playground-whuwbghlymon84t5ciewp3.streamlit.app/).

For example, instead of using `ff.col("price")`, you can use `[price]` in supported operations. Operations that support this syntax are documented below.

- ff.with_columns
    You can use flowfile_formulas: Optional[List[str]] = None, output_column_names: Optional[List[str]] = None, to create new columns based on Flowfile-formula syntax. The number of columns in `output_column_names` must match the number of formulas in `flowfile_formulas`.
- ff.filter
    You can use the flowfile_formula parameter to filter rows based on Flowfile Formula syntax.


## Column References

```python
import flowfile as ff

# Polars style
ff.col("price")

# Flowfile formula syntax (in supported operations)
"[price]"  # Equivalent to ff.col("price")
```

### Example Flowfile formula syntax
```python
import flowfile as ff

df = ff.FlowFrame({
    "product": ["Widget", "Gadget", "Tool"],
    "price": [10.50, 25.00, 8.75],
    "quantity": [100, 50, 200],
    "discount": [0.1, 0.15, 0.05]
})

# Using Flowfile formula syntax (Excel-like)
result = df.with_columns(
    flowfile_formulas=[
        "[price] * [quantity]",                    # Simple multiplication
        "[price] * (1 - [discount])",              # With parentheses
        "if [quantity] > 75 then 'High' else 'Low' endif",      # Conditional
        "round([price] * [discount], 2)"
    ],
    output_column_names=["revenue", "discounted_price", "volume_category", "discount_amount"],
    description="Calculate derived metrics"
)

# Mix formulas with regular Polars expressions
output = df.with_columns([
    ff.col("price").round(0).alias("price_rounded")  # Polars style
]).with_columns(
    flowfile_formulas=["[price_rounded] * [quantity]"],  # Formula style
    output_column_names=["estimated_revenue"]
)

```

## Arithmetic Operations

```python
# Standard expressions
df = df.with_columns([
    (ff.col("price") * ff.col("quantity")).alias("revenue"),
    (ff.col("price") * 1.1).alias("price_with_tax"),
    (ff.col("total") / ff.col("count")).alias("average")
])

# Formula syntax
df = df.with_columns(
    flowfile_formulas=[
        "[price] * [quantity]",
        "[price] * 1.1",
        "[total] / [count]"
    ],
    output_column_names=["revenue", "price_with_tax", "average"]
)
```

## String Operations

```python
df = df.with_columns([
    ff.col("name").str.to_uppercase().alias("name_upper"),
    ff.col("code").str.slice(0, 3).alias("prefix"),
    ff.col("text").str.contains("pattern").alias("has_pattern")
])
```

## Conditional Logic

```python
# When/then/otherwise
df = df.with_columns([
    ff.when(ff.col("price") > 100)
    .then(ff.lit("Premium"))
    .when(ff.col("price") > 50)
    .then(ff.lit("Standard"))
    .otherwise(ff.lit("Budget"))
    .alias("tier")
])
```

## Date Operations

```python
df = df.with_columns([
    ff.col("date").dt.year().alias("year"),
    ff.col("date").dt.month().alias("month"),
    ff.col("date").dt.day().alias("day"),
    ff.col("date").dt.weekday().alias("weekday")
])
```

## List Operations

```python
df = df.with_columns([
    ff.col("tags").list.len().alias("tag_count"),
    ff.col("values").list.sum().alias("total"),
    ff.col("items").list.first().alias("first_item")
])
```

!!! tip "Formula Syntax"
    Use `[column_name]` in formula strings for simpler syntax when supported by the operation.


---
[← Previous: FlowFile Operations](flowframe-operations.md) | [Next: Aggregations →](aggregations.md)
