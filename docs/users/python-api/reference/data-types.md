
# Data Types

Flowfile supports all Polars data types. This page covers the most commonly used types and conversions.

## Supported Types

| Type | Description | Example |
|------|-------------|---------|
| `Int8`, `Int16`, `Int32`, `Int64` | Signed integers | `123` |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | Unsigned integers | `456` |
| `Float32`, `Float64` | Floating point | `12.34` |
| `Boolean` | True/False values | `True` |
| `Utf8` / `String` | Text data | `"hello"` |
| `Date` | Date without time | `2024-01-15` |
| `Datetime` | Date with time | `2024-01-15 14:30:00` |
| `Time` | Time without date | `14:30:00` |
| `Duration` | Time delta | `2 days` |
| `List` | Nested arrays | `[1, 2, 3]` |
| `Struct` | Nested objects | `{"a": 1, "b": 2}` |

## Type Casting

```python
import flowfile as ff

df = ff.FlowFrame({
    "int_col": [1, 2, 3],
    "str_col": ["10", "20", "30"],
    "date_str": ["2024-01-01", "2024-01-02", "2024-01-03"]
})

# Cast types
df = df.with_columns([
    ff.col("int_col").cast(ff.Float64).alias("float_col"),
    ff.col("str_col").cast(ff.Int32).alias("parsed_int"),
    ff.col("date_str").str.strptime(ff.Date, "%Y-%m-%d").alias("date_col")
])
```

## Schema Inspection

```python
# Get schema without processing data
print(df.schema)
# [Column(name='int_col', dtype=Int64), ...]

# Check specific column type
print(df.schema[0].dtype)
# Int64
```

---
[← Previous: Writing data](writing-data.md) | [Next: FlowFile Operations →](flowframe-operations.md)
