
from flowfile_core.flowfile.flowfile_frame import flow_frame as pl
from flowfile_core.flowfile.flowfile_frame import selectors as scf
from polars import selectors as sc
from flowfile_core.flowfile.flowfile_frame.expr import col, cum_count

df = pl.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "B", "A"],
    "value": [10, None, 30, 40, None],
    "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
})

# Convert timestamp to proper datetime
df = df.with_columns(
    col("timestamp").str.to_datetime().alias("timestamp")
)


condition_df = df.filter(col("category") == "A")
result1 = df.with_columns(
    cum_count("value").over(pl.col('category')).alias("value_count")
)
