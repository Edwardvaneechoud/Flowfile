"""Grouped aggregations, the all_ selector, and window functions (rank and cum_sum over groups)."""

# --8<-- [start:example]
import flowfile as ff

# One group_by node fuses the grouping and every aggregation.
summary = (
    ff.read_csv("data/templates/orders.csv")
    .group_by("customer_id")
    .agg(
        ff.col("order_id").count().alias("n_orders"),
        ff.col("quantity").sum().alias("total_qty"),
        ff.col("quantity").mean().alias("avg_qty"),
        ff.col("quantity").min().alias("min_qty"),
        ff.col("quantity").max().alias("max_qty"),
        ff.col("quantity").std().alias("std_qty"),
        ff.col("quantity").first().alias("first_qty"),
        ff.col("quantity").implode().alias("all_qty"),
    )
    .collect()
)

# Sum every numeric column at once with the all_ selector inside select.
totals = (
    ff.read_csv("data/templates/orders.csv")
    .select(ff.col("quantity"))
    .select(ff.all_().sum())
    .collect()
)

# Window functions: rank and running total within each customer.
ranked = (
    ff.read_csv("data/templates/orders.csv")
    .with_columns(
        ff.col("quantity").rank(method="dense", descending=True).over("customer_id").alias("qty_rank"),
        ff.col("quantity").cum_sum().over("customer_id").alias("cum_qty"),
    )
    .collect()
)
# --8<-- [end:example]

assert summary.height == 401
assert summary["n_orders"].sum() == 1000
assert summary["total_qty"].sum() == 5461

assert totals.to_dicts()[0]["quantity"] == 5461

assert ranked.height == 1000
assert ranked["qty_rank"].min() == 1
