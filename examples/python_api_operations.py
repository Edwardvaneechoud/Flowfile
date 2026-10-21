"""Row and column operations: unique, cast, filter, sort, exclude, and string/date/window expressions."""

# --8<-- [start:example]
import flowfile as ff

# Deduplicate, derive columns, filter, sort, then drop a column with a selector.
cleaned = (
    ff.read_csv("data/templates/orders.csv")
    .unique(subset=["order_id"])
    .with_columns(
        (ff.col("quantity") * ff.lit(2)).alias("double_qty"),
        ff.col("quantity").cast(ff.Float64).alias("quantity_f"),
    )
    .filter(ff.col("quantity") >= 3)
    .sort("quantity", descending=True)
    .select(ff.col("*").exclude("product_id"))
    .collect()
)

# String and date namespaces on expressions.
enriched = (
    ff.read_csv("data/templates/customers.csv")
    .with_columns(
        ff.col("name").str.to_uppercase().alias("name_upper"),
        ff.col("email").str.slice(0, 5).alias("email_prefix"),
        ff.col("name").str.contains("a").alias("name_has_a"),
        ff.col("signup_date").dt.year().alias("signup_year"),
        ff.col("signup_date").dt.weekday().alias("signup_weekday"),
    )
    .collect()
)

# A running total per customer with a window (cum_sum over a group).
running = (
    ff.read_csv("data/templates/orders.csv")
    .sort("order_date")
    .with_columns(ff.col("quantity").cum_sum().over("customer_id").alias("running_qty"))
    .collect()
)
# --8<-- [end:example]

assert "product_id" not in cleaned.columns
assert "double_qty" in cleaned.columns and "quantity_f" in cleaned.columns
assert cleaned.height == 800

assert enriched.height == 500
assert enriched["signup_weekday"].min() >= 1
assert enriched["signup_weekday"].max() <= 7
assert enriched["email_prefix"].str.len_chars().max() <= 5

assert running.height == 1000
assert running["running_qty"].max() > 0
