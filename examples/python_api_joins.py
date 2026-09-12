"""Join types (inner/left/full/semi/anti/cross), concatenation with ff.concat, and join validation."""

# --8<-- [start:example]
import flowfile as ff

orders = ff.read_csv("data/templates/orders.csv")
customers = ff.read_csv("data/templates/customers.csv")
products = ff.read_csv("data/templates/products.csv")

# Enrich orders with customer and product attributes.
inner = orders.join(customers, on="customer_id", how="inner")
with_product = orders.join(products, on="product_id", how="left")

# full keeps unmatched rows from both sides; semi/anti filter the left side.
full = orders.join(customers, on="customer_id", how="full")
repeat_customers = customers.join(orders, on="customer_id", how="semi")
dormant_customers = customers.join(orders, on="customer_id", how="anti")

# Stack frames vertically with ff.concat (there is no vstack), then dedupe.
deduped = ff.concat([customers, customers]).unique()

# Validate a join key before joining: compare distinct keys to total rows.
key_stats = customers.select(
    ff.col("customer_id").n_unique().alias("distinct_customers"),
    ff.col("customer_id").count().alias("total_rows"),
)

inner_df = inner.collect()
full_df = full.collect()
repeat_df = repeat_customers.collect()
dormant_df = dormant_customers.collect()
key_stats_df = key_stats.collect()
# --8<-- [end:example]

with_product_df = with_product.collect()
deduped_df = deduped.collect()

assert inner_df.height == 1000
assert "name" in inner_df.columns
assert with_product_df.height == 1000

assert full_df.height == 1099
assert repeat_df.height == 401
assert dormant_df.height == 99
assert repeat_df.height + dormant_df.height == 500

assert deduped_df.height == 500

stats = key_stats_df.to_dicts()[0]
assert stats["distinct_customers"] == 500
assert stats["total_rows"] == 500
