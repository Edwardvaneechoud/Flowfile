"""Aggregate the enriched invoices into a city revenue report."""

import flowfile as ff

# Chapters 1-3: read, clean, and enrich the invoices.
SALES = "https://raw.githubusercontent.com/edwardvaneechoud/flowfile/main/data/templates/supermarket_sales.csv"
sales = ff.read_csv(SALES, description="Supermarket invoices")

valid = sales.unique().filter(flowfile_formula="[quantity] > 0 and [unit_price] > 0")
orders = valid.with_columns(flowfile_formulas=["[unit_price] * [quantity]"], output_column_names=["revenue"])

members = orders.filter(ff.col("customer_type") == "Member")
regulars = orders.filter(ff.col("customer_type") == "Normal")
combined = ff.concat([members, regulars], how="diagonal_relaxed")

targets = ff.from_dict(
    {
        "city": ["Bago", "Mandalay", "Naypyitaw", "Taunggyi", "Yangon"],
        "target": [30000.0, 35000.0, 25000.0, 30000.0, 35000.0],
    }
)
enriched = combined.join(targets, on="city", how="left")

# Chapter 4: one report row per city.
# --8<-- [start:bulk]
bulk = enriched.filter(ff.col("quantity") >= 8)
# --8<-- [end:bulk]

# --8<-- [start:aggregate]
summary = bulk.group_by("city").agg(
    ff.col("invoice_id").count().alias("orders"),
    ff.col("revenue").sum().alias("revenue"),
    ff.col("target").first().alias("target"),
)
report = summary.with_columns((ff.col("target") - ff.col("revenue")).alias("shortfall")).sort("city")
# --8<-- [end:aggregate]

# --8<-- [start:check]
report.flow_graph.print_tree()
print(report.collect())
# --8<-- [end:check]
