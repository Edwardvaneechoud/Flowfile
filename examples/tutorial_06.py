"""Route the whole city report to review or ready with a Gate."""

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

# Chapters 4-5: the city report, filtered by the min_quantity parameter.
minimum = ff.add_flow_parameter(enriched, ff.Parameter("min_quantity", default=8, type="integer"))
bulk = enriched.filter(ff.col("quantity") >= minimum)
summary = bulk.group_by("city").agg(
    ff.col("invoice_id").count().alias("orders"),
    ff.col("revenue").sum().alias("revenue"),
    ff.col("target").first().alias("target"),
)
report = summary.with_columns((ff.col("target") - ff.col("revenue")).alias("shortfall")).sort("city")

# Chapter 6: route the whole report.
# --8<-- [start:gate]
gate = ff.Gate(report, ff.col("shortfall") > 0)
review = gate.then.with_columns(ff.lit("review").alias("route"))
ready = gate.otherwise.with_columns(ff.lit("ready").alias("route"))
routed = ff.concat([review, ready], how="diagonal_relaxed")
# --8<-- [end:gate]

# --8<-- [start:check]
routed.flow_graph.print_tree()
print(routed.collect())

ff.set_flow_parameter(routed, minimum, 1)
print(routed.collect())
# --8<-- [end:check]
