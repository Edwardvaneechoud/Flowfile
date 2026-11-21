"""Extract the city report into a reusable child flow and compare thresholds."""

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

# Chapter 8: the report calculation from chapters 4-5 becomes a child flow.
# --8<-- [start:child]
incoming = ff.FlowInput("invoices", schema=enriched.schema)
child_minimum = ff.add_flow_parameter(incoming, ff.Parameter("min_quantity", default=8, type="integer"))
child_summary = (
    incoming.filter(ff.col("quantity") >= child_minimum)
    .group_by("city")
    .agg(
        ff.col("invoice_id").count().alias("orders"),
        ff.col("revenue").sum().alias("revenue"),
        ff.col("target").first().alias("target"),
    )
)
child_report = child_summary.with_columns((ff.col("target") - ff.col("revenue")).alias("shortfall")).sort("city")
child_report.to_flow_output("city_report")
report_flow = ff.register_flow(child_report, name="Tutorial city summary")
# --8<-- [end:child]

# --8<-- [start:call]
minimum = ff.add_flow_parameter(enriched, ff.Parameter("min_quantity", default=8, type="integer"))
call = ff.RunFlow(report_flow, invoices=enriched, params={"min_quantity": minimum})
report = call["city_report"]
# --8<-- [end:call]

# Chapter 6: the Gate now reads the child flow's report.
gate = ff.Gate(report, ff.col("shortfall") > 0)
review = gate.then.with_columns(ff.lit("review").alias("route"))
ready = gate.otherwise.with_columns(ff.lit("ready").alias("route"))
routed = ff.concat([review, ready], how="diagonal_relaxed")

# --8<-- [start:iterate]
scenarios = ff.from_dict({"minimum": [1, 8, 10]})
comparison = ff.RunFlow(
    report_flow,
    invoices=enriched,
    params={"min_quantity": ff.col("minimum")},
    param_frame=scenarios,
    iterate=True,
)
per_threshold = (
    comparison["city_report"]
    .group_by("param_min_quantity")
    .agg(ff.col("city").count().alias("cities"), ff.col("orders").sum().alias("invoices"))
    .sort("param_min_quantity")
)
# --8<-- [end:iterate]

# --8<-- [start:check]
child_report.flow_graph.print_tree()
print()
routed.flow_graph.print_tree()
print(report.collect())
print(per_threshold.collect())
# --8<-- [end:check]
