"""Save the finished city report, run it without the Designer, and open it on the canvas."""

import subprocess
import sys
from pathlib import Path

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

# Chapters 4, 5 and 8: the report comes from the reusable child flow.
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

minimum = ff.add_flow_parameter(enriched, ff.Parameter("min_quantity", default=8, type="integer"))
report = ff.RunFlow(report_flow, invoices=enriched, params={"min_quantity": minimum})["city_report"]

# Chapter 9: the shortfall percentage.
scored = ff.sql("SELECT *, ROUND(100.0 * shortfall / target, 2) AS shortfall_pct FROM report", report=report)

# Chapter 6: route the whole report.
gate = ff.Gate(scored, ff.col("shortfall") > 0)
review = gate.then.with_columns(ff.lit("review").alias("route"))
ready = gate.otherwise.with_columns(ff.lit("ready").alias("route"))
routed = ff.concat([review, ready], how="diagonal_relaxed")

# Chapter 7: the file and catalog writers.
output_dir = Path("city_report").resolve()
output_dir.mkdir(exist_ok=True)
routed.write_csv(output_dir / "city_report.csv")
routed.write_parquet(output_dir / "city_report.parquet")
reports = ff.CatalogReference("tutorial_sales", auto_create=True).schema("reports", auto_create=True)
reports.write_table(routed, "city_report", write_mode="overwrite")

# Chapter 10: ship it.
# --8<-- [start:save]
flow_path = output_dir / "city_report.yaml"
routed.flow_graph.save_flow(str(flow_path))
ff.register_flow(routed, name="Tutorial city report")
routed.flow_graph.print_tree()
# --8<-- [end:save]

# --8<-- [start:headless]
subprocess.run(
    [sys.executable, "-m", "flowfile", "run", "flow", str(flow_path), "--param", "min_quantity=1"],
    check=True,
    stdout=subprocess.DEVNULL,
)
print(ff.read_parquet(str(output_dir / "city_report.parquet")).collect())
# --8<-- [end:headless]

# --8<-- [start:editor]
ff.open_graph_in_editor(routed.flow_graph)
# --8<-- [end:editor]
